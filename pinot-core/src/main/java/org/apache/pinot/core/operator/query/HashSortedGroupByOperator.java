/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.query;

import com.google.common.base.CaseFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.combine.StreamingSortedGroupByCombineOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Single-segment GROUP BY for a segment that is not (known to be) sorted on the group-by column, an unsorted
/// immutable segment or a consuming one, that still has to join the sorted streaming GROUP BY merge as a key-ordered
/// stream.
///
/// **Algorithm.** It runs the existing per-segment hash aggregation (the wrapped [GroupByOperator], built by
/// [org.apache.pinot.core.plan.GroupByPlanNode]) to completion on the first [#nextBlock()], copies every group out
/// of the thread-local group-key state into self-contained [IntermediateRecord]s, sorts them by group key with the
/// merge comparator (ascending, a null key last when null handling is on, so it lines up with the merge), and then
/// emits them in blocks of [QueryContext#getSortedGroupByBlockSize()]. `null` marks the end. A segment that matches no
/// document still emits one empty block so a consumer learns the schema.
///
/// **Why the copy happens here.** The hash path's [AggregationGroupByResult] is backed by group-key maps held in
/// thread-locals that the same thread clears and refills for its next segment. Materializing in this operator, on the
/// thread that ran the aggregation and before the first block is returned, means no such state is ever read by another
/// thread or outlives the call, whichever thread the merge cursor later runs on.
///
/// **Limits.** The wrapped operator keeps its normal per-segment `numGroupsLimit`, and reports when it was reached;
/// that flag is carried on every emitted block. There is no ORDER BY trimming (the plan gate excludes it). Memory is
/// the segment's group count, as for the hash path.
///
/// **Thread-safety.** Not thread-safe; driven by one thread.
public class HashSortedGroupByOperator extends BaseOperator<GroupByResultsBlock> {
  public static final String EXPLAIN_NAME = "GROUP_BY_HASH_SORTED_STREAMING";

  private final QueryContext _queryContext;
  private final IndexSegment _indexSegment;
  private final Operator<GroupByResultsBlock> _hashOperator;
  private final Comparator<Object[]> _comparator;
  private final int _blockSize;

  @Nullable
  private DataSchema _dataSchema;
  @Nullable
  private List<IntermediateRecord> _sortedRecords;
  private int _position;
  private boolean _emittedBlock;
  private boolean _numGroupsLimitReached;
  private boolean _numGroupsWarningLimitReached;

  public HashSortedGroupByOperator(QueryContext queryContext, Operator<GroupByResultsBlock> hashOperator,
      IndexSegment indexSegment) {
    _queryContext = queryContext;
    _indexSegment = indexSegment;
    _hashOperator = hashOperator;
    _comparator = StreamingSortedGroupByCombineOperator.getKeyComparator(queryContext);
    _blockSize = queryContext.getSortedGroupByBlockSize();
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    if (_sortedRecords == null) {
      aggregateAndSort();
    }
    if (_position >= _sortedRecords.size() && _emittedBlock) {
      return null;
    }
    // An empty first block (zero-match segment) carries the schema
    _emittedBlock = true;
    int end = Math.min(_position + _blockSize, _sortedRecords.size());
    List<IntermediateRecord> chunk = new ArrayList<>(_sortedRecords.subList(_position, end));
    _position = end;
    if (_position >= _sortedRecords.size()) {
      // Let the segment's groups be collected as soon as the last block is out
      _sortedRecords = List.of();
      _position = 0;
    }
    GroupByResultsBlock block = new GroupByResultsBlock(_dataSchema, chunk, _queryContext);
    block.setNumGroupsLimitReached(_numGroupsLimitReached);
    block.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    return block;
  }

  private void aggregateAndSort() {
    GroupByResultsBlock hashBlock = _hashOperator.nextBlock();
    _dataSchema = hashBlock.getDataSchema();
    _numGroupsLimitReached = hashBlock.isNumGroupsLimitReached();
    _numGroupsWarningLimitReached = hashBlock.isNumGroupsWarningLimitReached();
    List<IntermediateRecord> records = hashBlock.getIntermediateRecords();
    AggregationGroupByResult groupByResult = hashBlock.getAggregationGroupByResult();
    if (records == null) {
      records = groupByResult != null ? materialize(groupByResult, _dataSchema.size()) : List.of();
    }
    List<IntermediateRecord> sorted = new ArrayList<>(records);
    sorted.sort((r1, r2) -> _comparator.compare(r1._record.getValues(), r2._record.getValues()));
    _sortedRecords = sorted;
  }

  /// Copies the groups out of the thread-local group-key state, then closes the key generator.
  private List<IntermediateRecord> materialize(AggregationGroupByResult groupByResult, int numColumns) {
    int numAggregationFunctions = _queryContext.getAggregationFunctions().length;
    int numKeyColumns = numColumns - numAggregationFunctions;
    List<IntermediateRecord> records = new ArrayList<>(groupByResult.getNumGroups());
    try {
      Iterator<GroupKeyGenerator.GroupKey> iterator = groupByResult.getGroupKeyIterator();
      int numExtracted = 0;
      while (iterator.hasNext()) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numExtracted++, EXPLAIN_NAME);
        GroupKeyGenerator.GroupKey groupKey = iterator.next();
        Object[] keys = groupKey._keys;
        Object[] values = Arrays.copyOf(keys, numColumns);
        for (int i = 0; i < numAggregationFunctions; i++) {
          values[numKeyColumns + i] = groupByResult.getResultForGroupId(i, groupKey._groupId);
        }
        records.add(IntermediateRecord.withoutOrderByValues(new Key(keys), new Record(values)));
      }
    } finally {
      groupByResult.closeGroupKeyGenerator();
    }
    return records;
  }

  /// The segment this operator reads, so the combine can read its partition metadata without acquiring it.
  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of(_hashOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _hashOperator.getExecutionStatistics();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLongIdempotent("blockSize", _blockSize);
  }
}
