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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.query.QueryScanCostContext;


/// Single-segment GROUP BY over a segment that is physically sorted on its one, dictionary-encoded, single-value
/// group-by column. It streams finished groups in key order instead of materializing every group of the segment.
///
/// **Algorithm.** The projection is read in document-id order, which is key order on a sorted segment. Within each
/// [ValueBlock] a key change is a change of dictionary id (the dictionary of a sorted column is itself sorted, so
/// dictionary-id order is value order within the segment). Each run of equal keys is aggregated with the regular
/// [AggregationFunction#aggregateGroupBySV] entry point, using the run's index within the block as the group id. The
/// last run of a block stays *open*: the next block may continue it, in which case the two intermediate results are
/// merged. Every other run is final and is emitted as a `(key, intermediate results)` row.
///
/// **Output.** Each [#nextBlock()] returns a [GroupByResultsBlock] of [IntermediateRecord]s whose rows are laid out
/// `[key, aggregation intermediates...]` and sorted ascending by key, with no key repeated within or across the blocks
/// of this operator. `null` marks the end. A segment that matches no document still emits one empty block so a
/// consumer learns the schema. This block is not Table-backed: it is only meant for [#nextBlock()] consumers that read
/// [GroupByResultsBlock#getIntermediateRecords()], such as the sorted streaming GROUP BY combine.
///
/// **No limits.** Unlike [GroupByOperator] this never creates a group-key generator, so `numGroupsLimit` and segment
/// trimming do not apply: memory is bounded by the largest block, not by the group count. The planner only builds it
/// when the query cannot trim at the leaf (see
/// [org.apache.pinot.core.plan.StreamingSortedGroupByPlanNode#getFallbackReason]).
///
/// **Thread-safety.** Not thread-safe; driven by one thread.
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSortedGroupByOperator extends BaseOperator<GroupByResultsBlock> {
  public static final String EXPLAIN_NAME = "GROUP_BY_SORTED_STREAMING";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext _groupByExpression;
  private final BaseProjectOperator<?> _projectOperator;
  private final long _numTotalDocs;
  private final DataSchema _dataSchema;

  // Scratch buffers sized to the largest value block seen
  private int[] _runIds = new int[0];
  private int[] _runDictIds = new int[0];

  // The open (last, possibly continuing) run. _openValues is null when no run is open.
  private int _openDictId;
  @Nullable
  private Object[] _openValues;

  private boolean _inputExhausted;
  private boolean _emittedBlock;
  private int _numDocsScanned = 0;

  public StreamingSortedGroupByOperator(QueryContext queryContext, AggregationInfo aggregationInfo,
      long numTotalDocs) {
    assert queryContext.getAggregationFunctions() != null && queryContext.getGroupByExpressions() != null
        && queryContext.getGroupByExpressions().size() == 1;
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _groupByExpression = queryContext.getGroupByExpressions().get(0);
    _projectOperator = aggregationInfo.getProjectOperator();
    _numTotalDocs = numTotalDocs;

    // Same layout as GroupByOperator: key column first, then the aggregation intermediates
    int numAggregationFunctions = _aggregationFunctions.length;
    String[] columnNames = new String[1 + numAggregationFunctions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[1 + numAggregationFunctions];
    columnNames[0] = _groupByExpression.toString();
    columnDataTypes[0] = DataSchema.ColumnDataType.fromDataTypeSV(
        _projectOperator.getResultColumnContext(_groupByExpression).getDataType());
    for (int i = 0; i < numAggregationFunctions; i++) {
      columnNames[1 + i] = _aggregationFunctions[i].getResultColumnName();
      columnDataTypes[1 + i] = _aggregationFunctions[i].getIntermediateResultColumnType();
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    List<IntermediateRecord> finished = new ArrayList<>();
    // Pull input until at least one run is final, so a block is never empty unless the segment is empty
    while (finished.isEmpty() && !_inputExhausted) {
      ValueBlock valueBlock = _projectOperator.nextBlock();
      if (valueBlock == null) {
        _inputExhausted = true;
        break;
      }
      processBlock(valueBlock, finished);
    }
    if (_inputExhausted && _openValues != null) {
      finished.add(toRecord(_openValues));
      _openValues = null;
    }
    if (finished.isEmpty() && _emittedBlock) {
      return null;
    }
    // An empty first block (zero-match segment) carries the schema
    _emittedBlock = true;
    return new GroupByResultsBlock(_dataSchema, finished, _queryContext);
  }

  private void processBlock(ValueBlock valueBlock, List<IntermediateRecord> finished) {
    int numDocs = valueBlock.getNumDocs();
    _numDocsScanned += numDocs;
    QueryScanCostContext scanCost = getScanCostContext();
    if (scanCost != null) {
      scanCost.addDocsScanned(numDocs);
      scanCost.addEntriesScannedPostFilter((long) numDocs * _projectOperator.getNumColumnsProjected());
    }
    if (numDocs == 0) {
      return;
    }
    BlockValSet keyValSet = valueBlock.getBlockValueSet(_groupByExpression);
    int[] dictIds = keyValSet.getDictionaryIdsSV();
    Dictionary dictionary = keyValSet.getDictionary();
    assert dictionary != null;

    if (_runIds.length < numDocs) {
      _runIds = new int[numDocs];
      _runDictIds = new int[numDocs];
    }
    int[] runIds = _runIds;
    int[] runDictIds = _runDictIds;
    int numRuns = 0;
    int previousDictId = -1;
    for (int i = 0; i < numDocs; i++) {
      int dictId = dictIds[i];
      if (i == 0 || dictId != previousDictId) {
        runDictIds[numRuns++] = dictId;
        previousDictId = dictId;
      }
      runIds[i] = numRuns - 1;
    }

    // Aggregate every run of this block at once, one holder per function, group id = run index in the block
    int numAggregationFunctions = _aggregationFunctions.length;
    GroupByResultHolder[] holders = new GroupByResultHolder[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction function = _aggregationFunctions[i];
      GroupByResultHolder holder = function.createGroupByResultHolder(numRuns, numRuns);
      holder.ensureCapacity(numRuns);
      Map<ExpressionContext, BlockValSet> blockValSetMap =
          AggregationFunctionUtils.getBlockValSetMap(function, valueBlock);
      function.aggregateGroupBySV(numDocs, runIds, holder, blockValSetMap);
      holders[i] = holder;
    }

    for (int run = 0; run < numRuns; run++) {
      if (run == 0 && _openValues != null && runDictIds[0] == _openDictId) {
        // The previous block's last run continues into this block
        for (int i = 0; i < numAggregationFunctions; i++) {
          _openValues[1 + i] = AggregationFunctionUtils.merge(_aggregationFunctions[i], _openValues[1 + i],
              _aggregationFunctions[i].extractGroupByResult(holders[i], run));
        }
        continue;
      }
      if (_openValues != null) {
        finished.add(toRecord(_openValues));
      }
      Object[] values = new Object[1 + numAggregationFunctions];
      values[0] = dictionary.getInternal(runDictIds[run]);
      for (int i = 0; i < numAggregationFunctions; i++) {
        values[1 + i] = _aggregationFunctions[i].extractGroupByResult(holders[i], run);
      }
      _openValues = values;
      _openDictId = runDictIds[run];
    }
  }

  private static IntermediateRecord toRecord(Object[] values) {
    return IntermediateRecord.withoutOrderByValues(new Key(new Object[]{values[0]}), new Record(values));
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of(_projectOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME + "(groupKeys:" + _groupByExpression + ", aggregations:"
        + Arrays.stream(_aggregationFunctions).map(AggregationFunction::toExplainString)
        .collect(Collectors.joining(", ")) + ")";
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putStringList("groupKeys", List.of(_groupByExpression.toString()));
    attributeBuilder.putStringList("aggregations",
        Arrays.stream(_aggregationFunctions).map(AggregationFunction::toExplainString).collect(Collectors.toList()));
  }
}
