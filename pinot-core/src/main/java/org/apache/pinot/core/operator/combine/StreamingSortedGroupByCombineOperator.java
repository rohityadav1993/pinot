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
package org.apache.pinot.core.operator.combine;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SortedRunsTable;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.query.StreamingSortedGroupByOperator;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Streaming combine for the sorted streaming GROUP BY: a k-way merge, by group key, of the key-ordered streams that
/// [StreamingSortedGroupByOperator] produces per segment, built on [SortedCursorMerge].
///
/// **Merge.** Cursors over all segments are activated up front. The loop polls the cursor with the smallest key,
/// merges its row into the *open* group when the keys are equal (with [AggregationFunctionUtils#merge], which settles
/// `null` intermediates), and otherwise closes the open group and opens a new one. Closed groups are batched into
/// blocks of [QueryContext#getSortedGroupByBlockSize()] and returned from [#getNextBlock()]; the open group is
/// never emitted early, because a later segment may still hold the same key. A key therefore appears at most once
/// across all output blocks, and the output is ascending by key.
///
/// **Key.** The merge comparator is the standard ORDER BY comparator (ascending) over the group-by key prefix of the
/// row, so a composite key needs no new code here; only the plan gate restricts engagement to one column today. Keys
/// compare by value, never by dictionary id, because each segment has its own dictionary.
///
/// **Result kind.** When the leaf returns final results ([QueryContext#isServerReturnFinalResult()] or the key
/// unpartitioned variant) each closed group's aggregation columns are converted with `extractFinalResult` and the
/// block schema carries the final column types, exactly like `IndexedTable#finish(sort, true)`. Otherwise blocks carry
/// intermediate results. Both are safe because a key is complete when emitted.
///
/// **No limits.** There is no group-count limit and no trimming; memory is bounded by one block per active cursor plus
/// the output batch.
///
/// **Threading.** Like [StreamingSelectionOrderByCombineOperator] this runs single-threaded on the consumer thread in
/// [#getNextBlock()] and bypasses the base worker-queue model (and its `Phaser`); no async work may be introduced
/// without restoring that fence. Single-use, not thread-safe.
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSortedGroupByCombineOperator extends BaseStreamingCombineOperator<GroupByResultsBlock> {
  public static final String EXPLAIN_NAME = "COMBINE_GROUP_BY_SORTED_STREAMING";

  private static final SortedCursorMerge.BlockRows GROUP_BY_BLOCK_ROWS = new SortedCursorMerge.BlockRows() {
    @Override
    public DataSchema getDataSchema(BaseResultsBlock block) {
      return ((GroupByResultsBlock) block).getDataSchema();
    }

    @Nullable
    @Override
    public List<Object[]> getRows(BaseResultsBlock block) {
      List<IntermediateRecord> records = ((GroupByResultsBlock) block).getIntermediateRecords();
      if (records == null) {
        return null;
      }
      List<Object[]> rows = new ArrayList<>(records.size());
      for (IntermediateRecord record : records) {
        rows.add(record._record.getValues());
      }
      return rows;
    }
  };

  private final AggregationFunction[] _aggregationFunctions;
  private final int _numKeyColumns;
  private final boolean _returnFinalResult;
  private final int _blockSize;
  private final Comparator<Object[]> _comparator;
  private final SortedCursorMerge _merge;

  // Merge progress (single-threaded; mutated only by the consumer thread driving getNextBlock())
  private boolean _activated;
  private long _numRowsMerged;
  /// The group being accumulated; not yet in _outputRows because a later cursor may hold the same key
  @Nullable
  private Object[] _openRow;
  private List<Record> _outputRows = new ArrayList<>();
  private DataSchema _outputSchema;
  private boolean _done;

  public StreamingSortedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    // Null merger and unused executor, as in StreamingSelectionOrderByCombineOperator: the base consumption path is
    // replaced and its entry points below fail loud.
    super(null, operators, queryContext, executorService);
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null && !groupByExpressions.isEmpty();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numKeyColumns = groupByExpressions.size();
    _returnFinalResult =
        queryContext.isServerReturnFinalResult() || queryContext.isServerReturnFinalResultKeyUnpartitioned();
    _blockSize = queryContext.getSortedGroupByBlockSize();

    List<OrderByExpressionContext> keyOrder = new ArrayList<>(_numKeyColumns);
    for (ExpressionContext groupByExpression : groupByExpressions) {
      keyOrder.add(new OrderByExpressionContext(groupByExpression, true, true));
    }
    _comparator = OrderByComparatorFactory.getComparator(keyOrder, queryContext.isNullHandlingEnabled());

    _merge = new SortedCursorMerge(_comparator, StreamingSortedGroupByCombineOperator::isStreamingOperator,
        GROUP_BY_BLOCK_ROWS, _numOperators);
    for (int i = 0; i < _numOperators; i++) {
      // No key bounds: every cursor is activated, nothing is pruned
      _merge.addCursor(_operators.get(i), null, null);
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLongIdempotent("blockSize", _blockSize);
    attributeBuilder.putBool("finalResult", _returnFinalResult);
    attributeBuilder.putLong("numSegments", _numOperators);
  }

  /// No worker threads: the merge runs lazily in [#getNextBlock()].
  @Override
  public void start() {
  }

  /// Releases any segment still acquired (idempotent) so an early stop cannot leak acquires.
  @Override
  public void stop() {
    _done = true;
    _merge.releaseAll();
  }

  @Override
  protected void processSegments() {
    throw new IllegalStateException(
        "StreamingSortedGroupByCombineOperator runs single-threaded; processSegments() must not be called");
  }

  @Override
  protected boolean isQuerySatisfied(GroupByResultsBlock resultsBlock, Object tracker) {
    throw new IllegalStateException(
        "StreamingSortedGroupByCombineOperator has no early exit; isQuerySatisfied() must not be called");
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    if (_done) {
      return terminalBlock();
    }
    try {
      long endTimeMs = _queryContext.getEndTimeMs();
      // Once per block, unconditionally: the periodic check inside the loop only fires every few thousand rows, which
      // a small block would never reach.
      QueryThreadContext.checkTerminationAndSampleUsage(EXPLAIN_NAME, endTimeMs);
      if (!_activated) {
        for (SortedCursorMerge.Cursor cursor : _merge.getCursors()) {
          QueryThreadContext.checkTerminationAndSampleUsage(EXPLAIN_NAME);
          _merge.activate(cursor);
        }
        _activated = true;
      }
      while (true) {
        // The loop may run through many rows without re-entering a child, so observe cancellation, pause and the
        // deadline here.
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically((int) _numRowsMerged++, EXPLAIN_NAME, endTimeMs);
        SortedCursorMerge.Cursor cursor = _merge.poll();
        if (cursor == null) {
          break;
        }
        Object[] row = cursor.currentHead();
        if (_openRow != null && _comparator.compare(_openRow, row) == 0) {
          mergeInto(_openRow, row);
        } else {
          if (_openRow != null) {
            _outputRows.add(closeGroup(_openRow));
          }
          _openRow = row;
        }
        cursor.advance();
        if (cursor.currentHead() != null) {
          _merge.offer(cursor);
        }
        if (_outputRows.size() >= _blockSize) {
          // Heap invariant holds: the advanced cursor is back in the heap.
          return flushDataBlock();
        }
      }
      // Every cursor is drained; the open group is complete.
      _done = true;
      _merge.finish();
      if (_openRow != null) {
        _outputRows.add(closeGroup(_openRow));
        _openRow = null;
      }
      if (!_outputRows.isEmpty()) {
        return flushDataBlock();
      }
      return terminalBlock();
    } catch (Exception e) {
      _done = true;
      _merge.releaseAll();
      return createExceptionResultsBlockAndAttachExecutionStats(e, "merging sorted group-by results");
    } catch (Throwable t) {
      // An Error must not leave segments acquired for the lifetime of the server.
      _done = true;
      _merge.releaseAll();
      throw t;
    }
  }

  private BaseResultsBlock terminalBlock() {
    return _merge.attachSchemaMismatchErrors(attachExecutionStats(new MetadataResultsBlock()));
  }

  /// Folds `row`'s aggregation intermediates into `openRow` (same key).
  private void mergeInto(Object[] openRow, Object[] row) {
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      int index = _numKeyColumns + i;
      openRow[index] = AggregationFunctionUtils.merge(_aggregationFunctions[i], openRow[index], row[index]);
    }
  }

  /// Turns a finished group into an output record, extracting final results when the leaf returns them.
  private Record closeGroup(Object[] row) {
    if (_returnFinalResult) {
      for (int i = 0; i < _aggregationFunctions.length; i++) {
        int index = _numKeyColumns + i;
        row[index] = _aggregationFunctions[i].extractFinalResult(row[index]);
      }
    }
    return new Record(row);
  }

  private BaseResultsBlock flushDataBlock() {
    List<Record> records = _outputRows;
    _outputRows = new ArrayList<>();
    return new GroupByResultsBlock(new SortedRunsTable(getOutputSchema(), records), _queryContext);
  }

  /// The output schema: the segments' intermediate schema, with the aggregation columns switched to their final types
  /// when the leaf returns final results.
  private DataSchema getOutputSchema() {
    if (_outputSchema == null) {
      // Rows only reach the output through a cursor, which captures the schema before accepting any.
      DataSchema schema = _merge.getDataSchema();
      assert schema != null;
      if (_returnFinalResult) {
        DataSchema.ColumnDataType[] columnDataTypes = schema.getColumnDataTypes().clone();
        for (int i = 0; i < _aggregationFunctions.length; i++) {
          columnDataTypes[_numKeyColumns + i] = _aggregationFunctions[i].getFinalResultColumnType();
        }
        schema = new DataSchema(schema.getColumnNames(), columnDataTypes);
      }
      _outputSchema = schema;
    }
    return _outputSchema;
  }

  @VisibleForTesting
  int getNumCursorsHoldingRows() {
    return _merge.getNumCursorsHoldingRows();
  }

  private static boolean isStreamingOperator(Operator operator) {
    return operator instanceof StreamingSortedGroupByOperator;
  }
}
