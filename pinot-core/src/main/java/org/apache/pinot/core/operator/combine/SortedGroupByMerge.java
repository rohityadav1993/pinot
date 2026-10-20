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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SortedRunsTable;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.HashSortedGroupByOperator;
import org.apache.pinot.core.operator.query.StreamingSortedGroupByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.query.QueryThreadContext;


/// One k-way merge, by group key, of the key-ordered per-segment streams of a sorted streaming GROUP BY, over a fixed
/// set of segment operators. It is the merge loop of [StreamingSortedGroupByCombineOperator], factored out so that the
/// combine can run one instance per partition (see [SortedGroupByPartitioning]) on worker threads, or a single one
/// inline on the consumer thread.
///
/// **Merge.** Cursors over all operators are activated on the first [#nextBlock()]. The loop polls the cursor with the
/// smallest key, merges its row into the *open* group when the keys are equal (with
/// [AggregationFunctionUtils#merge], which settles `null` intermediates), and otherwise closes the open group and opens
/// a new one. Closed groups are batched into blocks of [QueryContext#getSortedGroupByBlockSize()]; the open group is
/// never emitted early, because a later cursor may still hold the same key. A key therefore appears at most once across
/// the blocks of one merge, ascending.
///
/// **Null group.** With null handling on, a null key sorts last (the comparator is the ORDER BY one with nulls last),
/// so the null group is the last group of the merge and is merged like any other key.
///
/// **Result kind.** When the leaf returns final results each closed group's aggregation columns are converted with
/// `extractFinalResult` and the block schema carries the final column types, exactly like
/// `IndexedTable#finish(sort, true)`.
///
/// **No limits.** There is no group-count limit and no trimming. The numGroupsLimit flags a per-segment hash
/// aggregation reported ([HashSortedGroupByOperator]) are carried onto every emitted block.
///
/// **Thread-safety.** Not thread-safe and single-use. Every call must come from one thread at a time, and that thread
/// must be the one that activated the cursors (it holds the segment references); the combine hands a merge to exactly
/// one worker (or to the consumer thread) for its whole life. State written by that thread becomes visible to the
/// consumer through the combine's blocking queue.
@SuppressWarnings({"rawtypes", "unchecked"})
final class SortedGroupByMerge {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numKeyColumns;
  private final boolean _returnFinalResult;
  private final int _blockSize;
  private final Comparator<Object[]> _comparator;
  private final String _explainName;
  private final SortedCursorMerge _merge;

  private boolean _activated;
  private long _numRowsMerged;
  /// The group being accumulated; not yet in _outputRows because a later cursor may hold the same key
  @Nullable
  private Object[] _openRow;
  private List<Record> _outputRows = new ArrayList<>();
  private DataSchema _outputSchema;
  private boolean _done;
  private boolean _numGroupsLimitReached;
  private boolean _numGroupsWarningLimitReached;

  SortedGroupByMerge(List<Operator> operators, QueryContext queryContext, Comparator<Object[]> comparator,
      String explainName) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null && queryContext.getGroupByExpressions() != null;
    _numKeyColumns = queryContext.getGroupByExpressions().size();
    _returnFinalResult =
        queryContext.isServerReturnFinalResult() || queryContext.isServerReturnFinalResultKeyUnpartitioned();
    _blockSize = queryContext.getSortedGroupByBlockSize();
    _comparator = comparator;
    _explainName = explainName;
    _merge = new SortedCursorMerge(comparator, SortedGroupByMerge::isStreamingOperator, new GroupByBlockRows(),
        operators.size());
    for (Operator operator : operators) {
      // No key bounds: every cursor is activated, nothing is pruned
      _merge.addCursor(operator, null, null);
    }
  }

  /// Returns the next block of closed groups, or `null` when the merge is complete (every cursor drained and released).
  /// On any exception or error the held segments are released and the merge is finished.
  @Nullable
  GroupByResultsBlock nextBlock() {
    if (_done) {
      return null;
    }
    try {
      long endTimeMs = _queryContext.getEndTimeMs();
      // Once per block, unconditionally: the periodic check inside the loop only fires every few thousand rows, which
      // a small block would never reach.
      QueryThreadContext.checkTerminationAndSampleUsage(_explainName, endTimeMs);
      if (!_activated) {
        for (SortedCursorMerge.Cursor cursor : _merge.getCursors()) {
          QueryThreadContext.checkTerminationAndSampleUsage(_explainName);
          _merge.activate(cursor);
        }
        _activated = true;
      }
      while (true) {
        // The loop may run through many rows without re-entering a child, so observe cancellation, pause and the
        // deadline here.
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically((int) _numRowsMerged++, _explainName, endTimeMs);
        SortedCursorMerge.Cursor cursor = _merge.poll();
        if (cursor == null) {
          break;
        }
        Object[] row = cursor.currentHead();
        if (_openRow != null && _comparator.compare(_openRow, row) == 0) {
          mergeInto(_openRow, row);
        } else {
          closeOpenGroup();
          _openRow = row;
        }
        cursor.advance();
        if (cursor.currentHead() != null) {
          _merge.offer(cursor);
        }
        if (_outputRows.size() >= _blockSize) {
          // Heap invariant holds: the advanced cursor is back in the heap.
          return flush();
        }
      }
      // Every cursor is drained; the open group is complete.
      _done = true;
      _merge.finish();
      closeOpenGroup();
      return _outputRows.isEmpty() ? null : flush();
    } catch (Exception e) {
      _done = true;
      _merge.releaseAll();
      throw e;
    } catch (Throwable t) {
      // An Error must not leave segments acquired for the lifetime of the server.
      _done = true;
      _merge.releaseAll();
      throw t;
    }
  }

  private void closeOpenGroup() {
    if (_openRow != null) {
      _outputRows.add(closeGroup(_openRow));
      _openRow = null;
    }
  }

  /// Folds `row`'s aggregation intermediates into `into` (same key).
  private void mergeInto(Object[] into, Object[] row) {
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      int index = _numKeyColumns + i;
      into[index] = AggregationFunctionUtils.merge(_aggregationFunctions[i], into[index], row[index]);
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

  private GroupByResultsBlock toBlock(List<Record> records) {
    GroupByResultsBlock block = new GroupByResultsBlock(new SortedRunsTable(getOutputSchema(), records), _queryContext);
    block.setNumGroupsLimitReached(_numGroupsLimitReached);
    block.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    return block;
  }

  private GroupByResultsBlock flush() {
    List<Record> records = _outputRows;
    _outputRows = new ArrayList<>();
    return toBlock(records);
  }

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

  /// Releases every segment still acquired. Idempotent; for error and early-stop paths.
  void releaseAll() {
    _done = true;
    _merge.releaseAll();
  }

  <T extends BaseResultsBlock> T attachSchemaMismatchErrors(T block) {
    return _merge.attachSchemaMismatchErrors(block);
  }

  @VisibleForTesting
  int getNumCursorsHoldingRows() {
    return _merge.getNumCursorsHoldingRows();
  }

  /// Reads rows and schema from the per-segment operators' blocks, noting the numGroupsLimit flags they carry.
  private class GroupByBlockRows implements SortedCursorMerge.BlockRows {
    @Override
    public DataSchema getDataSchema(BaseResultsBlock block) {
      return ((GroupByResultsBlock) block).getDataSchema();
    }

    @Nullable
    @Override
    public List<Object[]> getRows(BaseResultsBlock block) {
      GroupByResultsBlock groupByBlock = (GroupByResultsBlock) block;
      _numGroupsLimitReached |= groupByBlock.isNumGroupsLimitReached();
      _numGroupsWarningLimitReached |= groupByBlock.isNumGroupsWarningLimitReached();
      List<IntermediateRecord> records = groupByBlock.getIntermediateRecords();
      if (records == null) {
        return null;
      }
      List<Object[]> rows = new ArrayList<>(records.size());
      for (IntermediateRecord record : records) {
        rows.add(record._record.getValues());
      }
      return rows;
    }
  }

  private static boolean isStreamingOperator(Operator operator) {
    return operator instanceof StreamingSortedGroupByOperator || operator instanceof HashSortedGroupByOperator;
  }
}
