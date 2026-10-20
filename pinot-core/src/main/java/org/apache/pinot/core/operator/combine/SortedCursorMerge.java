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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Sorted k-way merge over per-segment cursors, shared by the streaming combine operators that emit globally sorted
/// rows lazily (the streaming selection ORDER BY combine, and any other combine whose per-segment inputs are already
/// sorted on a key).
///
/// **What it owns.** One [Cursor] per segment operator, a [PriorityQueue] of the *active* cursors ordered by a
/// caller-supplied [Comparator] over each cursor's current head row (the "key tuple": the comparator decides which
/// positions of the `Object[]` row form the key, so rows may carry trailing non-key values), the segment
/// acquire/release lifecycle, drained-cursor cleanup, the combine-level [DataSchema], and schema-mismatch handling.
///
/// **What the caller owns.** The merge loop itself: when to activate a cursor (see [Cursor#activate()]), how many rows
/// to emit, tie handling, frontier/pruning policy, output blocks, termination checks, and the operator lifecycle. The
/// heap is exposed through [#peek()], [#poll()], [#offer(Cursor)] and [#compare(Cursor, Cursor)] so a caller can retain
/// a leader outside the heap, as the selection combine does. The invariant the caller must keep at the points it hands
/// control away is that the heap holds every active, non-exhausted cursor exactly once.
///
/// **Cursor sources.** A cursor reads blocks from its operator through a [BlockRows] adapter. A *streaming* child
/// (decided by the `streamingChildDetector` passed at construction, applied to the operator below any
/// [AcquireReleaseColumnsSegmentOperator]) is pulled block by block until it returns `null`. Any other child is read
/// for exactly one block.
///
/// **Acquire/release.** A cursor acquires its [AcquireReleaseColumnsSegmentOperator] (if it is one) on
/// [Cursor#activate()] and releases it when it is drained or on [#releaseAll()] / [#finish()]; never per block. Rows
/// handed out must be heap copies that remain valid after release.
///
/// **Schema.** The first block seen fixes the merge schema. A later block with a different schema is dropped, its
/// cursor is exhausted, and a deduplicated MERGE_RESPONSE message is recorded; attach it to the terminal block with
/// [#attachSchemaMismatchErrors(BaseResultsBlock)].
///
/// **Thread-safety.** Not thread-safe. All calls must come from the single consumer thread that drives the merge, which
/// is also the thread that holds the segment references. Single-use.
@SuppressWarnings({"rawtypes", "unchecked"})
public class SortedCursorMerge {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedCursorMerge.class);

  /// Adapts a child operator's block type to the rows and schema the merge needs.
  public interface BlockRows {
    DataSchema getDataSchema(BaseResultsBlock block);

    /// Rows of the block, each already sorted per the merge comparator within the block and across successive blocks of
    /// one cursor. May be `null` or empty.
    @Nullable
    List<Object[]> getRows(BaseResultsBlock block);
  }

  private final Comparator<Object[]> _comparator;
  private final Predicate<Operator> _streamingChildDetector;
  private final BlockRows _blockRows;
  private final List<Cursor> _cursorList = new ArrayList<>();
  private final PriorityQueue<Cursor> _priorityQueue;
  private Cursor[] _cursors;

  /// Captured from the first child block seen; all child blocks share the same schema
  @Nullable
  private DataSchema _dataSchema;
  /// Deduplicated MERGE_RESPONSE errors for blocks dropped on schema mismatch; null until the first mismatch
  @Nullable
  private Set<String> _dataSchemaMismatchErrors;
  /// Subset of the above not yet attached to a terminal block; drained on attach
  @Nullable
  private List<String> _unreportedDataSchemaMismatchErrors;

  /// @param comparator orders cursors by their current head row; must be consistent with each cursor's own row order
  /// @param streamingChildDetector whether the (unwrapped) child operator is a lazily-pulled streaming operator
  /// @param blockRows extracts schema and rows from a child block
  /// @param numCursorsHint expected number of cursors, used to size the heap
  public SortedCursorMerge(Comparator<Object[]> comparator, Predicate<Operator> streamingChildDetector,
      BlockRows blockRows, int numCursorsHint) {
    _comparator = comparator;
    _streamingChildDetector = streamingChildDetector;
    _blockRows = blockRows;
    _priorityQueue = new PriorityQueue<>(Math.max(1, numCursorsHint),
        (o1, o2) -> _comparator.compare(o1.currentHead(), o2.currentHead()));
  }

  /// Adds a cursor for `operator`. `minValue`/`maxValue` are optional bounds on the cursor's first key column that the
  /// caller may use for pruning; `null` means unknown. Must be called before [#sortCursorsByBound(boolean)] and
  /// [#getCursors()].
  public Cursor addCursor(Operator<BaseResultsBlock> operator, @Nullable Comparable minValue,
      @Nullable Comparable maxValue) {
    Cursor cursor = new Cursor(operator, minValue, maxValue);
    _cursorList.add(cursor);
    _cursors = null;
    return cursor;
  }

  /// All cursors in their current order (insertion order, or bound order after [#sortCursorsByBound(boolean)]).
  public Cursor[] getCursors() {
    if (_cursors == null) {
      _cursors = _cursorList.toArray(new Cursor[0]);
    }
    return _cursors;
  }

  /// Orders the cursors for lazy activation in frontier order: ascending by min value for ASC, descending by max value
  /// for DESC. Cursors without a bound go first because they must always be processed.
  public void sortCursorsByBound(boolean asc) {
    Cursor[] cursors = getCursors();
    if (asc) {
      Arrays.sort(cursors, (o1, o2) -> {
        if (o1._minValue == null) {
          return o2._minValue == null ? 0 : -1;
        }
        if (o2._minValue == null) {
          return 1;
        }
        return o1._minValue.compareTo(o2._minValue);
      });
    } else {
      Arrays.sort(cursors, (o1, o2) -> {
        if (o1._maxValue == null) {
          return o2._maxValue == null ? 0 : -1;
        }
        if (o2._maxValue == null) {
          return 1;
        }
        return o2._maxValue.compareTo(o1._maxValue);
      });
    }
  }

  /// The merge comparator over head rows.
  public Comparator<Object[]> getComparator() {
    return _comparator;
  }

  /// The merge schema, or `null` until a first block has been seen.
  @Nullable
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /// Compares two cursors' current heads with the merge comparator. Both must have a head.
  public int compare(Cursor c1, Cursor c2) {
    return _comparator.compare(c1.currentHead(), c2.currentHead());
  }

  /// The active cursor with the smallest head, or `null` if the heap is empty.
  @Nullable
  public Cursor peek() {
    return _priorityQueue.peek();
  }

  /// Removes and returns the active cursor with the smallest head, or `null` if the heap is empty.
  @Nullable
  public Cursor poll() {
    return _priorityQueue.poll();
  }

  /// Adds an active cursor that has a head. The cursor must not already be in the heap.
  public void offer(Cursor cursor) {
    _priorityQueue.offer(cursor);
  }

  /// Activates `cursor` and, if it has a first row, adds it to the heap.
  public void activate(Cursor cursor) {
    cursor.activate();
    if (cursor.currentHead() != null) {
      _priorityQueue.offer(cursor);
    }
  }

  /// Exhausts every cursor (releasing segments still held and dropping buffered rows) and empties the heap.
  public void finish() {
    for (Cursor cursor : getCursors()) {
      cursor.exhaust();
    }
    _priorityQueue.clear();
  }

  /// Releases every segment still acquired. Idempotent; for error and early-stop paths.
  public void releaseAll() {
    for (Cursor cursor : getCursors()) {
      cursor.release();
    }
  }

  @VisibleForTesting
  int getNumCursorsHoldingRows() {
    int numCursors = 0;
    for (Cursor cursor : getCursors()) {
      if (cursor._rows != null) {
        numCursors++;
      }
    }
    return numCursors;
  }

  /// Attaches the recorded schema-mismatch errors to `block` (normally the terminal block) and marks them reported, so
  /// a repeated terminal call does not report them twice.
  public <T extends BaseResultsBlock> T attachSchemaMismatchErrors(T block) {
    if (_unreportedDataSchemaMismatchErrors != null && !_unreportedDataSchemaMismatchErrors.isEmpty()) {
      for (String errorMessage : _unreportedDataSchemaMismatchErrors) {
        block.addErrorMessage(QueryErrorMessage.safeMsg(QueryErrorCode.MERGE_RESPONSE, errorMessage));
      }
      _unreportedDataSchemaMismatchErrors.clear();
    }
    return block;
  }

  /// Records that a segment's block was dropped because its schema disagreed with the merge schema. Deduplicated by
  /// message so a mid-reload table with many divergent segments cannot flood the response.
  private void recordDataSchemaMismatch(@Nullable DataSchema mismatched) {
    String errorMessage =
        String.format("Data schema mismatch between merged block: %s and block to merge: %s, drop block to merge",
            _dataSchema, mismatched);
    // NOTE: This is segment level log, so log at debug level to prevent flooding the log.
    LOGGER.debug(errorMessage);
    if (_dataSchemaMismatchErrors == null) {
      _dataSchemaMismatchErrors = new HashSet<>();
      _unreportedDataSchemaMismatchErrors = new ArrayList<>();
    }
    if (_dataSchemaMismatchErrors.add(errorMessage)) {
      _unreportedDataSchemaMismatchErrors.add(errorMessage);
    }
  }

  /// Iterates a single segment's locally-sorted rows, pulling blocks lazily from its operator. Streaming-backed cursors
  /// loop until the operator returns `null`; single-block-backed cursors read exactly one block. The segment is
  /// acquired on activation and released once exhausted or when the combine finishes (see
  /// [SortedCursorMerge]).
  public class Cursor {
    private final Operator<BaseResultsBlock> _operator;
    @Nullable
    private final Comparable _minValue;
    @Nullable
    private final Comparable _maxValue;

    private boolean _streamingChild;
    private boolean _streamingChildResolved;
    private List<Object[]> _rows;
    private int _pos;
    private boolean _acquired;
    /// Cached current head (rows.get(pos)) so the hot heap comparator does not re-index per comparison
    @Nullable
    private Object[] _head;

    Cursor(Operator<BaseResultsBlock> operator, @Nullable Comparable minValue, @Nullable Comparable maxValue) {
      _operator = operator;
      _minValue = minValue;
      _maxValue = maxValue;
    }

    /// Returns the current head row to be merged next, or `null` if not activated or exhausted.
    @Nullable
    public Object[] currentHead() {
      return _head;
    }

    /// Acquires the segment, resolves whether the child is the lazy streaming operator, and reads the first block.
    /// After this call [#currentHead()] returns the first row, or `null` if the segment contributes
    /// nothing.
    public void activate() {
      acquireSegment();
      if (!pullBlock()) {
        exhaust();
      }
    }

    /// Advances past the current head, pulling the next block lazily for streaming cursors. Releases the segment
    /// when the cursor is exhausted; afterwards [#currentHead()] returns `null`.
    public void advance() {
      _pos++;
      if (_pos < _rows.size()) {
        _head = _rows.get(_pos);
        return;
      }
      // Current block drained: streaming cursors pull the next run/block; single-block cursors are done.
      if (_streamingChild && pullBlock()) {
        return;
      }
      exhaust();
    }

    /// Loads the next non-empty block of rows from the operator, capturing the combine-level data schema on first
    /// sight. Returns `false` when the operator is exhausted (no more rows). Streaming-backed operators emit
    /// one run/block per call and `null` when done; single-block operators emit a single block and must not be
    /// called again afterwards, so an empty/null block from a single-block child is treated as exhausted.
    ///
    /// A block whose schema differs from the one already captured is dropped and reported, mirroring
    /// [org.apache.pinot.core.operator.combine.merger.SelectionOrderByResultsBlockMerger]. Segments on a server
    /// can disagree on schema mid-reload (a newly added column exists only in reloaded segments), and merging rows of
    /// differing width under one schema would corrupt the result rather than fail. The cursor is exhausted rather than
    /// skipping the one block, which drops the same unit the merger does: there, a block is a segment's whole result.
    private boolean pullBlock() {
      while (true) {
        BaseResultsBlock block = nextBlock();
        if (block == null) {
          return false;
        }
        if (_dataSchema == null) {
          _dataSchema = _blockRows.getDataSchema(block);
        } else if (!_dataSchema.equals(_blockRows.getDataSchema(block))) {
          recordDataSchemaMismatch(_blockRows.getDataSchema(block));
          return false;
        }
        List<Object[]> rows = _blockRows.getRows(block);
        if (rows != null && !rows.isEmpty()) {
          _rows = rows;
          _pos = 0;
          _head = rows.get(0);
          return true;
        }
        // An empty (non-null) block: a streaming child emits one on a zero-match segment to deliver its schema, which
        // the capture above has just taken. Keep pulling only for streaming children; a single-block child yields
        // exactly one block, so treat it as exhausted.
        if (!_streamingChild) {
          return false;
        }
      }
    }

    private BaseResultsBlock nextBlock() {
      try {
        BaseResultsBlock block = _operator.nextBlock();
        if (!_streamingChildResolved) {
          // The call above materializes the wrapped child, so its type can be read without forcing a separate
          // materializeChildOperator(). Resolved after the first pull rather than before it because that is the
          // earliest point at which the child exists.
          _streamingChild = isStreamingChild();
          _streamingChildResolved = true;
        }
        return block;
      } catch (RuntimeException e) {
        throw BaseCombineOperator.wrapOperatorException(_operator, e);
      }
    }

    /// Returns whether the underlying child operator is a streaming one per the detector. Must be
    /// called after the first [Operator#nextBlock()], which is what materializes a wrapped child.
    private boolean isStreamingChild() {
      Operator underlying = _operator;
      if (_operator instanceof AcquireReleaseColumnsSegmentOperator) {
        // The child is materialized by AcquireReleaseColumnsSegmentOperator#getNextBlock() before it delegates, so it
        // is non-null by the time we get here. Left unchecked deliberately: getChildOperators() returns
        // List.of(child), which itself throws NullPointerException on a null element, so a refactor that breaks the
        // invariant already fails there and an explicit check here would be unreachable.
        underlying = ((AcquireReleaseColumnsSegmentOperator) _operator).getChildOperators().get(0);
      }
      return _streamingChildDetector.test(underlying);
    }

    private void acquireSegment() {
      if (_operator instanceof AcquireReleaseColumnsSegmentOperator) {
        ((AcquireReleaseColumnsSegmentOperator) _operator).acquire();
      }
      _acquired = true;
    }

    /// Releases the segment if still held. Idempotent: safe to call from [#exhaust()] and combine cleanup.
    private void release() {
      if (_acquired) {
        if (_operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) _operator).release();
        }
        _acquired = false;
      }
    }

    /// Lower bound of the first key column, or `null` if unknown.
    @Nullable
    public Comparable getMinValue() {
      return _minValue;
    }

    /// Upper bound of the first key column, or `null` if unknown.
    @Nullable
    public Comparable getMaxValue() {
      return _maxValue;
    }

    /// Releases the segment and drops buffered rows; afterwards [#currentHead()] returns `null`. Idempotent.
    public void exhaust() {
      release();
      _rows = null;
      _head = null;
    }
  }
}
