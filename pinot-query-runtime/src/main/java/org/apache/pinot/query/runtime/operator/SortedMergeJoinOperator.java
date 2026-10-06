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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `SortedMergeJoinOperator` implements a streaming sorted merge join.
///
/// Unlike [HashJoinOperator], it does not materialize the right side into an in-memory hash table. Instead it
/// assumes both the left and right inputs are already sorted in ascending order on their respective join keys and
/// advances two cursors in lock-step (a two-pointer merge). Only one block per side is held in memory at a time, plus a
/// small buffer for the run of right rows that share the current join key (needed to support one-to-many and
/// many-to-many matches).
///
/// This makes memory usage proportional to the largest single-key run on the right side rather than the entire right
/// input, which is the key advantage for pre-sorted, pre-partitioned data layouts.
///
/// Output is produced in blocks of at most 1024 rows. The cross product of a duplicate key is resumable: the position
/// inside the buffered right run (and whether the current left row has matched yet, for LEFT joins) is kept across
/// blocks, so a downstream LIMIT that early-terminates the operator stops within a hot key instead of waiting for the
/// whole product to be materialized.
///
/// `maxRowsInJoin` follows the hash join's semantics. The buffered right-key run is the analog of the hash join's
/// build side: it is capped on the number of buffered rows (THROW fails the query; BREAK truncates the run and keeps
/// merging against the truncated run). The joined output is checked per output block, like the hash join checks the
/// rows produced for each left block (THROW fails the query; BREAK ends the join with the rows produced so far). The
/// output is not capped cumulatively, so a join that produces many blocks of at most `maxRowsInJoin` rows succeeds.
///
/// Whenever the join finishes before an input reached its EOS (the other input ended, a downstream operator
/// early-terminated, or BREAK was hit), the unfinished inputs are early-terminated and drained to their EOS so that
/// the stats the upstream stages attach to their EOS are not lost.
///
/// Join keys are ordered with `compareTo`, but hash join (and the exchange that partitions the inputs) treats keys by
/// `equals`. For `BIG_DECIMAL` keys, which differ (`1.0` vs `1.00`), a candidate pair is emitted only if the
/// `BIG_DECIMAL` key columns are also `equals`.
///
/// Buffered rows (the right run and each cursor's current block) are released as soon as the operator reaches an EOS,
/// fails or is closed/cancelled.
///
/// This operator is driven by a single consumer thread and is not thread-safe.
///
/// Preconditions (enforced by the planner, assumed here):
///   - Both inputs are sorted ascending, nulls last, on the join keys (left keys for the left input, right keys for
///       the right input), using the natural order of each key's stored type.
///   - The join is an equi-join (non-empty join keys). Non-equi conditions are still applied as residual filters.
///
/// Rows whose join key contains a `null` value never match (per SQL semantics) and are skipped; for LEFT joins
/// such left rows are emitted with `null` padding on the right.
///
/// Only INNER and LEFT joins are currently supported.
public class SortedMergeJoinOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMergeJoinOperator.class);
  private static final String EXPLAIN_NAME = "SORTED_MERGE_JOIN";
  private static final String MERGE_LOOP_SCOPE = "SortedMergeJoinOperator#mergeLoop";
  private static final String EMIT_MATCHED_KEY_SCOPE = "SortedMergeJoinOperator#emitMatchedKey";
  private static final String BUFFER_RIGHT_RUN_SCOPE = "SortedMergeJoinOperator#bufferRightRun";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES = Set.of(JoinRelType.INNER, JoinRelType.LEFT);
  // Maximum number of output rows per emitted block. A duplicate-key cross product resumes across blocks instead of
  // overshooting this.
  private static final int TARGET_BLOCK_SIZE_ROWS = 1024;

  private final Cursor _leftCursor;
  private final Cursor _rightCursor;
  private final boolean _needUnmatchedLeftRows;
  private final int[] _leftKeyIds;
  private final int[] _rightKeyIds;
  // One comparator per join key column, specialized once on the column's stored type so the hot path neither
  // re-dispatches on the type nor goes through a raw Comparable for the numeric types.
  private final ValueComparator[] _keyComparators;
  // Indexes (into the key arrays) of the BIG_DECIMAL key columns, which must additionally match by equals().
  private final int[] _bigDecimalKeyIndexes;
  // Reused to evaluate the residual predicate without allocating a view per candidate pair.
  private final PairView _pairView;
  private final DataSchema _resultSchema;
  private final int _leftColumnSize;
  private final int _resultColumnSize;
  private final List<TransformOperand> _nonEquiEvaluators;
  private final boolean _hasNonEquiConditions;
  private final int _maxRowsInJoin;
  private final JoinOverFlowMode _joinOverflowMode;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);
  // Reused buffer holding the run of right rows that share the current join key.
  // Not final: releaseBuffers() replaces it instead of clearing, so the grown backing array is dropped too.
  private List<Object[]> _rightRun = new ArrayList<>();
  // The left row whose key the buffered run belongs to; null while no run is buffered.
  @Nullable
  private Object[] _rightRunAnchor;
  // Resume state of the current left row against the buffered run, kept across output blocks.
  private int _rightRunIndex;
  private boolean _leftRowMatched;

  // Monotonically increasing count of examined input rows and candidate pairs. Used as the tick for
  // checkTerminationAndSampleUsagePeriodically, which only samples when the counter is a multiple of 8192. The number
  // of rows accumulated in the current output block cannot serve as that tick: it is capped at TARGET_BLOCK_SIZE_ROWS,
  // so it would sample on every call while the block is empty and then never again once it is not.
  private int _numRowsProcessed;
  @Nullable
  private MseBlock.Eos _eos;

  public SortedMergeJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    JoinRelType joinType = node.getJoinType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(joinType),
        "Join type: %s is not supported for sorted merge join", joinType);
    List<Integer> leftKeys = node.getLeftKeys();
    List<Integer> rightKeys = node.getRightKeys();
    Preconditions.checkState(!leftKeys.isEmpty(), "Sorted merge join operator requires join keys");
    Preconditions.checkState(leftKeys.size() == rightKeys.size(),
        "Left and right join keys must have the same size, got: %s and %s", leftKeys.size(), rightKeys.size());
    _leftCursor = new Cursor(leftInput);
    _rightCursor = new Cursor(rightInput);
    _needUnmatchedLeftRows = joinType == JoinRelType.LEFT;
    _leftKeyIds = toIntArray(leftKeys);
    _rightKeyIds = toIntArray(rightKeys);
    _keyComparators = new ValueComparator[_leftKeyIds.length];
    for (int i = 0; i < _leftKeyIds.length; i++) {
      // Left and right join key columns share a common type after planner type coercion, so the left column's stored
      // type is representative of both sides.
      _keyComparators[i] = ValueComparator.of(leftSchema.getColumnDataType(_leftKeyIds[i]).getStoredType());
    }
    _bigDecimalKeyIndexes = IntStream.range(0, _leftKeyIds.length)
        .filter(i -> leftSchema.getColumnDataType(_leftKeyIds[i]).getStoredType() == ColumnDataType.BIG_DECIMAL)
        .toArray();
    _leftColumnSize = leftSchema.size();
    _resultSchema = node.getDataSchema();
    _resultColumnSize = _resultSchema.size();
    _pairView = new PairView(_leftColumnSize, _resultColumnSize);
    List<RexExpression> nonEquiConditions = node.getNonEquiConditions();
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }
    _hasNonEquiConditions = !_nonEquiEvaluators.isEmpty();
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInJoin = BaseJoinOperator.getMaxRowsInJoin(metadata, nodeHint);
    _joinOverflowMode = BaseJoinOperator.getJoinOverflowMode(metadata, nodeHint);
  }

  private static int[] toIntArray(List<Integer> list) {
    int[] array = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public Type getOperatorType() {
    return Type.SORTED_MERGE_JOIN;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftCursor.getInput(), _rightCursor.getInput());
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  protected void releaseBuffers() {
    // Replaced, not cleared: clear() would keep the backing array the run grew to. Safe to replace because joinRow()
    // copies every row it emits, so no block handed downstream aliases this list.
    _rightRun = new ArrayList<>();
    _rightRunAnchor = null;
    _rightRunIndex = 0;
    _leftRowMatched = false;
    _leftCursor.releaseRows();
    _rightCursor.releaseRows();
  }

  @Override
  protected boolean hasBufferedState() {
    return !_rightRun.isEmpty() || _rightRunAnchor != null || _leftCursor.hasBufferedRows()
        || _rightCursor.hasBufferedRows();
  }

  @Override
  protected MseBlock getNextBlock() {
    try {
      MseBlock block = produceNextBlock();
      if (_eos != null) {
        // The buffers are dead weight from here on, whether this is a success, an error or an early termination.
        // Release them now rather than waiting for close()/cancel().
        releaseBuffers();
      }
      return block;
    } catch (RuntimeException e) {
      releaseBuffers();
      throw e;
    }
  }

  private MseBlock produceNextBlock() {
    if (_eos != null) {
      return _eos;
    }
    // If a downstream operator (e.g. a LIMIT) has signalled early termination, stop producing. The check runs once
    // per output block, and a duplicate-key run resumes across blocks, so this also stops in the middle of a hot key.
    if (_isEarlyTerminated) {
      finish();
      return _eos;
    }
    List<Object[]> rows = new ArrayList<>(TARGET_BLOCK_SIZE_ROWS);
    while (rows.size() < TARGET_BLOCK_SIZE_ROWS) {
      boolean leftHasRow = _leftCursor.advanceToNextRow();
      if (_leftCursor.isError()) {
        _eos = _leftCursor.getEos();
        return _eos;
      }
      if (!leftHasRow) {
        // Left input is exhausted. INNER and LEFT joins emit nothing for unmatched right rows.
        finish();
        break;
      }
      Object[] leftRow = _leftCursor.peek();
      if (_rightRunAnchor != null) {
        if (compareKeys(_rightRunAnchor, _leftKeyIds, leftRow, _leftKeyIds) == 0) {
          // The left row shares the buffered key: (resume) emitting its pairs with the right run.
          if (!emitMatchedKey(rows, leftRow)) {
            break;
          }
          continue;
        }
        clearRightRun();
      }
      boolean rightHasRow = _rightCursor.advanceToNextRow();
      if (_rightCursor.isError()) {
        _eos = _rightCursor.getEos();
        return _eos;
      }
      if (!rightHasRow) {
        // Right input is exhausted. Remaining left rows are unmatched.
        if (!_needUnmatchedLeftRows) {
          finish();
          break;
        }
        if (!addRow(rows, joinRow(leftRow, null))) {
          break;
        }
        _leftCursor.consume();
        checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, MERGE_LOOP_SCOPE);
        continue;
      }
      Object[] rightRow = _rightCursor.peek();
      // Null join keys never match per SQL semantics.
      if (hasNullKey(leftRow, _leftKeyIds)) {
        if (_needUnmatchedLeftRows && !addRow(rows, joinRow(leftRow, null))) {
          break;
        }
        _leftCursor.consume();
      } else if (hasNullKey(rightRow, _rightKeyIds)) {
        _rightCursor.consume();
      } else {
        int cmp = compareKeys(leftRow, _leftKeyIds, rightRow, _rightKeyIds);
        if (cmp < 0) {
          if (_needUnmatchedLeftRows && !addRow(rows, joinRow(leftRow, null))) {
            break;
          }
          _leftCursor.consume();
        } else if (cmp > 0) {
          _rightCursor.consume();
        } else if (!bufferRightRun(leftRow)) {
          break;
        }
      }
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, MERGE_LOOP_SCOPE);
    }
    if (!rows.isEmpty()) {
      return new RowHeapDataBlock(rows, _resultSchema);
    }
    // No rows produced in this pass: the loop only exits with empty rows once an EOS has been reached.
    assert _eos != null;
    return _eos;
  }

  /// Ends the join: tells every input that is still open to stop producing and reads it to its EOS (discarding any
  /// data), so that the upstream stats carried by the EOS blocks are merged. The result is a success EOS unless a
  /// drained input ends in an error, which is propagated. Buffers are released by [#getNextBlock()].
  private void finish() {
    MseBlock.Eos eos = SuccessMseBlock.INSTANCE;
    for (Cursor cursor : new Cursor[]{_leftCursor, _rightCursor}) {
      if (!cursor.isExhausted()) {
        cursor.getInput().earlyTerminate();
        cursor.drain();
      }
      if (cursor.isError() && !eos.isError()) {
        eos = cursor.getEos();
      }
    }
    _eos = eos;
  }

  private void clearRightRun() {
    _rightRun.clear();
    _rightRunAnchor = null;
    _rightRunIndex = 0;
    _leftRowMatched = false;
  }

  /// Buffers the run of right rows that share the join key of `anchor` (reusing [#_rightRun] across keys). Matching
  /// against the run is resumable: [#emitMatchedKey] walks it across as many output blocks as needed.
  ///
  /// This buffer is the operator's peak memory: a single hot join key with a very large right-side run would
  /// otherwise grow it without bound before a single output row is produced. It is the analog of the hash join's right
  /// table, so it is capped by `maxRowsInJoin` buffered rows, honouring `joinOverflowMode` like the hash join's build
  /// side: THROW aborts the query, BREAK truncates the run at the cap, skips the rest of that key's right rows and
  /// keeps merging against the truncated run (results may be partial).
  ///
  /// Returns `false` if the operator terminated while buffering (`_eos` is set).
  private boolean bufferRightRun(Object[] anchor) {
    List<Object[]> rightRun = _rightRun;
    rightRun.clear();
    boolean truncated = false;
    while (_rightCursor.advanceToNextRow()) {
      Object[] rightRow = _rightCursor.peek();
      if (compareKeys(anchor, _leftKeyIds, rightRow, _rightKeyIds) != 0) {
        break;
      }
      if (truncated) {
        // Past the cap in BREAK mode: skip the remaining rows of this key.
        _rightCursor.consume();
      } else if (rightRun.size() >= _maxRowsInJoin) {
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          // Record the stat before throwing so it propagates to the query response.
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) rightRun.size() + 1);
          BaseJoinOperator.throwForJoinRowLimitExceeded(
              "Cannot process sorted merge join, reached number of rows limit while buffering the right rows for a "
                  + "single join key: " + _maxRowsInJoin);
        }
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) rightRun.size());
        truncated = true;
        _rightCursor.consume();
      } else {
        rightRun.add(rightRow);
        _rightCursor.consume();
      }
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, BUFFER_RIGHT_RUN_SCOPE);
    }
    if (_rightCursor.isError()) {
      _eos = _rightCursor.getEos();
      return false;
    }
    _rightRunAnchor = anchor;
    _rightRunIndex = 0;
    _leftRowMatched = false;
    return true;
  }

  /// Emits the pairs of `leftRow` (the current left row, whose key equals the buffered right run's) with the right
  /// run, resuming at [#_rightRunIndex] and stopping once the output block is full. The left row is consumed only
  /// when its whole run was walked. [#_leftRowMatched] survives block boundaries, so a LEFT join pads the row with
  /// nulls exactly once, and only after every residual-filter candidate was rejected.
  ///
  /// Returns `false` if the operator terminated while emitting (`_eos` is set).
  private boolean emitMatchedKey(List<Object[]> rows, Object[] leftRow) {
    List<Object[]> rightRun = _rightRun;
    int runSize = rightRun.size();
    while (_rightRunIndex < runSize && rows.size() < TARGET_BLOCK_SIZE_ROWS) {
      Object[] rightRow = rightRun.get(_rightRunIndex++);
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, EMIT_MATCHED_KEY_SCOPE);
      if (bigDecimalKeysEqual(leftRow, rightRow)
          && (!_hasNonEquiConditions || matchNonEquiConditions(_pairView.of(leftRow, rightRow)))) {
        if (!addRow(rows, joinRow(leftRow, rightRow))) {
          return false;
        }
        _leftRowMatched = true;
      }
    }
    if (_rightRunIndex == runSize) {
      if (!_leftRowMatched && _needUnmatchedLeftRows && !addRow(rows, joinRow(leftRow, null))) {
        return false;
      }
      _leftCursor.consume();
      _rightRunIndex = 0;
      _leftRowMatched = false;
    }
    return true;
  }

  /// Appends a joined row to the current output block, applying the `maxRowsInJoin` limit to the number of rows of
  /// the block, like the hash join does for the rows produced for a left block. Returns `false` if the limit was
  /// reached in BREAK mode, in which case the operator has already been terminated and callers must stop producing;
  /// THROW mode raises instead of returning.
  private boolean addRow(List<Object[]> rows, Object[] row) {
    if (isMaxRowsLimitReached(rows.size())) {
      return false;
    }
    rows.add(row);
    return true;
  }

  /// Mirrors [BaseJoinOperator#isMaxRowsLimitReached(int)] for the rows of the current output block.
  private boolean isMaxRowsLimitReached(int numJoinedRows) {
    if (numJoinedRows < _maxRowsInJoin) {
      return false;
    }
    if (_joinOverflowMode == JoinOverFlowMode.THROW) {
      _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) numJoinedRows);
      BaseJoinOperator.throwForJoinRowLimitExceeded(
          "Cannot process sorted merge join, reached number of rows limit: " + _maxRowsInJoin);
    }
    // BREAK mode: end the join with the rows produced so far, draining the inputs so their stats are kept.
    LOGGER.info("Terminating sorted merge join early as the maximum number of rows limit was reached: {}",
        _maxRowsInJoin);
    _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) numJoinedRows);
    _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
    earlyTerminate();
    finish();
    return true;
  }

  /// Hash join matches keys with `equals()`, which is scale-sensitive for `BigDecimal`, while the merge orders by
  /// `compareTo()` (`1.0` and `1.00` compare equal). Rows whose BIG_DECIMAL key columns are equal by `compareTo` but
  /// not by `equals` must not be joined.
  private boolean bigDecimalKeysEqual(Object[] leftRow, Object[] rightRow) {
    for (int i : _bigDecimalKeyIndexes) {
      if (!leftRow[_leftKeyIds[i]].equals(rightRow[_rightKeyIds[i]])) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasNullKey(Object[] row, int[] keyIds) {
    for (int keyId : keyIds) {
      if (row[keyId] == null) {
        return true;
      }
    }
    return false;
  }

  /// Compares the join keys of two rows column by column using nulls-last ordering, matching the `NullDirection.LAST`
  /// collation the planner requests for both join inputs and the natural order of each stored type (the same
  /// `compareTo` the sorts use, so e.g. `NaN` sorts last and `-0.0` before `0.0`). Null keys never match (handled by
  /// the caller), but they must still compare consistently: a null-key row sorts after every non-null key, so a run
  /// scan that reaches one terminates the run rather than dereferencing it.
  private int compareKeys(Object[] row1, int[] keyIds1, Object[] row2, int[] keyIds2) {
    for (int i = 0; i < keyIds1.length; i++) {
      Object value1 = row1[keyIds1[i]];
      Object value2 = row2[keyIds2[i]];
      int result;
      if (value1 == null) {
        result = value2 == null ? 0 : 1;
      } else if (value2 == null) {
        result = -1;
      } else {
        result = _keyComparators[i].compare(value1, value2);
      }
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  /// Compares two non-null key values of one stored type.
  private interface ValueComparator {
    int compare(Object value1, Object value2);

    @SuppressWarnings({"unchecked", "rawtypes"})
    static ValueComparator of(ColumnDataType storedType) {
      switch (storedType) {
        case INT:
          return (v1, v2) -> Integer.compare((Integer) v1, (Integer) v2);
        case LONG:
          return (v1, v2) -> Long.compare((Long) v1, (Long) v2);
        case FLOAT:
          return (v1, v2) -> Float.compare((Float) v1, (Float) v2);
        case DOUBLE:
          return (v1, v2) -> Double.compare((Double) v1, (Double) v2);
        default:
          // STRING, BIG_DECIMAL, BYTES (ByteArray), etc. are all Comparable in their stored representation.
          return (v1, v2) -> ((Comparable) v1).compareTo(v2);
      }
    }
  }

  /// A mutable, reusable view over a left and right row joined together, so the residual predicate can be evaluated
  /// without allocating a view (or the joined row) per candidate pair. It must not be retained by the evaluators.
  private static final class PairView extends AbstractList<Object> implements RandomAccess {
    private final int _leftSize;
    private final int _size;
    private Object[] _leftRow;
    private Object[] _rightRow;

    PairView(int leftSize, int size) {
      _leftSize = leftSize;
      _size = size;
    }

    PairView of(Object[] leftRow, Object[] rightRow) {
      _leftRow = leftRow;
      _rightRow = rightRow;
      return this;
    }

    @Override
    public Object get(int i) {
      return i < _leftSize ? _leftRow[i] : _rightRow[i - _leftSize];
    }

    @Override
    public int size() {
      return _size;
    }
  }

  private Object[] joinRow(Object[] leftRow, @Nullable Object[] rightRow) {
    Object[] resultRow = new Object[_resultColumnSize];
    System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
    if (rightRow != null) {
      System.arraycopy(rightRow, 0, resultRow, _leftColumnSize, rightRow.length);
    }
    return resultRow;
  }

  private boolean matchNonEquiConditions(List<Object> row) {
    for (TransformOperand evaluator : _nonEquiEvaluators) {
      if (!BooleanUtils.isTrueInternalValue(evaluator.apply(row))) {
        return false;
      }
    }
    return true;
  }

  /// A lazy, block-at-a-time cursor over a [MultiStageOperator] input. Only one data block is held in memory at a
  /// time. Use [#advanceToNextRow()] to ensure a current row is available, [#peek()] to read it without
  /// consuming, and [#consume()] to move to the next row.
  private final class Cursor {
    private final MultiStageOperator _input;
    private List<Object[]> _rows = List.of();
    private int _index;
    @Nullable
    private MseBlock.Eos _eosBlock;

    Cursor(MultiStageOperator input) {
      _input = input;
    }

    MultiStageOperator getInput() {
      return _input;
    }

    /// Ensures a current row is available, fetching subsequent blocks as needed. Returns `false` when the input
    /// is exhausted (an EOS block was encountered), in which case [#getEos()] is populated.
    boolean advanceToNextRow() {
      while (_index >= _rows.size()) {
        if (_eosBlock != null) {
          return false;
        }
        MseBlock block = _input.nextBlock();
        if (block.isData()) {
          _rows = ((MseBlock.Data) block).asRowHeap().getRows();
          _index = 0;
        } else {
          _eosBlock = (MseBlock.Eos) block;
          _rows = List.of();
          _index = 0;
          return false;
        }
      }
      return true;
    }

    Object[] peek() {
      return _rows.get(_index);
    }

    void consume() {
      _index++;
    }

    boolean isError() {
      return _eosBlock != null && _eosBlock.isError();
    }

    boolean isExhausted() {
      return _eosBlock != null;
    }

    /// Reads the input to its EOS, discarding any data, and records that EOS. The input should already have been
    /// early-terminated. Reading the EOS is what makes an upstream mailbox merge the stats of its sender.
    void drain() {
      releaseRows();
      MseBlock block = _input.nextBlock();
      while (block.isData()) {
        block = _input.nextBlock();
      }
      _eosBlock = (MseBlock.Eos) block;
    }

    /// Drops the block this cursor points at. The list belongs to the input block, so replace, never clear.
    /// `_eosBlock` is left alone: it is the exhaustion flag, not buffered rows.
    void releaseRows() {
      _rows = List.of();
      _index = 0;
    }

    boolean hasBufferedRows() {
      return !_rows.isEmpty();
    }

    @Nullable
    MseBlock.Eos getEos() {
      return _eosBlock;
    }
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    MAX_ROWS_IN_JOIN_REACHED(StatMap.Type.BOOLEAN),
    /// The max number of rows buffered for a right key run or emitted in an output block, whichever hit the limit.
    MAX_ROWS_IN_JOIN(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return Math.max(value1, value2);
      }
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
