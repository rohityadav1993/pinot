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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.JoinHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.BaseJoinOperator.StatKey;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;


/**
 * Assumes input is sorted appropriately, and runs a merge join algorithm.
 * TODO: I am skipping past duplicates right now.
 */
public class SortedMergeJoinOperator extends MultiStageOperator {
  private final MultiStageOperator _leftInput;
  private final MultiStageOperator _rightInput;
  private final JoinRelType _joinRelType;
  protected final DataSchema _resultSchema;
  protected final int _leftColumnSize;
  protected final int _resultColumnSize;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);
  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  /**
   * Max rows allowed to build the right table hash collection. Also max rows emitted in each join with a block from
   * the left table.
   */
  private final int _maxRowsInJoin;
  /**
   * Mode when join overflow happens, supported values: THROW or BREAK.
   *   THROW(default): Break right table build process, and throw exception, no JOIN with left table performed.
   *   BREAK: Break right table build process, continue to perform JOIN operation, results might be partial.
   */
  private final JoinOverFlowMode _joinOverflowMode;
  @Nullable
  private MseBlock.Eos _eos;

  // Iterator state for streaming merge — initialized lazily on first getNextBlock() call
  @Nullable private LazyBlockIterator _lazyLeftIter;
  @Nullable private LazyBlockIterator _lazyRightIter;
  @Nullable private Iterator<Object[]> _leftIter;
  @Nullable private Iterator<Object[]> _rightIter;
  @Nullable private Object[] _leftRow;
  @Nullable private Object[] _rightRow;

  public SortedMergeJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    _leftInput = leftInput;
    _rightInput = rightInput;
    _joinRelType = node.getJoinType();
    _resultSchema = node.getDataSchema();
    _leftColumnSize = node.getInputs().get(0).getDataSchema().size();
    _resultColumnSize = _resultSchema.size();
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInJoin = getMaxRowsInJoin(metadata, nodeHint);
    _joinOverflowMode = getJoinOverflowMode(metadata, nodeHint);
    _leftKeySelector = KeySelectorFactory.getKeySelector(node.getLeftKeys());
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
  }

  @Override
  protected Logger logger() {
    return BaseJoinOperator.LOGGER;
  }

  @Override
  public Type getOperatorType() {
    return Type.HASH_JOIN;
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  protected MseBlock getNextBlock()
      throws Exception {
    if (_eos != null) {
      return _eos;
    }
    // Lazy init: create block-pull iterators on first call
    if (_lazyLeftIter == null) {
      _lazyLeftIter = new LazyBlockIterator(_leftInput);
      _lazyRightIter = new LazyBlockIterator(_rightInput);
      _leftIter = new DedupIterator(_leftKeySelector, _lazyLeftIter);
      _rightIter = new DedupIterator(_rightKeySelector, _lazyRightIter);
    }
    switch (_joinRelType) {
      case LEFT:
        return computeLeftJoin();
      default:
        throw new UnsupportedOperationException("Only left join supported right now");
    }
  }

  protected MseBlock computeLeftJoin() {
    List<Object[]> result = new ArrayList<>(10_000);
    while (_leftRow != null || _leftIter.hasNext()) {
      if (_leftRow == null) {
        _leftRow = _leftIter.next();
      }
      // Eagerly propagate right-side errors
      if (_lazyRightIter.isError()) {
        _eos = _lazyRightIter.getEos();
        return _eos;
      }
      if (_rightRow == null) {
        _rightRow = _rightIter.hasNext() ? _rightIter.next() : null;
      }
      if (_rightRow == null) {
        // Right side exhausted — emit unmatched left row
        Object[] newRow = new Object[_resultColumnSize];
        System.arraycopy(_leftRow, 0, newRow, 0, _leftColumnSize);
        result.add(newRow);
        _leftRow = null;
        continue;
      }
      Comparable leftKey = (Comparable) _leftKeySelector.getKey(_leftRow);
      Comparable rightKey = (Comparable) _rightKeySelector.getKey(_rightRow);
      if (isNullKey(leftKey)) {
        _leftRow = null;
        continue;
      }
      if (isNullKey(rightKey)) {
        _rightRow = null;
        continue;
      }
      int c = leftKey.compareTo(rightKey);
      if (c == 0) {
        Object[] newRow = new Object[_resultColumnSize];
        System.arraycopy(_leftRow, 0, newRow, 0, _leftColumnSize);
        System.arraycopy(_rightRow, 0, newRow, _leftColumnSize, _resultColumnSize - _leftColumnSize);
        result.add(newRow);
        _leftRow = null;
        _rightRow = null;
      } else if (c > 0) {
        _rightRow = null;
      } else {
        Object[] newRow = new Object[_resultColumnSize];
        System.arraycopy(_leftRow, 0, newRow, 0, _leftColumnSize);
        result.add(newRow);
        _leftRow = null;
      }
    }
    // Left exhausted — set EOS; prefer error EOS over success
    MseBlock.Eos leftEos = _lazyLeftIter.getEos();
    MseBlock.Eos rightEos = _lazyRightIter.getEos();
    if (leftEos != null && leftEos.isError()) {
      _eos = leftEos;
    } else if (rightEos != null && rightEos.isError()) {
      _eos = rightEos;
    } else {
      _eos = leftEos != null ? leftEos : SuccessMseBlock.INSTANCE;
    }
    return new RowHeapDataBlock(result, _resultSchema);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftInput, _rightInput);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return "MERGE_JOIN";
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  protected static int getMaxRowsInJoin(Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String maxRowsInJoinStr = joinOptions.get(JoinHintOptions.MAX_ROWS_IN_JOIN);
        if (maxRowsInJoinStr != null) {
          return Integer.parseInt(maxRowsInJoinStr);
        }
      }
    }
    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(opChainMetadata);
    return maxRowsInJoin != null ? maxRowsInJoin : BaseJoinOperator.DEFAULT_MAX_ROWS_IN_JOIN;
  }

  protected static JoinOverFlowMode getJoinOverflowMode(Map<String, String> contextMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String joinOverflowModeStr = joinOptions.get(JoinHintOptions.JOIN_OVERFLOW_MODE);
        if (joinOverflowModeStr != null) {
          return JoinOverFlowMode.valueOf(joinOverflowModeStr);
        }
      }
    }
    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(contextMetadata);
    return joinOverflowMode != null ? joinOverflowMode : BaseJoinOperator.DEFAULT_JOIN_OVERFLOW_MODE;
  }

  /**
   * Check if a join key contains null values. In SQL standard, null keys should not match in equi-joins.
   **/
  private static boolean isNullKey(Object key) {
    if (key == null) {
      return true;
    }
    if (key instanceof Key) {
      Object[] components = ((Key) key).getValues();
      for (Object comp : components) {
        if (comp == null) {
          return true;
        }
      }
      return false;
    }
    // For single keys (non-composite), key == null is already checked above
    return false;
  }

  static class DedupIterator implements Iterator<Object[]> {
    private final KeySelector<?> _keySelector;
    private final Iterator<Object[]> _iterator;
    Holder _cached = new Holder();

    DedupIterator(KeySelector<?> keySelector, Iterator<Object[]> iterator) {
      _keySelector = keySelector;
      _iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return _cached._isSet || _iterator.hasNext();
    }

    @Override
    public Object[] next() {
      Object[] ret = null;
      if (_cached._isSet) {
        ret = _cached._row;
        _cached.clear();
      } else {
        Preconditions.checkState(_iterator.hasNext(), "No more elements");
        ret = _iterator.next();
      }
      while (_iterator.hasNext()) {
        Object[] tmp = _iterator.next();
        if (!Objects.equals(_keySelector.getKey(ret), _keySelector.getKey(tmp))) {
          _cached.set(tmp);
          break;
        }
      }
      return ret;
    }
  }

  static class Holder {
    Object[] _row = null;
    boolean _isSet = false;

    void set(Object[] row) {
      _row = row;
      _isSet = true;
    }

    void clear() {
      _row = null;
      _isSet = false;
    }
  }

  /**
   * Lazily fetches blocks from a {@link MultiStageOperator}, exposing them as a flat {@link Iterator}.
   * Only one block is held in memory at a time; the next block is fetched when the current one is exhausted.
   */
  static class LazyBlockIterator implements Iterator<Object[]> {
    private final MultiStageOperator _input;
    private Iterator<Object[]> _current = java.util.Collections.emptyIterator();
    private boolean _done = false;
    @Nullable
    private MseBlock.Eos _eos;

    LazyBlockIterator(MultiStageOperator input) {
      _input = input;
    }

    @Override
    public boolean hasNext() {
      while (!_current.hasNext() && !_done) {
        MseBlock block;
        try {
          block = _input.nextBlock();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        if (block.isData()) {
          _current = ((MseBlock.Data) block).asRowHeap().getRows().iterator();
        } else {
          _eos = (MseBlock.Eos) block;
          _done = true;
        }
      }
      return _current.hasNext();
    }

    @Override
    public Object[] next() {
      return _current.next();
    }

    @Nullable
    MseBlock.Eos getEos() {
      return _eos;
    }

    boolean isError() {
      return _eos != null && _eos.isError();
    }
  }
}
