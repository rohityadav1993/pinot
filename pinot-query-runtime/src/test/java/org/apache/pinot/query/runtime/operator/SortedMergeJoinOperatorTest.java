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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Unit tests for [SortedMergeJoinOperator]. Inputs are always provided pre-sorted (ascending) on the join keys,
/// which is the operator's precondition. Several tests assert that the streaming sorted merge join produces the same
/// result multiset as [HashJoinOperator] for the same inputs.
public class SortedMergeJoinOperatorTest {
  // The operator's output block cap.
  private static final int MAX_BLOCK_ROWS = 1024;
  private static final DataSchema CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
  private static final DataSchema RESULT_SCHEMA = new DataSchema(
      new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

  @Test
  public void shouldHandleInnerJoinOnInt() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(rows.get(1), new Object[]{2, "BB", 2, "BB"});
  }

  @Test
  public void shouldHandleKeyCollisionOneToMany() {
    // Inputs sorted ascending on the string join key (column 1).
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(rows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(rows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleManyToMany() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    // 2 x 2 cross product for the single shared key.
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, "k", 10, "k"});
    assertEquals(rows.get(1), new Object[]{1, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 10, "k"});
    assertEquals(rows.get(3), new Object[]{2, "k", 20, "k"});
  }

  @Test
  public void shouldHandleLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "CC")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(rows.get(1), new Object[]{2, "CC", null, null});
  }

  @Test
  public void shouldHandleEmptyLeftInput() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    assertEquals(drain(operator).size(), 0);
  }

  @Test
  public void shouldHandleEmptyRightInputInnerJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    assertEquals(drain(operator).size(), 0);
  }

  @Test
  public void shouldHandleLongKeysNearPrecisionBoundary() {
    DataSchema schema = new DataSchema(new String[]{"long_col", "string_col"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING});
    DataSchema resultSchema = new DataSchema(
        new String[]{"l_long", "l_str", "r_long", "r_str"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.STRING});
    long base = 1L << 53;
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(schema)
        .addRow(base, "a")
        .addRow(base + 1, "b")
        .addRow(base + 2, "c")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(schema)
        .addRow(base + 1, "x")
        .addRow(base + 2, "y")
        .buildWithEos();
    SortedMergeJoinOperator operator = new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left, schema,
        right, new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.INNER, List.of(0),
            List.of(0), List.of(), JoinNode.JoinStrategy.SORTED));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{base + 1, "b", base + 1, "x"});
    assertEquals(rows.get(1), new Object[]{base + 2, "c", base + 2, "y"});
  }

  @Test
  public void shouldRespectMaxRowsInJoinThrowMode() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .addRow(3, "k")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1), nodeHint);
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError(), "THROW overflow mode should produce an error block");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void shouldRejectRightJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    getOperator(left, right, RESULT_SCHEMA, JoinRelType.RIGHT, List.of(0), List.of(0));
  }

  @Test
  public void shouldHandleEmptyRightInputLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(0), List.of(0));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "Aa", null, null});
    assertEquals(rows.get(1), new Object[]{2, "BB", null, null});
  }

  @Test
  public void shouldHandleMultiBlockStreamingInputs() {
    // Rows are split across multiple blocks on both sides; the merge must span block boundaries, and a single key
    // run ("k") must be assembled from rows arriving in different blocks.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "k")
        .finishBlock()
        .addRow(3, "k")
        .addRow(4, "z")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "a")
        .finishBlock()
        .addRow(20, "k")
        .finishBlock()
        .addRow(30, "k")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 5);
    assertEquals(rows.get(0), new Object[]{1, "a", 10, "a"});
    assertEquals(rows.get(1), new Object[]{2, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 30, "k"});
    assertEquals(rows.get(3), new Object[]{3, "k", 20, "k"});
    assertEquals(rows.get(4), new Object[]{3, "k", 30, "k"});
  }

  @Test
  public void shouldHandleMultiColumnKey() {
    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    DataSchema resultSchema = new DataSchema(
        new String[]{"l_int", "l_str", "r_int", "r_str"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    // Sorted ascending on (int_col, string_col).
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "a")
        .addRow(1, "b")
        .addRow(2, "a")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, "a")
        .addRow(1, "c")
        .addRow(2, "a")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, resultSchema, JoinRelType.INNER, List.of(0, 1), List.of(0, 1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "a", 1, "a"});
    assertEquals(rows.get(1), new Object[]{2, "a", 2, "a"});
  }

  @Test
  public void shouldExcludeNullKeysFromInnerJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, null)
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(3, null)
        .addRow(4, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{2, "BB", 4, "BB"});
  }

  @Test
  public void shouldPreserveNullKeyLeftRowsInLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, null)
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(3, null)
        .addRow(4, "BB")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertTrue(containsRow(rows, new Object[]{1, null, null, null}));
    assertTrue(containsRow(rows, new Object[]{2, "BB", 4, "BB"}));
  }

  /// Regression: the planner asks for NULLS LAST collation on both join inputs
  /// (`PinotJoinExchangeNodeInsertRule`), so a null-key row arrives immediately *after* the last non-null run — which
  /// is exactly where the run-scanning loops call `compareKeys` on it. A key comparator that unboxes or calls
  /// `compareTo` without a null check throws NPE there and fails the query for any nullable join key. The two tests
  /// above place nulls first, the opposite of the real ordering, so they cannot catch this.
  @Test
  public void shouldExcludeNullKeysSortedLastFromInnerJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "BB")
        .addRow(1, null)
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(4, "BB")
        .addRow(3, null)
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{2, "BB", 4, "BB"});
  }

  @Test
  public void shouldPreserveNullKeysSortedLastInLeftJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(2, "BB")
        .addRow(1, null)
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(4, "BB")
        .addRow(3, null)
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertTrue(containsRow(rows, new Object[]{2, "BB", 4, "BB"}));
    assertTrue(containsRow(rows, new Object[]{1, null, null, null}));
  }

  /// A null key on the right side alone, sorted last, must terminate the right run without being dereferenced.
  @Test
  public void shouldTerminateRightRunAtNullKeySortedLast() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "AA")
        .addRow(2, "BB")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "AA")
        .addRow(11, "AA")
        .addRow(12, "BB")
        .addRow(13, null)
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 3);
    assertTrue(containsRow(rows, new Object[]{1, "AA", 10, "AA"}));
    assertTrue(containsRow(rows, new Object[]{1, "AA", 11, "AA"}));
    assertTrue(containsRow(rows, new Object[]{2, "BB", 12, "BB"}));
  }

  @Test
  public void shouldRespectMaxRowsInJoinBreakMode() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .addRow(3, "k")
        .addRow(4, "k")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "5")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1), nodeHint);
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 5, "Should emit exactly the limit then break (potential 4 x 2 = 8)");
    StatMap<SortedMergeJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(SortedMergeJoinOperator.StatKey.class, operator.calculateStats());
    assertTrue(statMap.getBoolean(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
  }

  /// Guards against the sorted merge join ignoring downstream early termination. The inputs are large enough to span
  /// several output blocks (many distinct keys, 1:1 matches), so after the first data block there are still more blocks
  /// to produce. Before the fix, calling [MultiStageOperator#earlyTerminate()] had no effect and the next
  /// `nextBlock()` would return another data block from the remaining input; now it must return a clean success
  /// EOS promptly.
  @Test
  public void shouldStopProducingAfterEarlyTerminate() {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    // Distinct, ascending, zero-padded string keys so lexicographic order matches insertion order (the operator
    // requires inputs pre-sorted on the join key). 4000 keys with 1:1 matches produce ~4 output blocks.
    for (int i = 0; i < 4000; i++) {
      String key = String.format("k%04d", i);
      leftBuilder.addRow(i, key);
      rightBuilder.addRow(i, key);
    }
    MultiStageOperator left = leftBuilder.buildWithEos();
    MultiStageOperator right = rightBuilder.buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));

    MseBlock first = operator.nextBlock();
    assertTrue(first.isData());

    operator.earlyTerminate();

    MseBlock next = operator.nextBlock();
    assertTrue(next.isEos());
    assertFalse(next.isError());
    assertSame(next, SuccessMseBlock.INSTANCE);
  }

  @Test
  public void shouldMatchHashJoinResultsForRandomizedInnerJoin() {
    assertParityWithHashJoin(JoinRelType.INNER, 12345L);
  }

  @Test
  public void shouldMatchHashJoinResultsForRandomizedLeftJoin() {
    assertParityWithHashJoin(JoinRelType.LEFT, 67890L);
  }

  private void assertParityWithHashJoin(JoinRelType joinType, long seed) {
    Random random = new Random(seed);
    List<Object[]> leftRows = randomRows(random, 200, 40);
    List<Object[]> rightRows = randomRows(random, 200, 40);
    Object[][] sortedLeft = sortByKey(leftRows).toArray(new Object[0][]);
    Object[][] sortedRight = sortByKey(rightRows).toArray(new Object[0][]);

    // Sorted merge join (requires sorted inputs).
    SortedMergeJoinOperator sortedOperator = getOperator(
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedLeft).buildWithEos(),
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedRight).buildWithEos(),
        RESULT_SCHEMA, joinType, List.of(0), List.of(0));
    List<Object[]> sortedResult = drain(sortedOperator);

    // Hash join over the same data (order-independent).
    HashJoinOperator hashOperator = new HashJoinOperator(OperatorTestUtil.getTracingContext(),
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedLeft).buildWithEos(), CHILD_SCHEMA,
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addBlock(sortedRight).buildWithEos(),
        new JoinNode(-1, RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), joinType, List.of(0), List.of(0),
            List.of(), JoinNode.JoinStrategy.HASH));
    List<Object[]> hashResult = new ArrayList<>();
    MseBlock hashBlock = hashOperator.nextBlock();
    while (hashBlock.isData()) {
      hashResult.addAll(((MseBlock.Data) hashBlock).asRowHeap().getRows());
      hashBlock = hashOperator.nextBlock();
    }
    assertFalse(hashBlock.isError());

    assertEquals(toMultiset(sortedResult), toMultiset(hashResult),
        "Sorted merge join must produce the same result multiset as hash join");
    // Sorted merge join output must be ordered by the (int) join key.
    for (int i = 1; i < sortedResult.size(); i++) {
      Integer prev = (Integer) sortedResult.get(i - 1)[0];
      Integer curr = (Integer) sortedResult.get(i)[0];
      assertTrue(prev <= curr, "Sorted merge join output must be ascending on the join key");
    }
  }

  private static List<Object[]> randomRows(Random random, int numRows, int keyRange) {
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(new Object[]{random.nextInt(keyRange), "v" + i});
    }
    return rows;
  }

  private static List<Object[]> sortByKey(List<Object[]> rows) {
    List<Object[]> copy = new ArrayList<>(rows);
    copy.sort(Comparator.comparingInt(r -> (Integer) r[0]));
    return copy;
  }

  private static List<String> toMultiset(List<Object[]> rows) {
    List<String> repr = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      repr.add(Arrays.toString(row));
    }
    repr.sort(Comparator.naturalOrder());
    return repr;
  }

  private static boolean containsRow(List<Object[]> rows, Object[] expected) {
    for (Object[] row : rows) {
      if (Arrays.equals(row, expected)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void shouldPropagateLeftInputError() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testLeftError")));
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "b")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages()
        .get(QueryErrorCode.UNKNOWN).contains("testLeftError"));
  }

  @Test
  public void shouldPropagateRightInputError() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "b")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testRightError")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0));
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages()
        .get(QueryErrorCode.UNKNOWN).contains("testRightError"));
  }

  @Test
  public void shouldHandleLeftJoinManyToMany() {
    // LEFT JOIN with multiple left rows and multiple right rows sharing the same key, plus an additional key
    // to verify correct cursor advancement after the many-to-many group.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .addRow(3, "m")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .addRow(30, "m")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    // 2 left x 2 right for "k" = 4 rows, plus 1 left x 1 right for "m" = 1 row => 5 total
    assertEquals(rows.size(), 5);
    assertEquals(rows.get(0), new Object[]{1, "k", 10, "k"});
    assertEquals(rows.get(1), new Object[]{1, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 10, "k"});
    assertEquals(rows.get(3), new Object[]{2, "k", 20, "k"});
    assertEquals(rows.get(4), new Object[]{3, "m", 30, "m"});
  }

  @Test
  public void shouldHandleLeftJoinManyToManyAcrossBlocks() {
    // LEFT JOIN where left rows sharing a key span across block boundaries, and the right run also spans blocks.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .finishBlock()
        .addRow(2, "k")
        .addRow(3, "m")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .finishBlock()
        .addRow(20, "k")
        .addRow(30, "m")
        .buildWithEos();
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1));
    List<Object[]> rows = drain(operator);
    // 2 left x 2 right for "k" = 4 rows, plus 1 left x 1 right for "m" = 1 row => 5 total
    assertEquals(rows.size(), 5);
    assertEquals(rows.get(0), new Object[]{1, "k", 10, "k"});
    assertEquals(rows.get(1), new Object[]{1, "k", 20, "k"});
    assertEquals(rows.get(2), new Object[]{2, "k", 10, "k"});
    assertEquals(rows.get(3), new Object[]{2, "k", 20, "k"});
    assertEquals(rows.get(4), new Object[]{3, "m", 30, "m"});
  }

  @Test
  public void shouldApplyNonEquiCondition() {
    // Both sides share key=1 with different string values; the non-equi condition filters some matches.
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(1, "b")
        .buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(1, "c")
        .buildWithEos();
    // Non-equi condition: left.string_col (result col 1) != right.string_col (result col 3)
    RexExpression nonEqui = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(),
        List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(3)));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(0), List.of(0), List.of(nonEqui));
    List<Object[]> rows = drain(operator);
    // (1,"a") x (1,"c") matches (a != c), (1,"b") x (1,"a") matches (b != a), (1,"b") x (1,"c") matches (b != c)
    // (1,"a") x (1,"a") does NOT match (a == a)
    assertEquals(rows.size(), 3);
  }

  /// Drains the operator to a success EOS, asserting that every block is bounded and that the buffers are released.
  private static List<Object[]> drain(MultiStageOperator operator) {
    List<Object[]> rows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      assertTrue(((MseBlock.Data) block).getNumRows() <= MAX_BLOCK_ROWS, "Output blocks must be bounded");
      rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertFalse(block.isError(), "Did not expect an error block");
    assertFalse(operator.hasBufferedState(), "Buffers must be released at EOS");
    return rows;
  }

  /// Reads blocks until the first EOS and returns it.
  private static MseBlock.Eos drainToEos(MultiStageOperator operator) {
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      block = operator.nextBlock();
    }
    return (MseBlock.Eos) block;
  }

  private static RexExpression compare(SqlKind kind, RexExpression left, RexExpression right) {
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, kind.name(), List.of(left, right));
  }

  private static PlanNode.NodeHint maxRowsInJoinHint(int maxRowsInJoin, String overflowMode) {
    return new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, Integer.toString(maxRowsInJoin),
            PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, overflowMode)));
  }

  @DataProvider
  public Object[][] residualFilter() {
    return new Object[][]{{false}, {true}};
  }

  /// A duplicate-key cross product must resume across output blocks (the left run spans two input blocks, the right
  /// run two input blocks), for both the equi-only and the residual path, with a trailing key to prove the cursors
  /// are positioned correctly after the resumed run. With the residual, the trailing left row is rejected and padded.
  @Test(dataProvider = "residualFilter")
  public void shouldStreamDuplicateKeyAcrossOutputBlocks(boolean withResidualFilter) {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(5, "k")
        .finishBlock()
        .addRow(6, "k")
        .addRow(7, "m")
        .buildWithEos();
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 1500; i++) {
      rightBuilder.addRow(10 + i, "k");
      if (i == 749) {
        rightBuilder.finishBlock();
      }
    }
    MultiStageOperator right = rightBuilder.addRow(0, "m").buildWithEos();
    List<RexExpression> residuals = withResidualFilter
        ? List.of(compare(SqlKind.LESS_THAN, new RexExpression.InputRef(0), new RexExpression.InputRef(2))) : List.of();
    SortedMergeJoinOperator operator = new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left,
        CHILD_SCHEMA, right, new JoinNode(-1, RESULT_SCHEMA, maxRowsInJoinHint(10_000, "THROW"), List.of(),
        JoinRelType.LEFT, List.of(1), List.of(1), residuals, JoinNode.JoinStrategy.SORTED));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 3001);
    for (int i = 0; i < 3000; i++) {
      assertEquals(rows.get(i), new Object[]{i < 1500 ? 5 : 6, "k", 10 + i % 1500, "k"});
    }
    assertEquals(rows.get(3000), withResidualFilter
        ? new Object[]{7, "m", null, null} : new Object[]{7, "m", 0, "m"});
  }

  /// Guards the resumable emission: a hot key whose cross product is 9M rows (far above the default maxRowsInJoin of
  /// 1M, so a build-the-whole-product operator would throw) must return one bounded block and then stop on early
  /// termination, the way a downstream LIMIT drives it.
  @Test
  public void shouldStopInsideHotKeyOnEarlyTerminate() {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 3000; i++) {
      leftBuilder.addRow(i, "k");
      rightBuilder.addRow(i, "k");
    }
    SortedMergeJoinOperator operator = getOperator(leftBuilder.buildWithEos(), rightBuilder.buildWithEos(),
        RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    MseBlock first = operator.nextBlock();
    assertTrue(first.isData(), "A hot key must not be materialized before the first block is returned");
    assertEquals(((MseBlock.Data) first).getNumRows(), MAX_BLOCK_ROWS);
    operator.earlyTerminate();
    MseBlock next = operator.nextBlock();
    assertSame(next, SuccessMseBlock.INSTANCE);
    assertFalse(operator.hasBufferedState());
  }

  /// A LEFT row must be null-padded exactly once, and only if every residual candidate was rejected, even when its
  /// run of candidates is walked across several output blocks. Left rows (5, k) and (6, k) each accept exactly the
  /// first 1024 of 2500 right rows: the output block fills on the last accepted candidate, so the remaining
  /// candidates (all rejected) are walked in the next block, and the match flag has to survive that boundary
  /// (otherwise the row would be padded again). Key "q" rejects every candidate: one padded row.
  @Test
  public void shouldPadLeftRowOnceWhenResidualRejectsAcrossBlocks() {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(5, "k")
        .addRow(6, "k")
        .addRow(8, "q")
        .addRow(9, "z");
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 2500; i++) {
      rightBuilder.addRow(i, "k");
    }
    for (int i = 0; i < 3; i++) {
      rightBuilder.addRow(5000 + i, "q");
    }
    RexExpression residual = compare(SqlKind.LESS_THAN, new RexExpression.InputRef(2),
        new RexExpression.Literal(ColumnDataType.INT, 1024));
    SortedMergeJoinOperator operator = getOperator(leftBuilder.buildWithEos(), rightBuilder.buildWithEos(),
        RESULT_SCHEMA, JoinRelType.LEFT, List.of(1), List.of(1), List.of(residual));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2050);
    for (int i = 0; i < 2048; i++) {
      assertEquals(rows.get(i), new Object[]{i < 1024 ? 5 : 6, "k", i % 1024, "k"});
    }
    assertEquals(rows.get(2048), new Object[]{8, "q", null, null});
    assertEquals(rows.get(2049), new Object[]{9, "z", null, null});
  }

  /// The residual rejects every candidate of a run that is larger than an output block: each left row is padded once.
  @Test
  public void shouldPadEachLeftRowOnceWhenResidualRejectsEverything() {
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 2500; i++) {
      rightBuilder.addRow(100 + i, "k");
    }
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .buildWithEos();
    RexExpression residual =
        compare(SqlKind.GREATER_THAN, new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    SortedMergeJoinOperator operator = getOperator(left, rightBuilder.buildWithEos(), RESULT_SCHEMA,
        JoinRelType.LEFT, List.of(1), List.of(1), List.of(residual));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, "k", null, null});
    assertEquals(rows.get(1), new Object[]{2, "k", null, null});
  }

  /// A right key run larger than maxRowsInJoin is rejected while it is buffered, even if the total output would fit.
  @Test
  public void shouldThrowWhenRightKeyRunExceedsMaxRowsInJoin() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "k").buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(10, "k")
        .addRow(20, "k")
        .addRow(30, "k")
        .buildWithEos();
    SortedMergeJoinOperator operator = getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1),
        List.of(1), maxRowsInJoinHint(2, "THROW"));
    MseBlock.Eos eos = drainToEos(operator);
    assertTrue(eos.isError(), "THROW must fail the query when the right key run overflows");
    assertTrue(((ErrorMseBlock) eos).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("right rows for a single join key"));
    assertFalse(operator.hasBufferedState());
  }

  /// BREAK mode on the right key run, like the hash join's build side: the run is truncated at the cap, the rest of
  /// that key's right rows are skipped, and the merge continues, so later left keys (including LEFT padding) are still
  /// produced. The run (1500 of 1600 right rows) is above the output block size, so the output cap never fires.
  @Test
  public void shouldTruncateRightKeyRunAndKeepMergingInBreakMode() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .addRow(2, "k")
        .addRow(3, "z")
        .buildWithEos();
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    rightBuilder.addRow(10, "a");
    for (int i = 0; i < 1600; i++) {
      rightBuilder.addRow(i, "k");
    }
    SortedMergeJoinOperator operator = getOperator(left, rightBuilder.buildWithEos(), RESULT_SCHEMA,
        JoinRelType.LEFT, List.of(1), List.of(1), maxRowsInJoinHint(1500, "BREAK"));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 1 + 1500 + 1);
    assertEquals(rows.get(0), new Object[]{1, "a", 10, "a"});
    assertEquals(rows.get(1), new Object[]{2, "k", 0, "k"});
    assertEquals(rows.get(1500), new Object[]{2, "k", 1499, "k"});
    assertEquals(rows.get(1501), new Object[]{3, "z", null, null}, "left rows after the truncated key must survive");
    StatMap<SortedMergeJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(SortedMergeJoinOperator.StatKey.class, operator.calculateStats());
    assertTrue(statMap.getBoolean(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
    assertEquals(statMap.getLong(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN), 1500);
  }

  /// maxRowsInJoin is not a cumulative output cap: like the hash join, whose limit applies per output block, a join
  /// producing 5000 rows over several blocks succeeds with a cap of 2500, in both overflow modes.
  @Test(dataProvider = "overflowModes")
  public void shouldNotBoundTotalOutputAcrossBlocks(String overflowMode) {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 50; i++) {
      leftBuilder.addRow(i, "k");
    }
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 100; i++) {
      rightBuilder.addRow(i, "k");
    }
    SortedMergeJoinOperator operator = getOperator(leftBuilder.buildWithEos(), rightBuilder.buildWithEos(),
        RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1), maxRowsInJoinHint(2500, overflowMode));
    assertEquals(drain(operator).size(), 5000);
    StatMap<SortedMergeJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(SortedMergeJoinOperator.StatKey.class, operator.calculateStats());
    assertFalse(statMap.getBoolean(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
  }

  /// A 1:1 join of many blocks, far above the cap in total, succeeds (the 5M-row 1:1 join vs the 1M default, scaled
  /// down) because every output block stays below the cap.
  @Test
  public void shouldSucceedWhenTotalOutputExceedsCapAcrossManyBlocks() {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 20_000; i++) {
      String key = String.format("k%06d", i);
      leftBuilder.addRow(i, key);
      rightBuilder.addRow(i, key);
      if (i % 700 == 699) {
        leftBuilder.finishBlock();
        rightBuilder.finishBlock();
      }
    }
    SortedMergeJoinOperator operator = getOperator(leftBuilder.buildWithEos(), rightBuilder.buildWithEos(),
        RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1), maxRowsInJoinHint(5000, "THROW"));
    assertEquals(drain(operator).size(), 20_000);
  }

  /// The output limit is per output block: with a cap below the block size and 1:1 keys, the block that reaches the cap
  /// fails the query in THROW mode.
  @Test
  public void shouldThrowWhenAnOutputBlockReachesMaxRowsInJoin() {
    SortedMergeJoinOperator operator = getOneToOneOperator(10, maxRowsInJoinHint(5, "THROW"));
    MseBlock.Eos eos = drainToEos(operator);
    assertTrue(eos.isError());
    assertTrue(((ErrorMseBlock) eos).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("reached number of rows limit: 5"));
    assertFalse(operator.hasBufferedState());
  }

  /// BREAK on the output limit: the block is cut at the cap and the join ends with a success EOS.
  @Test
  public void shouldCutAnOutputBlockAtMaxRowsInJoinInBreakMode() {
    SortedMergeJoinOperator operator = getOneToOneOperator(10, maxRowsInJoinHint(5, "BREAK"));
    List<Object[]> rows = drain(operator);
    assertEquals(rows.size(), 5);
    StatMap<SortedMergeJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(SortedMergeJoinOperator.StatKey.class, operator.calculateStats());
    assertTrue(statMap.getBoolean(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
    assertEquals(statMap.getLong(SortedMergeJoinOperator.StatKey.MAX_ROWS_IN_JOIN), 5);
  }

  private SortedMergeJoinOperator getOneToOneOperator(int numRows, PlanNode.NodeHint hint) {
    BlockListMultiStageOperator.Builder leftBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < numRows; i++) {
      leftBuilder.addRow(i, "k" + i);
      rightBuilder.addRow(i, "k" + i);
    }
    return getOperator(leftBuilder.buildWithEos(), rightBuilder.buildWithEos(), RESULT_SCHEMA, JoinRelType.INNER,
        List.of(1), List.of(1), hint);
  }

  @DataProvider
  public Object[][] overflowModes() {
    return new Object[][]{{"THROW"}, {"BREAK"}};
  }

  /// When one input ends the join, the other (still open) input is told to stop producing.
  @Test
  public void shouldEarlyTerminateTheOtherInputWhenOneSideFinishes() {
    BlockListMultiStageOperator leftEmpty = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    BlockListMultiStageOperator rightRows =
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    assertEquals(drain(getOperator(leftEmpty, rightRows, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1)))
        .size(), 0);
    assertTrue(rightRows._isEarlyTerminated, "right input is no longer needed once the left is exhausted");

    BlockListMultiStageOperator leftRows =
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    BlockListMultiStageOperator rightEmpty = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    assertEquals(drain(getOperator(leftRows, rightEmpty, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1)))
        .size(), 0);
    assertTrue(leftRows._isEarlyTerminated, "left input is no longer needed once the right is exhausted (INNER)");

    BlockListMultiStageOperator leftRows2 =
        new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "a").buildWithEos();
    BlockListMultiStageOperator rightEmpty2 = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    assertEquals(drain(getOperator(leftRows2, rightEmpty2, RESULT_SCHEMA, JoinRelType.LEFT, List.of(1),
        List.of(1))).size(), 1);
    assertFalse(leftRows2._isEarlyTerminated, "a LEFT join still needs every left row");
  }

  /// The right input fails after the matching run was buffered across a block boundary: the error must surface.
  @Test
  public void shouldPropagateRightErrorWhileBufferingRun() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).addRow(1, "k").buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .finishBlock()
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testRunError")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    MseBlock.Eos eos = drainToEos(operator);
    assertTrue(eos.isError());
    assertTrue(((ErrorMseBlock) eos).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains("testRunError"));
    assertFalse(operator.hasBufferedState());
  }

  /// The left input fails in the middle of a duplicate run: the query must end in the error, never in a success EOS.
  @Test
  public void shouldPropagateLeftErrorInTheMiddleOfARun() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .finishBlock()
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testLeftRunError")));
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 1500; i++) {
      rightBuilder.addRow(i, "k");
    }
    SortedMergeJoinOperator operator = getOperator(left, rightBuilder.buildWithEos(), RESULT_SCHEMA,
        JoinRelType.INNER, List.of(1), List.of(1));
    MseBlock.Eos eos = drainToEos(operator);
    assertTrue(eos.isError());
    assertTrue(((ErrorMseBlock) eos).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains("testLeftRunError"));
    assertFalse(operator.hasBufferedState());
  }

  @DataProvider
  public Object[][] cancelOrClose() {
    return new Object[][]{{true}, {false}};
  }

  /// Releasing the buffers after a block was returned must not touch the block already handed downstream.
  @Test(dataProvider = "cancelOrClose")
  public void shouldReleaseBufferedRowsWithoutChangingOutput(boolean cancel) {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "k")
        .addRow(2, "k")
        .buildWithEos();
    BlockListMultiStageOperator.Builder rightBuilder = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA);
    for (int i = 0; i < 1500; i++) {
      rightBuilder.addRow(i, "k");
    }
    SortedMergeJoinOperator operator = getOperator(left, rightBuilder.buildWithEos(), RESULT_SCHEMA,
        JoinRelType.INNER, List.of(1), List.of(1));
    MseBlock.Data output = (MseBlock.Data) operator.nextBlock();
    assertTrue(operator.hasBufferedState(), "the right run is still buffered mid-key");
    if (cancel) {
      operator.cancel(new RuntimeException("cancelled"));
    } else {
      operator.close();
    }
    assertFalse(operator.hasBufferedState());
    operator.close();
    assertFalse(operator.hasBufferedState());
    assertEquals(output.getNumRows(), MAX_BLOCK_ROWS);
    assertEquals(output.asRowHeap().getRows().get(0), new Object[]{1, "k", 0, "k"});
  }

  /// A THROW from the output limit (an exception, not an EOS block) must also leave nothing buffered.
  @Test
  public void shouldReleaseBuffersWhenEmissionLimitThrows() {
    SortedMergeJoinOperator operator = getOneToOneOperator(10, maxRowsInJoinHint(5, "THROW"));
    assertTrue(drainToEos(operator).isError());
    assertFalse(operator.hasBufferedState());
  }

  /// Upstream stage stats only arrive with each input's EOS, so an input that is not read to its EOS loses them.
  /// Every way of finishing early must therefore drain the inputs that are still open.
  @Test
  public void shouldKeepUpstreamStatsWhenLeftEndsFirst() {
    EosStatsOperator left = new EosStatsOperator(List.of(), 100);
    EosStatsOperator right = new EosStatsOperator(List.of(block(1, "a"), block(2, "b")), 20);
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    assertEquals(drain(operator).size(), 0);
    assertTrue(right._eosReturned, "the still-open right input must be read to its EOS");
    assertEquals(inputEmittedRows(operator, 0), 100);
    assertEquals(inputEmittedRows(operator, 1), 20);
  }

  @Test
  public void shouldKeepUpstreamStatsWhenRightEndsFirst() {
    EosStatsOperator left = new EosStatsOperator(List.of(block(1, "a"), block(2, "b")), 100);
    EosStatsOperator right = new EosStatsOperator(List.of(), 20);
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    assertEquals(drain(operator).size(), 0);
    assertTrue(left._eosReturned);
    assertEquals(inputEmittedRows(operator, 0), 100);
    assertEquals(inputEmittedRows(operator, 1), 20);
  }

  @Test
  public void shouldKeepUpstreamStatsOnDownstreamEarlyTerminate() {
    // 1:1 keys over 3 input blocks of 1000 rows each: the first output block (1024 rows) leaves both inputs open.
    EosStatsOperator left = new EosStatsOperator(oneToOneBlocks(3, 1000), 100);
    EosStatsOperator right = new EosStatsOperator(oneToOneBlocks(3, 1000), 20);
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    assertTrue(operator.nextBlock().isData());
    assertFalse(left._eosReturned || right._eosReturned, "the inputs must still be open when terminating");
    operator.earlyTerminate();
    assertSame(operator.nextBlock(), SuccessMseBlock.INSTANCE);
    assertTrue(left._eosReturned && right._eosReturned);
    assertEquals(inputEmittedRows(operator, 0), 100);
    assertEquals(inputEmittedRows(operator, 1), 20);
  }

  @Test
  public void shouldKeepUpstreamStatsOnBreakOverflow() {
    EosStatsOperator left = new EosStatsOperator(
        List.of(block(1, "k0"), block(2, "k1"), block(3, "k2"), block(4, "k3"), block(5, "k4")), 100);
    EosStatsOperator right = new EosStatsOperator(
        List.of(block(1, "k0"), block(2, "k1"), block(3, "k2"), block(4, "k3"), block(5, "k4")), 20);
    SortedMergeJoinOperator operator = getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1),
        List.of(1), maxRowsInJoinHint(3, "BREAK"));
    assertEquals(drain(operator).size(), 3);
    assertTrue(left._eosReturned && right._eosReturned);
    assertEquals(inputEmittedRows(operator, 0), 100);
    assertEquals(inputEmittedRows(operator, 1), 20);
  }

  /// An input that fails while it is drained after an early finish still fails the query, like the hash join.
  @Test
  public void shouldPropagateErrorOfInputDrainedAfterEarlyFinish() {
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA).buildWithEos();
    MultiStageOperator right = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addRow(1, "a")
        .finishBlock()
        .buildWithError(ErrorMseBlock.fromException(new RuntimeException("testDrainError")));
    SortedMergeJoinOperator operator =
        getOperator(left, right, RESULT_SCHEMA, JoinRelType.INNER, List.of(1), List.of(1));
    MseBlock.Eos eos = drainToEos(operator);
    assertTrue(eos.isError());
    assertTrue(((ErrorMseBlock) eos).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains("testDrainError"));
  }

  /// Hash join compares keys with equals(), which is scale-sensitive for BigDecimal, while the merge orders by
  /// compareTo (1.0 == 1.00). Only pairs that are also equal by equals() are joined; a LEFT row whose only
  /// compareTo-equal candidates differ in scale is padded.
  @Test
  public void shouldJoinBigDecimalKeysByEqualsNotCompareTo() {
    DataSchema schema = new DataSchema(new String[]{"d", "tag"},
        new ColumnDataType[]{ColumnDataType.BIG_DECIMAL, ColumnDataType.STRING});
    DataSchema resultSchema = new DataSchema(new String[]{"d1", "tag1", "d2", "tag2"},
        new ColumnDataType[]{ColumnDataType.BIG_DECIMAL, ColumnDataType.STRING, ColumnDataType.BIG_DECIMAL,
            ColumnDataType.STRING});
    for (JoinRelType joinType : new JoinRelType[]{JoinRelType.INNER, JoinRelType.LEFT}) {
      MultiStageOperator left = new BlockListMultiStageOperator.Builder(schema)
          .addRow(new BigDecimal("1.0"), "l1.0")
          .addRow(new BigDecimal("1.00"), "l1.00")
          .addRow(new BigDecimal("2"), "l2")
          .buildWithEos();
      MultiStageOperator right = new BlockListMultiStageOperator.Builder(schema)
          .addRow(new BigDecimal("1.0"), "r1.0")
          .addRow(new BigDecimal("1.000"), "r1.000")
          .addRow(new BigDecimal("2.0"), "r2.0")
          .buildWithEos();
      SortedMergeJoinOperator operator = new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left,
          schema, right, new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), joinType, List.of(0),
          List.of(0), List.of(), JoinNode.JoinStrategy.SORTED));
      List<String> actual = new ArrayList<>();
      for (Object[] row : drain(operator)) {
        actual.add(row[1] + "|" + row[3]);
      }
      if (joinType == JoinRelType.INNER) {
        assertEquals(actual, List.of("l1.0|r1.0"));
      } else {
        assertEquals(actual, List.of("l1.0|r1.0", "l1.00|null", "l2|null"));
      }
    }
  }

  private static List<MseBlock> oneToOneBlocks(int numBlocks, int rowsPerBlock) {
    List<MseBlock> blocks = new ArrayList<>();
    for (int b = 0; b < numBlocks; b++) {
      Object[][] rows = new Object[rowsPerBlock][];
      for (int i = 0; i < rowsPerBlock; i++) {
        int id = b * rowsPerBlock + i;
        rows[i] = new Object[]{id, String.format("k%06d", id)};
      }
      blocks.add(OperatorTestUtil.block(CHILD_SCHEMA, rows));
    }
    return blocks;
  }

  private static MseBlock block(int intValue, String stringValue) {
    return OperatorTestUtil.block(CHILD_SCHEMA, new Object[]{intValue, stringValue});
  }

  /// The marker an [EosStatsOperator] input contributed to the stage stats of the join (operator 0 is the left input,
  /// 1 the right input).
  private static long inputEmittedRows(SortedMergeJoinOperator join, int inputIndex) {
    @SuppressWarnings("unchecked")
    StatMap<LiteralValueOperator.StatKey> statMap =
        (StatMap<LiteralValueOperator.StatKey>) join.calculateStats().getCurrentStats().getOperatorStats(inputIndex);
    return statMap.getLong(LiteralValueOperator.StatKey.EMITTED_ROWS);
  }

  /// Mimics a mailbox receive: its stats (a marker in EMITTED_ROWS) only become visible once its EOS was read, and
  /// after earlyTerminate() it skips the remaining data and goes straight to the EOS.
  private static final class EosStatsOperator extends MultiStageOperator {
    private final Iterator<MseBlock> _blocks;
    private final long _marker;
    private final StatMap<LiteralValueOperator.StatKey> _statMap = new StatMap<>(LiteralValueOperator.StatKey.class);
    private boolean _eosReturned;

    EosStatsOperator(List<MseBlock> blocks, long marker) {
      super(OperatorTestUtil.getTracingContext());
      _blocks = blocks.iterator();
      _marker = marker;
    }

    @Override
    protected MseBlock getNextBlock() {
      if (!_isEarlyTerminated && _blocks.hasNext()) {
        return _blocks.next();
      }
      if (!_eosReturned) {
        _eosReturned = true;
        _statMap.merge(LiteralValueOperator.StatKey.EMITTED_ROWS, _marker);
      }
      return SuccessMseBlock.INSTANCE;
    }

    @Override
    public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    }

    @Override
    protected Logger logger() {
      return LoggerFactory.getLogger(EosStatsOperator.class);
    }

    @Override
    public Type getOperatorType() {
      return Type.LITERAL;
    }

    @Override
    public List<MultiStageOperator> getChildOperators() {
      return List.of();
    }

    @Override
    public StatMap<LiteralValueOperator.StatKey> copyStatMaps() {
      return new StatMap<>(_statMap);
    }

    @Nullable
    @Override
    public String toExplainString() {
      return "EOS_STATS";
    }
  }

  private SortedMergeJoinOperator getOperator(MultiStageOperator left, MultiStageOperator right,
      DataSchema resultSchema, JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys) {
    return getOperator(left, right, resultSchema, joinType, leftKeys, rightKeys, PlanNode.NodeHint.EMPTY);
  }

  private SortedMergeJoinOperator getOperator(MultiStageOperator left, MultiStageOperator right,
      DataSchema resultSchema, JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys,
      PlanNode.NodeHint nodeHint) {
    return new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left, CHILD_SCHEMA, right,
        new JoinNode(-1, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, List.of(),
            JoinNode.JoinStrategy.SORTED));
  }

  private SortedMergeJoinOperator getOperator(MultiStageOperator left, MultiStageOperator right,
      DataSchema resultSchema, JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys,
      List<RexExpression> nonEquiConditions) {
    return new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(), left, CHILD_SCHEMA, right,
        new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), joinType, leftKeys, rightKeys,
            nonEquiConditions, JoinNode.JoinStrategy.SORTED));
  }
}
