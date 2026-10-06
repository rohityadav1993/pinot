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
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Seeded randomized differential test of [SortedMergeJoinOperator] against [HashJoinOperator] as the oracle.
///
/// Each iteration draws a key type, single or composite keys, INNER or LEFT, with or without a residual predicate,
/// a small key domain (to force duplicates on both sides, hot keys larger than an output block, and empty sides),
/// null keys and null payloads, and random input block boundaries (including empty blocks). The inputs are sorted with
/// [SortUtils.SortComparator] using ASC NULLS LAST, the collation the planner requests, so the test also pins the
/// operator's key comparison to the order the sorts and the merge exchange actually produce, for every supported key
/// type (including NaN and signed zero for floating point). The multisets of output rows must be identical, the
/// output must stay ordered by the left keys, blocks must be bounded and buffers released.
public class SortedMergeJoinOperatorDifferentialTest {
  private static final int ITERATIONS_PER_KEY_TYPE = 150;
  private static final long BASE_SEED = 20261005L;
  private static final int MAX_BLOCK_ROWS = 1024;

  private static final float[] FLOAT_VALUES =
      {Float.NEGATIVE_INFINITY, -1.5f, -0.0f, 0.0f, 2.5f, Float.NaN, Float.POSITIVE_INFINITY, 1e30f};
  private static final double[] DOUBLE_VALUES =
      {Double.NEGATIVE_INFINITY, -1.5, -0.0, 0.0, 2.5, Double.NaN, Double.POSITIVE_INFINITY, 1e300};

  private enum KeyType {
    INT(ColumnDataType.INT, 6),
    LONG(ColumnDataType.LONG, 6),
    FLOAT(ColumnDataType.FLOAT, FLOAT_VALUES.length),
    DOUBLE(ColumnDataType.DOUBLE, DOUBLE_VALUES.length),
    BIG_DECIMAL(ColumnDataType.BIG_DECIMAL, 6),
    STRING(ColumnDataType.STRING, 6),
    BYTES(ColumnDataType.BYTES, 6),
    BOOLEAN(ColumnDataType.BOOLEAN, 2),
    TIMESTAMP(ColumnDataType.TIMESTAMP, 6);

    private final ColumnDataType _dataType;
    private final int _maxDomain;

    KeyType(ColumnDataType dataType, int maxDomain) {
      _dataType = dataType;
      _maxDomain = maxDomain;
    }

    /// The stored (internal row) representation of the `ordinal`-th distinct key value.
    Object value(int ordinal, Random random) {
      switch (this) {
        case INT:
          return ordinal - 3;
        case LONG:
          return (ordinal - 3) * 4_000_000_000L + 1;
        case FLOAT:
          return FLOAT_VALUES[ordinal];
        case DOUBLE:
          return DOUBLE_VALUES[ordinal];
        case BIG_DECIMAL:
          // Random scale: 1, 1.0, 1.00, 1.000 are equal by compareTo but not by equals (which hash join uses).
          return BigDecimal.valueOf(ordinal - 3).setScale(random.nextInt(4));
        case STRING:
          return "s" + (char) ('a' + ordinal) + (ordinal % 2 == 0 ? "" : "-long-suffix");
        case BYTES:
          // Values above 127 are negative as signed bytes: the order must be the unsigned one of ByteArray.
          return new ByteArray(new byte[]{(byte) (ordinal * 50), (byte) ordinal});
        case BOOLEAN:
          return ordinal;
        case TIMESTAMP:
          return 1_700_000_000_000L + (ordinal - 3) * 86_400_000L;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @DataProvider
  public Object[][] keyTypes() {
    List<Object[]> params = new ArrayList<>();
    for (KeyType keyType : KeyType.values()) {
      params.add(new Object[]{keyType});
    }
    return params.toArray(new Object[0][]);
  }

  @Test(dataProvider = "keyTypes")
  public void shouldMatchHashJoin(KeyType keyType) {
    for (int iteration = 0; iteration < ITERATIONS_PER_KEY_TYPE; iteration++) {
      runIteration(keyType, BASE_SEED + keyType.ordinal() * 100_000L + iteration);
    }
  }

  private static void runIteration(KeyType keyType, long seed) {
    Random random = new Random(seed);
    boolean composite = random.nextBoolean();
    JoinRelType joinType = random.nextBoolean() ? JoinRelType.INNER : JoinRelType.LEFT;
    boolean residual = random.nextBoolean();
    DataSchema childSchema = new DataSchema(new String[]{"k0", "k1", "p"},
        new ColumnDataType[]{keyType._dataType, ColumnDataType.STRING, ColumnDataType.INT});
    DataSchema resultSchema = new DataSchema(new String[]{"k0", "k1", "p", "rk0", "rk1", "rp"},
        new ColumnDataType[]{keyType._dataType, ColumnDataType.STRING, ColumnDataType.INT, keyType._dataType,
            ColumnDataType.STRING, ColumnDataType.INT});
    List<Integer> keys = composite ? List.of(0, 1) : List.of(0);
    List<RelFieldCollation> collations = new ArrayList<>();
    for (int key : keys) {
      collations.add(new RelFieldCollation(key, Direction.ASCENDING, NullDirection.LAST));
    }
    SortUtils.SortComparator comparator = new SortUtils.SortComparator(collations, false);

    int domain = 1 + random.nextInt(keyType._maxDomain);
    List<Object[]> leftRows = randomRows(random, keyType, domain);
    List<Object[]> rightRows = randomRows(random, keyType, domain);
    leftRows.sort(comparator);
    rightRows.sort(comparator);

    List<RexExpression> residuals = residual
        ? List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.LESS_THAN.name(),
            List.of(new RexExpression.InputRef(2), new RexExpression.InputRef(5)))) : List.of();
    PlanNode.NodeHint hint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "100000000")));
    String description = "seed=" + seed + " keyType=" + keyType + " composite=" + composite + " joinType=" + joinType
        + " residual=" + residual + " domain=" + domain + " left=" + leftRows.size() + " right=" + rightRows.size();

    SortedMergeJoinOperator sorted = new SortedMergeJoinOperator(OperatorTestUtil.getTracingContext(),
        input(childSchema, leftRows, random), childSchema, input(childSchema, rightRows, random),
        new JoinNode(-1, resultSchema, hint, List.of(), joinType, keys, keys, residuals,
            JoinNode.JoinStrategy.SORTED));
    List<Object[]> sortedResult = new ArrayList<>();
    MseBlock block = sorted.nextBlock();
    while (block.isData()) {
      int numRows = ((MseBlock.Data) block).getNumRows();
      assertTrue(numRows > 0 && numRows <= MAX_BLOCK_ROWS, "Unbounded or empty output block, " + description);
      sortedResult.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = sorted.nextBlock();
    }
    assertFalse(block.isError(), "Unexpected error block, " + description);
    assertFalse(sorted.hasBufferedState(), "Buffers not released at EOS, " + description);

    HashJoinOperator hash = new HashJoinOperator(OperatorTestUtil.getTracingContext(),
        input(childSchema, leftRows, random), childSchema, input(childSchema, rightRows, random),
        new JoinNode(-1, resultSchema, hint, List.of(), joinType, keys, keys, residuals, JoinNode.JoinStrategy.HASH));
    List<Object[]> hashResult = new ArrayList<>();
    block = hash.nextBlock();
    while (block.isData()) {
      hashResult.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = hash.nextBlock();
    }
    assertFalse(block.isError(), "Unexpected hash join error block, " + description);

    assertEquals(toSortedStrings(sortedResult), toSortedStrings(hashResult), description);
    // The output follows the left input order, which is what a stacked merge exchange relies on.
    for (int i = 1; i < sortedResult.size(); i++) {
      assertTrue(comparator.compare(sortedResult.get(i - 1), sortedResult.get(i)) <= 0,
          "Output not ordered on the left keys at row " + i + ", " + description);
    }
  }

  private static List<Object[]> randomRows(Random random, KeyType keyType, int domain) {
    int numRows = random.nextInt(10) < 7 ? random.nextInt(40) : random.nextInt(300);
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      Object k0 = random.nextInt(100) < 15 ? null : keyType.value(random.nextInt(domain), random);
      Object k1 = random.nextInt(100) < 15 ? null : "x" + random.nextInt(3);
      Object payload = random.nextInt(100) < 10 ? null : random.nextInt(5);
      rows.add(new Object[]{k0, k1, payload});
    }
    return rows;
  }

  /// Splits the rows into blocks at random boundaries, with the occasional empty block.
  private static MultiStageOperator input(DataSchema schema, List<Object[]> rows, Random random) {
    List<MseBlock> blocks = new ArrayList<>();
    int index = 0;
    while (index < rows.size()) {
      if (random.nextInt(10) == 0) {
        blocks.add(new RowHeapDataBlock(new ArrayList<>(), schema));
      }
      int end = Math.min(rows.size(), index + 1 + random.nextInt(50));
      blocks.add(new RowHeapDataBlock(new ArrayList<>(rows.subList(index, end)), schema));
      index = end;
    }
    return new BlockListMultiStageOperator(OperatorTestUtil.getTracingContext(), blocks);
  }

  private static List<String> toSortedStrings(List<Object[]> rows) {
    List<String> strings = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      strings.add(Arrays.toString(row));
    }
    strings.sort(null);
    return strings;
  }
}
