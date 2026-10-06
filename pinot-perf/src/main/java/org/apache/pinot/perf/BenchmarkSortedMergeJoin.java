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
package org.apache.pinot.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.SortedMergeJoinOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Operator-level comparison of [SortedMergeJoinOperator] and [HashJoinOperator].
///
/// Both operators are fed the very same pre-built [MseBlock]s through in-memory child operators ([BlockSource]), so no
/// mailbox, serialization or sort is involved. **The sort cost is explicitly excluded**: the inputs are generated
/// already sorted ascending on the join keys, which is what the planner guarantees (via sender-side sort and a k-way
/// merge receive) for a real sorted merge join. A real plan pays that sort (and a sort-preserving exchange) on top of
/// what is measured here; this benchmark therefore isolates the join algorithm itself and is *not* an end-to-end
/// latency comparison. Per-block overhead of [MultiStageOperator#nextBlock()] (stopwatch, thread allocation snapshot)
/// is paid by both operators and by the child sources alike.
///
/// Benchmark methods:
///   - [#sortedJoin] / [#hashJoin]: drain the join completely. One op is one full join, so the score is joins/s; the
///     number of output rows is printed once per trial (stdout) to convert to rows/s. Use `-prof gc` for B/op.
///   - [#sortedJoinLimit] / [#hashJoinLimit]: a LIMIT-like consumer that stops pulling after `limitRows` rows. This is
///     where a streaming merge can win: the hash join must build the whole right side before emitting the first row.
///
/// Parameters:
///   - `rowsPerSide`: rows on each side.
///   - `dist`: UNIQUE is 1:1 (about 10% of keys unmatched on each side), DUP10 has 10 rows per key on both sides
///     (a 10x10 cross product per key, so the output is about 10x the input), HOT keeps unique keys but one key in
///     the middle of the key space holds 10% of the right rows and 100 left rows (output about 10x the right side
///     when the left side is 1M rows).
///   - `keyType`: INT, LONG, STRING, or INT_STRING (two key columns).
///   - `joinType`: INNER or LEFT.
///   - `residual`: adds a non-equi condition (`left.payload <> right.payload`, true for every pair, so the output
///     is unchanged and only the evaluation cost is added).
///   - `limitRows`: only used by the `*Limit` methods.
///
/// The `@Param` defaults are a single configuration on purpose: a full cross product is thousands of runs. Use
/// [#main(String[])] with `smoke` or `full` for curated groups, or pass `-p` overrides to the JMH command line.
///
/// Thread-safety: single-threaded; the shared input blocks are never modified by either operator.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms8g", "-Xmx8g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkSortedMergeJoin {
  private static final int BLOCK_SIZE = 4096;
  private static final int HOT_LEFT_ROWS = 100;

  @Param({"100000"})
  public int _rowsPerSide;

  @Param({"UNIQUE"})
  public String _dist;

  @Param({"INT"})
  public String _keyType;

  @Param({"INNER"})
  public String _joinType;

  @Param({"false"})
  public boolean _residual;

  @Param({"1000"})
  public int _limitRows;

  private OpChainExecutionContext _context;
  private DataSchema _leftSchema;
  private DataSchema _rightSchema;
  private List<MseBlock> _leftBlocks;
  private List<MseBlock> _rightBlocks;
  private JoinNode _sortedNode;
  private JoinNode _hashNode;

  @Setup(Level.Trial)
  public void setUp() {
    MailboxService mailboxService = Mockito.mock(MailboxService.class);
    Mockito.when(mailboxService.getHostname()).thenReturn("localhost");
    Mockito.when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    StageMetadata stageMetadata =
        new StageMetadata(0, List.of(workerMetadata), Map.of(DispatchablePlanFragment.TABLE_NAME_KEY, "testTable"));
    // The hot-key shapes legitimately emit far more than the default 1M-row cap
    Map<String, String> metadata = Map.of(QueryOptionKey.MAX_ROWS_IN_JOIN, String.valueOf(Integer.MAX_VALUE));
    _context = OpChainExecutionContext.fromQueryContext(mailboxService, metadata, stageMetadata, workerMetadata, null,
        true, true, QueryExecutionContext.forMseTest());

    KeyType keyType = KeyType.valueOf(_keyType);
    _leftSchema = keyType.schema("l");
    _rightSchema = keyType.schema("r");
    int[] leftIds = leftIds(_dist, _rowsPerSide);
    int[] rightIds = rightIds(_dist, _rowsPerSide);
    _leftBlocks = blocks(leftIds, keyType, _leftSchema, 0L);
    // Payload ranges are disjoint so the residual `<>` is always true.
    _rightBlocks = blocks(rightIds, keyType, _rightSchema, 1L << 40);

    int numKeys = keyType.numKeyColumns();
    List<Integer> keys = new ArrayList<>(numKeys);
    for (int i = 0; i < numKeys; i++) {
      keys.add(i);
    }
    int leftSize = _leftSchema.size();
    String[] names = new String[leftSize + _rightSchema.size()];
    ColumnDataType[] types = new ColumnDataType[names.length];
    for (int i = 0; i < leftSize; i++) {
      names[i] = _leftSchema.getColumnName(i);
      types[i] = _leftSchema.getColumnDataType(i);
    }
    for (int i = 0; i < _rightSchema.size(); i++) {
      names[leftSize + i] = _rightSchema.getColumnName(i);
      types[leftSize + i] = _rightSchema.getColumnDataType(i);
    }
    DataSchema resultSchema = new DataSchema(names, types);
    List<RexExpression> nonEqui = List.of();
    if (_residual) {
      int leftPayload = leftSize - 1;
      int rightPayload = names.length - 1;
      nonEqui = List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(),
          List.of(new RexExpression.InputRef(leftPayload), new RexExpression.InputRef(rightPayload))));
    }
    JoinRelType joinType = JoinRelType.valueOf(_joinType);
    _sortedNode = new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), joinType, keys, keys, nonEqui,
        JoinNode.JoinStrategy.SORTED);
    _hashNode = new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), joinType, keys, keys, nonEqui,
        JoinNode.JoinStrategy.HASH);

    // Sanity check (not timed): both operators must agree on the output size, otherwise the comparison is void.
    long sortedRows = drain(newSorted(), Long.MAX_VALUE);
    long hashRows = drain(newHash(), Long.MAX_VALUE);
    if (sortedRows != hashRows) {
      throw new IllegalStateException(
          "Sorted join emitted " + sortedRows + " rows but hash join emitted " + hashRows + " for " + describe());
    }
    System.out.println("[BenchmarkSortedMergeJoin] " + describe() + " outputRows=" + sortedRows);
  }

  private String describe() {
    return "rows=" + _rowsPerSide + " dist=" + _dist + " key=" + _keyType + " join=" + _joinType + " residual="
        + _residual;
  }

  private MultiStageOperator newSorted() {
    return new SortedMergeJoinOperator(_context, new BlockSource(_context, _leftBlocks), _leftSchema,
        new BlockSource(_context, _rightBlocks), _sortedNode);
  }

  private MultiStageOperator newHash() {
    return new HashJoinOperator(_context, new BlockSource(_context, _leftBlocks), _leftSchema,
        new BlockSource(_context, _rightBlocks), _hashNode);
  }

  /// Pulls blocks until EOS or until at least `limit` rows were seen, like a LIMIT does. Returns the rows seen.
  private static long drain(MultiStageOperator operator, long limit) {
    long rows = 0;
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      rows += ((MseBlock.Data) block).asRowHeap().getRows().size();
      if (rows >= limit) {
        return rows;
      }
      block = operator.nextBlock();
    }
    if (block.isError()) {
      throw new IllegalStateException("Join returned an error block: " + block);
    }
    return rows;
  }

  @Benchmark
  public void sortedJoin(Blackhole bh) {
    bh.consume(drain(newSorted(), Long.MAX_VALUE));
  }

  @Benchmark
  public void hashJoin(Blackhole bh) {
    bh.consume(drain(newHash(), Long.MAX_VALUE));
  }

  @Benchmark
  public void sortedJoinLimit(Blackhole bh) {
    bh.consume(drain(newSorted(), _limitRows));
  }

  @Benchmark
  public void hashJoinLimit(Blackhole bh) {
    bh.consume(drain(newHash(), _limitRows));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Data generation
  // ---------------------------------------------------------------------------------------------------------------

  /// Key id of each left row, non-decreasing.
  private static int[] leftIds(String dist, int n) {
    int[] ids = new int[n];
    switch (dist) {
      case "UNIQUE":
        for (int i = 0; i < n; i++) {
          ids[i] = i;
        }
        break;
      case "DUP10":
        for (int i = 0; i < n; i++) {
          ids[i] = i / 10;
        }
        break;
      case "HOT":
        hot(ids, n, Math.min(HOT_LEFT_ROWS, n));
        break;
      default:
        throw new IllegalArgumentException("Unknown dist: " + dist);
    }
    return ids;
  }

  /// Key id of each right row, non-decreasing. Skips every 10th key so that part of the left side is unmatched.
  private static int[] rightIds(String dist, int n) {
    int[] ids = new int[n];
    switch (dist) {
      case "UNIQUE":
        for (int i = 0; i < n; i++) {
          ids[i] = i + i / 9;
        }
        break;
      case "DUP10":
        for (int i = 0; i < n; i++) {
          int key = i / 10;
          ids[i] = key + key / 9;
        }
        break;
      case "HOT":
        hot(ids, n, n / 10);
        break;
      default:
        throw new IllegalArgumentException("Unknown dist: " + dist);
    }
    return ids;
  }

  /// Unique keys except for `hotRows` copies of the key `n / 2`, which sits in the middle of the key space.
  private static void hot(int[] ids, int n, int hotRows) {
    int hotKey = n / 2;
    int nonHot = n - hotRows;
    int pos = 0;
    for (int t = 0; t < nonHot; t++) {
      if (t == hotKey) {
        for (int h = 0; h < hotRows; h++) {
          ids[pos++] = hotKey;
        }
      }
      ids[pos++] = t < hotKey ? t : t + 1;
    }
    while (pos < n) {
      // The hot key was beyond the non-hot range (tiny n)
      ids[pos++] = hotKey;
    }
  }

  private static List<MseBlock> blocks(int[] ids, KeyType keyType, DataSchema schema, long payloadBase) {
    List<MseBlock> blocks = new ArrayList<>(ids.length / BLOCK_SIZE + 1);
    List<Object[]> rows = new ArrayList<>(BLOCK_SIZE);
    for (int i = 0; i < ids.length; i++) {
      rows.add(keyType.row(ids[i], payloadBase + i));
      if (rows.size() == BLOCK_SIZE) {
        blocks.add(new RowHeapDataBlock(rows, schema));
        rows = new ArrayList<>(BLOCK_SIZE);
      }
    }
    if (!rows.isEmpty()) {
      blocks.add(new RowHeapDataBlock(rows, schema));
    }
    return blocks;
  }

  /// Key column layouts. Key values are order-preserving functions of the key id (strings are zero padded).
  private enum KeyType {
    INT(1),
    LONG(1),
    STRING(1),
    INT_STRING(2);

    private final int _numKeyColumns;

    KeyType(int numKeyColumns) {
      _numKeyColumns = numKeyColumns;
    }

    int numKeyColumns() {
      return _numKeyColumns;
    }

    DataSchema schema(String prefix) {
      switch (this) {
        case INT:
          return new DataSchema(new String[]{prefix + "_k", prefix + "_v"},
              new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
        case LONG:
          return new DataSchema(new String[]{prefix + "_k", prefix + "_v"},
              new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG});
        case STRING:
          return new DataSchema(new String[]{prefix + "_k", prefix + "_v"},
              new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.LONG});
        default:
          return new DataSchema(new String[]{prefix + "_k1", prefix + "_k2", prefix + "_v"},
              new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.LONG});
      }
    }

    Object[] row(int id, long payload) {
      switch (this) {
        case INT:
          return new Object[]{id, payload};
        case LONG:
          return new Object[]{10_000_000_000L + id, payload};
        case STRING:
          return new Object[]{String.format("key-%010d", id), payload};
        default:
          return new Object[]{id >> 6, String.format("s%02d", id & 63), payload};
      }
    }
  }

  /// A leaf-like source replaying shared, pre-built blocks and then EOS. The blocks and their rows are shared by
  /// every operator instance and never copied.
  private static final class BlockSource extends MultiStageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockSource.class);
    private final List<MseBlock> _blocks;
    private final StatMap<LiteralValueOperator.StatKey> _statMap = new StatMap<>(LiteralValueOperator.StatKey.class);
    private int _next;

    BlockSource(OpChainExecutionContext context, List<MseBlock> blocks) {
      super(context);
      _blocks = blocks;
    }

    @Override
    protected Logger logger() {
      return LOGGER;
    }

    @Override
    public Type getOperatorType() {
      return Type.LITERAL;
    }

    @Override
    public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
      _statMap.merge(LiteralValueOperator.StatKey.EXECUTION_TIME_MS, time);
      _statMap.merge(LiteralValueOperator.StatKey.EMITTED_ROWS, numRows);
    }

    @Override
    protected MseBlock getNextBlock() {
      return _next < _blocks.size() ? _blocks.get(_next++) : SuccessMseBlock.INSTANCE;
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
      return "BLOCK_SOURCE";
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Runner
  // ---------------------------------------------------------------------------------------------------------------

  private static final String DRAIN = BenchmarkSortedMergeJoin.class.getSimpleName() + ".(sortedJoin|hashJoin)$";
  private static final String LIMIT =
      BenchmarkSortedMergeJoin.class.getSimpleName() + ".(sortedJoinLimit|hashJoinLimit)$";

  private static void run(boolean smoke, String regex, String[] rows, String[] dists, String[] keyTypes,
      String[] joinTypes, String[] residuals)
      throws RunnerException {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(regex)
        .param("_rowsPerSide", rows)
        .param("_dist", dists)
        .param("_keyType", keyTypes)
        .param("_joinType", joinTypes)
        .param("_residual", residuals)
        .addProfiler(GCProfiler.class);
    if (smoke) {
      opt.warmupIterations(2).warmupTime(TimeValue.seconds(2)).measurementIterations(3)
          .measurementTime(TimeValue.seconds(2));
    }
    String profiler = System.getProperty("profiler");
    if (profiler != null) {
      opt.addProfiler(profiler);
    }
    String result = System.getProperty("jmh.result");
    if (result != null) {
      opt.resultFormat(ResultFormatType.JSON).result(result);
    }
    new Runner(opt.build()).run();
  }

  /// Usage: `main [smoke|full]` (default `full`). Optional system properties: `-Djmh.result=<file.json>` writes the
  /// JMH JSON result (overwritten by each group, so read stdout for the whole run), `-Dprofiler=<name>` adds one more
  /// JMH profiler (e.g. `stack`). The full set is about 34 drain runs and 6 limit runs, around 10-15 minutes.
  public static void main(String[] args)
      throws RunnerException {
    boolean smoke = args.length > 0 && args[0].equals("smoke");
    String[] rows = smoke ? new String[]{"100000"} : new String[]{"100000", "1000000"};
    String[] allDists = {"UNIQUE", "DUP10", "HOT"};
    String[] no = {"false"};
    String[] inner = {"INNER"};
    String[] intKey = {"INT"};
    String[] bigRows = smoke ? rows : new String[]{"1000000"};

    // Distribution x size (INT, INNER, no residual)
    run(smoke, DRAIN, rows, allDists, intKey, inner, no);
    // Key types, where per-row comparison and key extraction differ the most
    run(smoke, DRAIN, bigRows, new String[]{"DUP10"}, new String[]{"LONG", "STRING", "INT_STRING"}, inner, no);
    if (!smoke) {
      run(false, DRAIN, bigRows, new String[]{"UNIQUE"}, new String[]{"LONG", "STRING", "INT_STRING"}, inner, no);
    }
    // LEFT join, with and without a residual filter
    run(smoke, DRAIN, bigRows, smoke ? new String[]{"UNIQUE"} : allDists, intKey, new String[]{"LEFT"},
        new String[]{"false", "true"});
    // Residual on INNER
    if (!smoke) {
      run(false, DRAIN, bigRows, allDists, intKey, inner, new String[]{"true"});
    }
    // LIMIT-like consumer
    run(smoke, LIMIT, rows, smoke ? new String[]{"UNIQUE", "HOT"} : allDists, intKey, inner, no);
  }
}
