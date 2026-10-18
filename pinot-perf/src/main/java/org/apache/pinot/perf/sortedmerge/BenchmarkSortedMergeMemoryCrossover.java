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
package org.apache.pinot.perf.sortedmerge;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.perf.sortedmerge.SortedMergeDriver.OrderBy;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Arm 4, instrument 1: how allocation and latency of the sorted streaming merge scale against the default
/// `MinMaxValueBasedSelectionOrderByCombineOperator` as `LIMIT` and segment overlap depth vary.
///
/// <p><b>This measures allocation churn, not peak live heap.</b> `gc.alloc.rate.norm` is bytes allocated per
/// operation; an operator that allocates twice as much may still hold far less at any instant. The retention claim
/// this arm exists to test is about the ceiling, and the ceiling is measured separately by
/// {@link SortedMergeMemoryProbe} under a bisected `-Xmx`. Report the two numbers separately and never mix them.
///
/// <p>Run with allocation profiling, reduced iteration counts, and the build cache off:
/// ```
/// ./mvnw -pl pinot-perf -am install -DskipTests -Ddevelocity.cache.local.enabled=false
/// java -cp pinot-perf/target/classes:... org.apache.pinot.perf.sortedmerge.BenchmarkSortedMergeMemoryCrossover \
///     -wi 1 -i 3 -f 1 -r 5s -w 5s -prof gc
/// ```
///
/// <p>Hypothesis, with `L = limit + offset`, `D` = overlap depth and `W` = concurrent combine tasks: `ON` retains
/// `D * min(L, MAX_DOC_PER_CALL)` plus a `blockSize` output buffer, while `OFF` retains roughly
/// `W * min(L, rowsPerSegment)` plus a merged accumulator. That gives two regimes. Below `L = 10_000` both arms scale
/// linearly in `L`, so `L` cancels and the comparison is purely **`D` against `W`** with no crossover at all. Above
/// it, `ON`'s per-cursor cap binds while `OFF` keeps growing, putting the crossover near `L = 10_000 * D / W`.
///
/// <p>With `W` pinned to 10 and the fixture's depths of 1 / 10 / 100, that predicts `ON` winning at every limit for
/// DISJOINT, a dead heat below 10_000 then a crossover at 10_000 for PARTIAL, and a crossover at 100_000 for FULL.
/// The PARTIAL dead heat is the useful cell: it is the one place the model predicts no difference, so it also tests
/// whether this harness can report a null result rather than manufacturing a winner.
///
/// <p>A measurement that disagrees is the finding; do not tune the fixture until it agrees. An earlier version of this
/// comment predicted `limit = 10_000 * overlapDepth`, which omitted `W` entirely and was refuted by reading the
/// operators rather than by any measurement.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
// Pinned rather than left to `-t`. SortedMergeCpuMeter and the executor under test are both shared across benchmark
// threads, so a multi-threaded run would attribute the same pool CPU to every concurrent invocation. The meter also
// refuses a second reader at runtime, but failing to start is better than failing mid-trial.
@Threads(1)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
public class BenchmarkSortedMergeMemoryCrossover {

  @Param({"10", "100", "1000", "10000", "100000", "1000000"})
  private int _limit;

  @Param({"DISJOINT", "PARTIAL", "FULL"})
  private SortedMergeFixture.Overlap _overlap;

  @Param({"OFF", "ON"})
  private SortedSelectionMergeMode _mode;

  /// The two shapes described on [SortedMergeDriver.OrderBy]: `TS_VAL` keeps the existing `valCol` tiebreaker and
  /// pins the per-cursor block at `MAX_DOC_PER_CALL`; `TS_ONLY` drops it, which tightens the block to
  /// `min(limit + offset, MAX_DOC_PER_CALL)` and puts the `ON` arm on its "no tail to sort" emission path. Both
  /// values are swept by default -- this doubles the headline grid, deliberately, because the two shapes are
  /// expected to disagree about where the crossover falls and that disagreement is itself a result.
  @Param({"TS_VAL", "TS_ONLY"})
  private OrderBy _orderBy;

  /// Held at the shipped default for the headline grid. `DEFAULT_SORTED_SELECTION_MERGE_BLOCK_SIZE` and
  /// `DocIdSetPlanNode.MAX_DOC_PER_CALL` are both 10_000, so at the default the output buffer and the per-cursor block
  /// are the same size and can mask which of the two dominates. Re-run the crossover points with 1000 as a control.
  @Param({"10000"})
  private int _blockSize;

  @Param({"100"})
  private int _numSegments;

  @Param({"50000"})
  private int _rowsPerSegment;

  /// K, the fixture's {@link SortedMergeFixture#DEFAULT_KEY_CARDINALITY} knob: how many consecutive rows within a
  /// segment share one {@link SortedMergeFixture#TS_COL} value. Pinned to a single value, "1" -- the pre-existing
  /// behaviour -- so the headline grid does not grow; override with `-p keyCardinality=1,100` to sweep it.
  @Param({"1"})
  private int _keyCardinality;

  /// `maxExecutionThreads`, i.e. `W`. Pinned to a single value, "10", which is exactly what
  /// `QueryMultiThreadingUtils#MAX_NUM_THREADS_PER_QUERY` resolves to on a host with >= 20 cores (this box has 96).
  /// A single-valued `@Param` is deliberate: it keeps the headline grid at 36 configs while still recording the
  /// value in every JMH result row, and it can be overridden on the command line with
  /// `-p maxExecutionThreads=1,2,5,10` to run a dedicated `W` sweep without editing code.
  @Param({"10"})
  private int _maxExecutionThreads;

  private List<IndexSegment> _segments;
  private ExecutorService _executorService;
  private SortedMergeDriver.PreparedQuery _prepared;
  private SortedMergeCpuMeter _cpuMeter;

  /// CPU time consumed across all threads doing combine work, reported by JMH as secondary metrics next to the
  /// wall-clock score. Reads 0 unless `-Darm4.cpuTime=true` reaches the forked JVM; see {@link SortedMergeCpuMeter}.
  ///
  /// <p>Two counters are reported rather than one, because `Type.EVENTS` publishes the *sum* over the iterations of
  /// a trial and does not normalise by anything. `cpuNanos` alone is therefore uninterpretable without knowing how
  /// many invocations produced it, and deriving that from the wall-clock score is both circular and imprecise.
  /// `cpuOps` counts those invocations under exactly the same aggregation, so the figure to report is
  ///
  /// ```
  /// cpu ms per operation = cpuNanos / cpuOps / 1e6
  /// ```
  ///
  /// <p>Dividing the two also cancels the iteration count, so the result does not change with `-i`.
  ///
  /// <p>Both are reset at {@link Level#Iteration} rather than {@link Level#Trial} so that JMH's per-iteration raw
  /// data is per-iteration; a trial-scoped counter would make each iteration report the running total of the ones
  /// before it, and the ratio would still be right while every raw value was wrong.
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class CpuTimeCounter {
    private long _cpuNanos;
    private long _cpuOps;

    /// JMH names the secondary metric after this method, so the column is reported as `cpuNanos`.
    public long cpuNanos() {
      return _cpuNanos;
    }

    /// Invocations that contributed to [#cpuNanos], reported as `cpuOps`. The divisor, not a result in itself.
    public long cpuOps() {
      return _cpuOps;
    }

    /// Only called when metering is on, so `cpuOps` stays 0 for an unmetered run. That matters because the figure to
    /// report is `cpuNanos / cpuOps`: counting operations that contributed no CPU reading would turn "not measured"
    /// into a confident 0 ms/op.
    void add(long cpuNanos) {
      _cpuNanos += cpuNanos;
      _cpuOps++;
    }

    @Setup(Level.Iteration)
    public void reset() {
      _cpuNanos = 0;
      _cpuOps = 0;
    }
  }

  @Setup(Level.Trial)
  public void setUp() {
    _cpuMeter = SortedMergeCpuMeter.fromSystemProperty();
    _executorService = SortedMergeDriver.newBoundedExecutor(_maxExecutionThreads, _cpuMeter);
    _segments = SortedMergeFixture.segments(_overlap, _numSegments, _rowsPerSegment, _keyCardinality);
    int depth = SortedMergeFixture.measuredDepth(_overlap, _numSegments, _rowsPerSegment, _keyCardinality);
    // Parse the query and build the hundred per-segment plan nodes once, here, rather than inside the timed method.
    // That cost is the same on both arms so it would not change which one wins, but it is a fixed addition to every
    // measurement, and at small LIMIT it is large next to the merge itself -- which is exactly where the model
    // predicts the two arms should be indistinguishable. A fixed overhead added equally to both arms is the most
    // effective way to turn a real dead heat into an unreadable one.
    _prepared = SortedMergeDriver.prepare(_segments, _limit, _mode, _blockSize, _maxExecutionThreads, _orderBy);
    // G1: fail the trial now rather than silently comparing an arm against itself. run() throws if the planned
    // combine operator does not match the arm.
    SortedMergeDriver.Result probe = SortedMergeDriver.run(_prepared, _executorService, false, false);
    System.out.printf("[arm4] setup: overlap=%s depth=%d mode=%s limit=%d orderBy=%s blockSize=%d "
            + "maxExecutionThreads=%d keyCardinality=%d numTasks=%d segmentsProcessed=%d operator=%s "
            + "cpuMetering=%s%n", _overlap, depth, _mode, _limit, _orderBy, _blockSize, _maxExecutionThreads,
        _keyCardinality, probe._numTasks, probe._numSegmentsProcessed, probe._combineOperatorClass.getSimpleName(),
        _cpuMeter.isEnabled());
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _executorService.shutdownNow();
    // Segments are shared across trials via the fixture cache and are released by SortedMergeFixture#destroyAll in
    // the forked JVM's shutdown hook below, not here.
  }

  /// `collectRows` and `computeDigest` are both off: the row list and the multiset digest exist for the correctness
  /// gate, and doing either here would charge the harness's own work -- up to a million row hashes per invocation --
  /// to the operator under measurement.
  @Benchmark
  public long mergeTopK(CpuTimeCounter cpuTimeCounter) {
    if (!_cpuMeter.isEnabled()) {
      return SortedMergeDriver.run(_prepared, _executorService, false, false)._numRows;
    }
    long cpuNanosBefore = _cpuMeter.cpuNanosBeforeWork();
    long numRows = SortedMergeDriver.run(_prepared, _executorService, false, false)._numRows;
    cpuTimeCounter.add(_cpuMeter.cpuNanosAfterWork() - cpuNanosBefore);
    return numRows;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkSortedMergeMemoryCrossover.class.getSimpleName()).build()).run();
  }
}
