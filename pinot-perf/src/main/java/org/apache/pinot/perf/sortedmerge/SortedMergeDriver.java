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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.combine.MinMaxValueBasedSelectionOrderByCombineOperator;
import org.apache.pinot.core.operator.combine.StreamingSelectionOrderByCombineOperator;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;


/// Runs one selection ORDER BY combine over a [SortedMergeFixture] fixture, on either arm of the arm-4 benchmark.
///
/// <p>Both arms come from a single code path with one flag flipped. The combine is always built with a
/// {@link ResultsBlockStreamer} present, so `sortedSelectionMergeMode=ON` selects
/// {@link StreamingSelectionOrderByCombineOperator} and `OFF` falls through to
/// {@link MinMaxValueBasedSelectionOrderByCombineOperator}. No baseline worktree or second harness is needed, and the
/// two arms therefore differ only in the operator under test.
///
/// <p>The `OFF` arm would be identical with a null streamer, and that equivalence is worth recording because it is
/// not obvious. Plan nodes never see the streamer; for a selection query that has an ORDER BY,
/// `InstancePlanMakerImplV2#makeStreamingSegmentPlanNode` delegates straight to `makeSegmentPlanNode` because
/// `isSelectionOnlyQuery` is false, so the leaf is `SelectionPlanNode` either way and varies only on the merge mode.
/// In the combine, a null streamer skips the streaming block entirely while a non-null streamer with the mode `OFF`
/// falls through it, and both reach the same `CombinePlanNode#createSelectionCombineOperator` call. The streamer is
/// kept non-null here because it models the decision a streaming-capable server actually faces -- take the sorted
/// merge, or not -- rather than the SSE path where the mode is force-cleared and the feature is unreachable.
///
/// <p>The combine is constructed directly via {@link CombinePlanNode} rather than through
/// `InstancePlanMakerImplV2#makeInstancePlan`, which force-clears the mode for a non-streaming query. No plan-maker
/// resolution is needed here because the mode is always set explicitly to `ON` or `OFF`, never `AUTO`.
///
/// <p>Note that the mode gates the streaming *leaf* too (`SelectionPlanNode`), so `ON` swaps leaf and combine
/// together. Results measure the feature as a whole and must not be reported as a combine-only effect.
public final class SortedMergeDriver {
  private SortedMergeDriver() {
  }

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();

  /// The two query shapes this harness drives. Both project the same three columns
  /// ({@link SortedMergeFixture#TS_COL}, {@link SortedMergeFixture#VAL_COL}, {@link SortedMergeFixture#PAYLOAD_COL})
  /// and differ only in the ORDER BY clause.
  ///
  /// <p>`SelectionPlanNode#run` computes `sortedColumnsPrefixSize`, the length of the ORDER BY prefix that is a
  /// physically sorted column ({@link SortedMergeFixture#TS_COL} here). When that prefix covers the *whole*
  /// ORDER BY, the per-cursor scan block is tightened from `DocIdSetPlanNode.MAX_DOC_PER_CALL` to
  /// `min(limit + offset, MAX_DOC_PER_CALL)`, and `StreamingSelectionOrderByOperator` takes its "no tail to sort"
  /// emission path with no per-run heap at all.
  ///
  /// <p>{@link #TS_VAL} appends the globally unique {@link SortedMergeFixture#VAL_COL} as a tiebreaker specifically
  /// to defeat that tightening: with it, the ORDER BY has two expressions but only the first is a sorted column, so
  /// the prefix (1) is shorter than the ORDER BY (2) and the block stays pinned at `MAX_DOC_PER_CALL` in every cell.
  /// That total order is also what makes the strict gate under this shape possible -- see [#TS_ONLY] and
  /// [SortedMergeMemoryProbe] for why the gate cannot be as strong there.
  ///
  /// <p>{@link #TS_ONLY} drops the tiebreaker on purpose, to exercise the tightened block size and the "no tail to
  /// sort" path that {@link #TS_VAL} structurally cannot reach.
  public enum OrderBy {
    /// `ORDER BY tsCol, valCol`. A total order; every returned-row-set is uniquely determined by (overlap, limit).
    TS_VAL,
    /// `ORDER BY tsCol` alone. Not a total order once {@link SortedMergeFixture#TS_COL} repeats across segments
    /// (PARTIAL and FULL overlap): rows tying on `tsCol` may straddle the LIMIT boundary, and two correct operators
    /// may legitimately return different row sets while agreeing on every value that determines correctness.
    /// Consequently [SortedMergeMemoryProbe]'s gate is *deliberately* weaker for this shape -- it cannot assert full
    /// row-set equality without producing false failures on ties it cannot adjudicate, so it falls back to the
    /// weakest comparison that is still sound: row count, the multiset of `tsCol` values, and sortedness. That
    /// weakening is a consequence of the shape, not an oversight, and the gate's output line says so explicitly.
    TS_ONLY
  }

  /// The executor every arm-4 run must use. Bounded, never cached.
  ///
  /// <p>This is load-bearing for the validity of the comparison, not a detail. A real Pinot server runs segment
  /// tasks on a bounded query worker pool, so an unbounded cached pool would let all of the combine's `_numTasks`
  /// run at once. The `OFF` arm's retention scales with how many per-segment top-K structures are in flight, so an
  /// unbounded pool inflates its memory beyond anything production would see -- biasing the result in favour of the
  /// `ON` arm, which is the feature under test. Centralized here so that reasoning lives in one place and cannot be
  /// updated in one call site while the others silently keep the bias.
  public static ExecutorService newBoundedExecutor(int maxExecutionThreads) {
    return newBoundedExecutor(maxExecutionThreads, new SortedMergeCpuMeter(false));
  }

  /// As [#newBoundedExecutor(int)], but builds the pool through `cpuMeter`'s thread factory so the meter can attribute
  /// pool CPU time to the arm under test. An executor built by the other overload is invisible to any meter, which
  /// would make the `OFF` arm read as single-threaded; prefer this one wherever CPU time is being recorded.
  public static ExecutorService newBoundedExecutor(int maxExecutionThreads, SortedMergeCpuMeter cpuMeter) {
    return Executors.newFixedThreadPool(maxExecutionThreads, cpuMeter.threadFactory("arm4-combine"));
  }

  /// Result of one combine run. Rows are retained only when the caller asks for them; the memory arms must not hold a
  /// million rows in the harness while measuring how many rows the operator holds.
  public static class Result {
    public long _numRows;
    /// Order-independent digest of the returned rows: the sum of per-row hashes. Commutative, so it compares the
    /// multiset without materialising or sorting it. Collisions are possible in principle and this is a gate rather
    /// than a proof; small limits are additionally checked with a full canonical multiset comparison.
    public long _rowDigest;
    /// Order-independent digest of the returned rows' {@link SortedMergeFixture#TS_COL} values alone, accumulated the
    /// same commutative way as [#_rowDigest]. This is the digest the [OrderBy#TS_ONLY] gate compares, and it exists
    /// because under that shape [#_rowDigest] is unusable: ties on `tsCol` straddle the LIMIT boundary, so two
    /// correct arms may legitimately return different *rows*. Which tied rows come back is free; how many rows carry
    /// each distinct `tsCol` value is not, so a digest over those values alone is sound under ties. Unlike a
    /// materialised multiset it needs no row retention, which is what lets the gate keep checking at limits above
    /// `FULL_MULTISET_LIMIT` instead of falling back to a bare row count.
    public long _tsDigest;
    public int _numBlocks;
    public Class<?> _combineOperatorClass;
    /// The number of combine tasks (`W`) the query was actually planned with, from
    /// [QueryMultiThreadingUtils#getNumTasksForQuery]. This is the concurrency the `OFF` arm's retention scales
    /// with, so it must be recorded alongside every run rather than left to whatever the host resolves the default
    /// to.
    public int _numTasks;
    /// How many segments the combine actually processed, as opposed to how many segments existed. Recorded because
    /// `MinMaxValueBasedSelectionOrderByCombineOperator` prunes segments via min/max metadata (see its
    /// `_endOperatorId` logic), so at `DISJOINT` overlap with a small `LIMIT` only a handful of the `W` planned
    /// tasks may have touched a real segment. See {@link #run} for exactly where this is read from for each arm.
    ///
    /// <p>Sourced from `BaseResultsBlock#getNumSegmentsMatched()`, not `getNumSegmentsProcessed()`:
    /// `BaseCombineOperator#attachExecutionStats` sets `numSegmentsProcessed` to the total operator count
    /// unconditionally (`_operators.size()`), which does not reflect pruning at all, while `numSegmentsMatched`
    /// counts only the operators whose `getNumDocsScanned() > 0`, i.e. the ones a segment-level operator's
    /// `nextBlock()` actually ran on. A pruned segment's operator is never invoked, so it never scans a doc and is
    /// correctly excluded.
    public int _numSegmentsProcessed;
    /// Populated only when `collectRows` is set.
    public List<Object[]> _rows;
  }

  /// As [#buildQueryContext(int, SortedSelectionMergeMode, int, int, OrderBy)] with the existing [OrderBy#TS_VAL]
  /// shape, for callers written before the shape parameter existed.
  public static QueryContext buildQueryContext(int limit, SortedSelectionMergeMode mode, int blockSize,
      int maxExecutionThreads) {
    return buildQueryContext(limit, mode, blockSize, maxExecutionThreads, OrderBy.TS_VAL);
  }

  /// Builds the query context for one configuration.
  ///
  /// <p>Under [OrderBy#TS_VAL] the ORDER BY ends with the globally unique {@link SortedMergeFixture#VAL_COL} so the
  /// comparator is a total order and the top-K boundary is unambiguous; the merge still genuinely interleaves
  /// segments because {@link SortedMergeFixture#TS_COL} overlaps across them. Under [OrderBy#TS_ONLY] the
  /// tiebreaker is dropped so that `sortedColumnsPrefixSize == orderByExpressions.size()` in `SelectionPlanNode`,
  /// which is the shape that reaches the tightened per-cursor block size and the "no tail to sort" path -- see
  /// [OrderBy] for the full reasoning and its correctness-gate consequence.
  ///
  /// <p>`maxExecutionThreads` is set explicitly rather than left at the `QueryContext` default of -1. Left unset,
  /// `QueryMultiThreadingUtils#getNumTasks` resolves -1 to `MAX_NUM_THREADS_PER_QUERY`, i.e.
  /// `max(1, min(10, numCores / 2))`, which is host-dependent (10 on this 96-core box, 6 on a 12-core laptop) and
  /// would make the `OFF` arm's task count -- and therefore its retention -- incomparable across machines.
  public static QueryContext buildQueryContext(int limit, SortedSelectionMergeMode mode, int blockSize,
      int maxExecutionThreads, OrderBy orderBy) {
    String orderByClause = orderBy == OrderBy.TS_ONLY ? SortedMergeFixture.TS_COL
        : SortedMergeFixture.TS_COL + ", " + SortedMergeFixture.VAL_COL;
    String query = "SELECT " + SortedMergeFixture.TS_COL + ", " + SortedMergeFixture.VAL_COL + ", "
        + SortedMergeFixture.PAYLOAD_COL + " FROM " + SortedMergeFixture.RAW_TABLE_NAME + " ORDER BY "
        + orderByClause + " LIMIT " + limit;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSortedSelectionMergeMode(mode);
    if (blockSize > 0) {
      queryContext.setSortedSelectionMergeBlockSize(blockSize);
    }
    queryContext.setMaxExecutionThreads(maxExecutionThreads);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    return queryContext;
  }

  /// A query context plus the per-segment plan nodes built from it, prepared once and reused across many runs.
  ///
  /// <p>Exists so the timed path does not re-parse SQL and rebuild a hundred plan nodes on every invocation. That
  /// per-call overhead is identical on both arms so it does not change which one wins, but it is a fixed cost added
  /// to every measurement, and at small `LIMIT` -- where the whole merge is only a few thousand rows -- it is large
  /// relative to the signal. The most interesting predicted cell in the grid is a *dead heat*, and a fixed overhead
  /// added equally to both arms is exactly what would turn a real dead heat into noise.
  ///
  /// <p>Reuse is safe because the plan nodes are stateless factories: `SelectionPlanNode#run` reads the query and
  /// segment context and returns freshly constructed operators on each call. The operators themselves are single-use
  /// and are still built per invocation, inside {@link CombinePlanNode#run}.
  public static class PreparedQuery {
    private final QueryContext _queryContext;
    private final List<PlanNode> _planNodes;
    private final int _numSegments;
    private final OrderBy _orderBy;

    PreparedQuery(QueryContext queryContext, List<PlanNode> planNodes, int numSegments, OrderBy orderBy) {
      _queryContext = queryContext;
      _planNodes = planNodes;
      _numSegments = numSegments;
      _orderBy = orderBy;
    }

    /// The query shape this was prepared with. The gate needs this to know which comparison strength applies --
    /// see [SortedMergeMemoryProbe].
    public OrderBy getOrderBy() {
      return _orderBy;
    }
  }

  /// As [#prepare(List, int, SortedSelectionMergeMode, int, int, OrderBy)] with the existing [OrderBy#TS_VAL] shape,
  /// for callers written before the shape parameter existed.
  public static PreparedQuery prepare(List<IndexSegment> segments, int limit, SortedSelectionMergeMode mode,
      int blockSize, int maxExecutionThreads) {
    return prepare(segments, limit, mode, blockSize, maxExecutionThreads, OrderBy.TS_VAL);
  }

  /// Parses the query and builds the per-segment plan nodes once, for repeated execution by [#run(PreparedQuery,
  /// ExecutorService, boolean, boolean)].
  public static PreparedQuery prepare(List<IndexSegment> segments, int limit, SortedSelectionMergeMode mode,
      int blockSize, int maxExecutionThreads, OrderBy orderBy) {
    QueryContext queryContext = buildQueryContext(limit, mode, blockSize, maxExecutionThreads, orderBy);
    List<PlanNode> planNodes = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      planNodes.add(PLAN_MAKER.makeStreamingSegmentPlanNode(new SegmentContext(segment), queryContext));
    }
    return new PreparedQuery(queryContext, planNodes, segments.size(), orderBy);
  }

  /// Plans and drains one combine over an already-prepared query.
  ///
  /// <p>Guard G1: the concrete combine operator class is asserted against the arm before anything is measured. This
  /// is the single check that catches the whole silent-no-op family -- a mode that was force-cleared, an option that
  /// never took effect, a gate that did not fire -- each of which would otherwise produce a clean run comparing an
  /// arm against itself.
  public static Result run(PreparedQuery prepared, ExecutorService executorService, boolean collectRows,
      boolean computeDigest) {
    QueryContext queryContext = prepared._queryContext;
    // The deadline is absolute, so a context prepared once and reused would expire partway through a long JMH trial
    // and start failing every query. Refresh it per run.
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    SortedSelectionMergeMode mode = queryContext.getSortedSelectionMergeMode();
    // A no-op streamer. Its only job is to be non-null, which is what puts CombinePlanNode on the streaming branch;
    // the streaming combine returns its data through nextBlock(), not through the streamer.
    ResultsBlockStreamer streamer = block -> {
    };
    Operator<?> combineOperator =
        new CombinePlanNode(prepared._planNodes, queryContext, executorService, streamer).run();

    Class<?> expected = mode == SortedSelectionMergeMode.ON ? StreamingSelectionOrderByCombineOperator.class
        : MinMaxValueBasedSelectionOrderByCombineOperator.class;
    if (combineOperator.getClass() != expected) {
      throw new IllegalStateException(
          "G1 engagement check failed for mode " + mode + ": expected " + expected.getSimpleName() + " but the plan "
              + "produced " + combineOperator.getClass().getSimpleName() + ". The arm is not measuring what it "
              + "claims to; do not report this run.");
    }

    Result result = new Result();
    result._combineOperatorClass = combineOperator.getClass();
    result._numTasks =
        QueryMultiThreadingUtils.getNumTasksForQuery(prepared._numSegments, queryContext.getMaxExecutionThreads());
    if (collectRows) {
      result._rows = new ArrayList<>();
    }
    if (mode == SortedSelectionMergeMode.ON) {
      // Drive the streaming combine: bounded data blocks until the terminal metadata block. The streaming combine
      // (see StreamingSelectionOrderByCombineOperator#getNextBlock) only calls attachExecutionStats() when it builds
      // the terminal MetadataResultsBlock; the intermediate SelectionResultsBlocks carry no execution statistics, so
      // the segments-processed count has to be read off that terminal block.
      while (true) {
        BaseResultsBlock block = (BaseResultsBlock) combineOperator.nextBlock();
        if (block instanceof MetadataResultsBlock) {
          result._numSegmentsProcessed = block.getNumSegmentsMatched();
          break;
        }
        accumulate(result, ((SelectionResultsBlock) block).getRows(), collectRows, computeDigest);
        if (result._numBlocks > 1_000_000) {
          throw new IllegalStateException("Streaming combine did not terminate");
        }
      }
    } else {
      // The non-streaming combine attaches execution statistics directly to the single SelectionResultsBlock it
      // returns (BaseSingleBlockCombineOperator#getNextBlock -> checkTerminateExceptionAndAttachExecutionStats), so
      // the count is read off it here.
      SelectionResultsBlock block = (SelectionResultsBlock) combineOperator.nextBlock();
      result._numSegmentsProcessed = block.getNumSegmentsMatched();
      accumulate(result, block.getRows(), collectRows, computeDigest);
    }
    return result;
  }

  /// Accumulates one block's rows. `computeDigest` is off on the timed path: the digests exist for the correctness
  /// gate, and hashing up to a million rows per invocation would add harness cost to every measurement of an
  /// operator that is not doing the hashing.
  ///
  /// <p>Both digests are accumulated together under the one flag. They are cheap relative to each other, only the
  /// gate ever asks for either, and which one is *compared* is the gate's decision rather than this method's: see
  /// [Result#_rowDigest] and [Result#_tsDigest].
  private static void accumulate(Result result, List<Object[]> rows, boolean collectRows, boolean computeDigest) {
    if (rows == null) {
      return;
    }
    result._numBlocks++;
    result._numRows += rows.size();
    if (computeDigest) {
      for (Object[] row : rows) {
        result._rowDigest += rowHash(row);
        result._tsDigest += tsHash(row);
      }
    }
    if (collectRows) {
      result._rows.addAll(rows);
    }
  }

  /// Hashes a row including each cell's runtime class, so a stored-type or boxing difference between the two arms
  /// (a LONG emitted where the other emits INT) changes the digest instead of hiding behind a type-blind comparison.
  private static long rowHash(Object[] row) {
    long hash = 1L;
    for (Object cell : row) {
      long cellHash = cell == null ? 0L : 31L * cell.getClass().getName().hashCode() + cell.hashCode();
      hash = hash * 1_000_003L + cellHash;
    }
    return hash;
  }

  /// Hashes only a row's {@link SortedMergeFixture#TS_COL} cell, for [Result#_tsDigest]. Includes the cell's runtime
  /// class for the same reason [#rowHash] does, so a LONG emitted where the other arm emits INT changes the digest
  /// rather than comparing equal through a type-blind widening.
  @VisibleForTesting
  static long tsHash(Object[] row) {
    Object cell = row[0];
    return cell == null ? 0L : 31L * cell.getClass().getName().hashCode() + cell.hashCode();
  }

  /// Canonical, order-independent rendering of collected rows, for the full multiset comparison at small limits.
  public static List<String> canonicalize(List<Object[]> rows) {
    List<String> canonical = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < row.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        Object cell = row[i];
        sb.append(cell == null ? "null" : cell.getClass().getSimpleName() + ":" + cell);
      }
      canonical.add(sb.append(']').toString());
    }
    canonical.sort(null);
    return canonical;
  }

  /// Sorted multiset of {@link SortedMergeFixture#TS_COL} values, i.e. `row[0]` of every row (the SELECT list is
  /// always `tsCol, valCol, payloadCol`). Used by the [OrderBy#TS_ONLY] gate in place of the full-row comparisons:
  /// under that shape ties on `tsCol` legitimately straddle the LIMIT boundary, so *which* tied rows come back is
  /// not determined, but *how many* rows carry each distinct `tsCol` value is, and that is exactly what this
  /// captures.
  public static List<Long> tsColumnMultiset(List<Object[]> rows) {
    List<Long> values = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      values.add(((Number) row[0]).longValue());
    }
    values.sort(null);
    return values;
  }

  /// Asserts the order-by is respected within the returned rows. Cheap sanity check on top of the multiset gate: the
  /// two arms could agree on the multiset and still differ on whether the output is globally sorted.
  public static void assertSortedByTs(List<Object[]> rows) {
    for (int i = 1; i < rows.size(); i++) {
      long previous = ((Number) rows.get(i - 1)[0]).longValue();
      long current = ((Number) rows.get(i)[0]).longValue();
      if (previous > current) {
        throw new IllegalStateException(
            "Rows are not sorted by " + SortedMergeFixture.TS_COL + " at position " + i + ": " + Arrays.toString(
                rows.get(i - 1)) + " then " + Arrays.toString(rows.get(i)));
      }
    }
  }
}
