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
import javax.annotation.Nullable;
import org.apache.pinot.perf.sortedmerge.SortedMergeDriver.OrderBy;
import org.apache.pinot.perf.sortedmerge.SortedMergeDriver.Result;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;


/// Arm 4, instrument 2 and the step-3 correctness gate.
///
/// <p>Two modes, both deliberately outside JMH:
///
/// <p><b>`gate`</b> -- runs every configuration on both arms and checks that they return the same rows. This is the
/// stop-gate: a memory comparison between two operators that disagree about the answer is meaningless, so nothing
/// downstream runs until this passes. Comparison is by count plus an order-independent digest, never by row order:
/// the two paths legitimately differ on the ordering of rows that tie on every order-by key. Small limits are
/// additionally compared as a full canonical multiset, which also catches a stored-type or boxing difference.
///
/// <p><b>`run`</b> -- runs exactly one configuration once and exits: 0 on success, 3 on {@link OutOfMemoryError},
/// 1 on anything else. A shell loop bisects `-Xmx` over this to find the minimum heap each arm survives on. Crude and
/// slow, and the only instrument here that answers "what memory does this actually need" rather than "how much does
/// it churn".
public final class SortedMergeMemoryProbe {
  private SortedMergeMemoryProbe() {
  }

  private static final int EXIT_OOM = 3;
  private static final int EXIT_ERROR = 1;

  /// Above this limit the gate compares counts and digests only. Materialising both arms' rows for a full multiset
  /// comparison at a million rows would cost more heap than the operators under test.
  private static final int FULL_MULTISET_LIMIT = 10_000;

  private static final int[] GATE_LIMITS = {10, 100, 1_000, 10_000, 100_000, 1_000_000};

  public static void main(String[] args) {
    if (args.length == 0) {
      usage();
      System.exit(EXIT_ERROR);
    }
    try {
      switch (args[0]) {
        case "gate":
          System.exit(gate(args));
          break;
        case "run":
          System.exit(runOnce(args));
          break;
        default:
          usage();
          System.exit(EXIT_ERROR);
          break;
      }
    } catch (OutOfMemoryError e) {
      // Caught deliberately: for the bisection an OOM is the measurement, not a crash. Print before anything else
      // allocates.
      System.out.println("OOM");
      System.exit(EXIT_OOM);
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(EXIT_ERROR);
    }
  }

  /// Default for the trailing `maxExecutionThreads` CLI argument on both subcommands: what
  /// `QueryMultiThreadingUtils#MAX_NUM_THREADS_PER_QUERY` resolves to on a host with >= 20 cores, matching the
  /// single JMH `@Param` value in {@link BenchmarkSortedMergeMemoryCrossover}.
  private static final int DEFAULT_MAX_EXECUTION_THREADS = 10;

  private static void usage() {
    System.err.println("Usage:");
    System.err.println(
        "  gate [numSegments] [rowsPerSegment] [blockSize] [maxExecutionThreads] [orderBy] [keyCardinality]");
    System.err.println("  run <overlap> <mode> <limit> [numSegments] [rowsPerSegment] [blockSize] "
        + "[maxExecutionThreads] [orderBy] [keyCardinality]");
    System.err.println("  orderBy is one of " + Arrays.toString(OrderBy.values())
        + ", default " + OrderBy.TS_VAL + " (kept for CLI backward compatibility).");
    System.err.println("  keyCardinality defaults to " + SortedMergeFixture.DEFAULT_KEY_CARDINALITY
        + " (kept for CLI backward compatibility).");
  }

  /// Step 3, guard G2. Returns 0 if every configuration agrees across the two arms.
  ///
  /// <p>The strength of "agrees" depends on [OrderBy]. Under [OrderBy#TS_VAL] the ORDER BY is a total order, so the
  /// gate asserts full row-set equality: row count, the order-independent full-row digest, and (at small limits) a
  /// full canonical multiset. Under [OrderBy#TS_ONLY] ties on `tsCol` legitimately straddle the LIMIT boundary, so
  /// that comparison would produce false failures; the gate instead checks row count, a commutative digest over
  /// `tsCol` values, and -- where rows are retained -- their full multiset and sortedness. See
  /// [SortedMergeDriver.OrderBy#TS_ONLY].
  ///
  /// <p>Both digests are computed for both shapes; which one is *compared* is decided here, per shape, and never by
  /// omission. The full-row digest is meaningless under [OrderBy#TS_ONLY] and is simply not consulted on that branch.
  /// Every gate line prints a `checked=[...]` tag naming the level that actually ran, so a cell that fell back to a
  /// weaker comparison says so instead of reading like a clean pass.
  private static int gate(String[] args) {
    int numSegments = intArg(args, 1, SortedMergeFixture.DEFAULT_NUM_SEGMENTS);
    int rowsPerSegment = intArg(args, 2, SortedMergeFixture.DEFAULT_ROWS_PER_SEGMENT);
    int blockSize = intArg(args, 3, 10_000);
    int maxExecutionThreads = intArg(args, 4, DEFAULT_MAX_EXECUTION_THREADS);
    OrderBy orderBy = orderByArg(args, 5);
    int keyCardinality = intArg(args, 6, SortedMergeFixture.DEFAULT_KEY_CARDINALITY);

    ExecutorService executorService = SortedMergeDriver.newBoundedExecutor(maxExecutionThreads);
    List<String> failures = new ArrayList<>();
    boolean strict = orderBy == OrderBy.TS_VAL;
    try {
      for (SortedMergeFixture.Overlap overlap : SortedMergeFixture.Overlap.values()) {
        List<IndexSegment> segments =
            SortedMergeFixture.segments(overlap, numSegments, rowsPerSegment, keyCardinality);
        int depth = SortedMergeFixture.measuredDepth(overlap, numSegments, rowsPerSegment, keyCardinality);
        for (int limit : GATE_LIMITS) {
          boolean collectRows = limit <= FULL_MULTISET_LIMIT;
          SortedMergeDriver.Result off =
              SortedMergeDriver.run(
                  SortedMergeDriver.prepare(segments, limit, SortedSelectionMergeMode.OFF, blockSize,
                      maxExecutionThreads, orderBy), executorService, collectRows, true);
          SortedMergeDriver.Result on =
              SortedMergeDriver.run(
                  SortedMergeDriver.prepare(segments, limit, SortedSelectionMergeMode.ON, blockSize,
                      maxExecutionThreads, orderBy), executorService, collectRows, true);
          String config =
              overlap + " depth=" + depth + " limit=" + limit + " keyCardinality=" + keyCardinality;
          String failureReason = cellVerdict(off, on, orderBy, collectRows);
          if (failureReason != null) {
            failures.add(config + ": " + failureReason);
            continue;
          }

          if (strict) {
            System.out.printf(
                "[arm4] gate OK  %-40s rows=%d blocks OFF=%d ON=%d numTasks=%d segmentsProcessed OFF=%d ON=%d%n",
                config, on._numRows, off._numBlocks, on._numBlocks, off._numTasks, off._numSegmentsProcessed,
                on._numSegmentsProcessed);
          } else {
            // The checked=[...] tag makes the weaker level visible rather than letting it pass silently. The
            // tsCol-value digest is sound under ties and needs no retained rows, so unlike the full-row digest it
            // is available at every limit. That is what stops the cells above FULL_MULTISET_LIMIT -- which is
            // exactly where the crossover this harness exists to locate sits -- from degrading to a bare row
            // count, which would pass for two arms returning completely different keys.
            String checkLevel = collectRows
                ? "tsCol digest + tsCol multiset + sortedness (no full-row check: ties are not a total order)"
                : "tsCol digest only (rows not retained above FULL_MULTISET_LIMIT, so no multiset and no "
                    + "sortedness check)";
            // Whether the two arms actually chose different tied rows. Both full-row digests are computed, and
            // under this shape they are allowed to differ -- that freedom is the whole reason the strict comparison
            // had to be relaxed. Printing it turns "the gate was weakened" from an assertion into an observation:
            // a cell reading "yes" is one the strict gate would have failed on despite both arms being correct.
            String tiedRowsDiffer = off._rowDigest == on._rowDigest ? "no" : "yes";
            System.out.printf(
                "[arm4] gate OK  %-40s orderBy=%-7s rows=%d blocks OFF=%d ON=%d numTasks=%d segmentsProcessed "
                    + "OFF=%d ON=%d armsPickedDifferentTiedRows=%s checked=[%s]%n", config, orderBy, on._numRows,
                off._numBlocks, on._numBlocks, off._numTasks, off._numSegmentsProcessed, on._numSegmentsProcessed,
                tiedRowsDiffer, checkLevel);
          }
        }
      }
    } finally {
      executorService.shutdownNow();
    }

    if (failures.isEmpty()) {
      System.out.println("[arm4] gate PASSED for all configurations");
      return 0;
    }
    System.out.println("[arm4] gate FAILED:");
    for (String failure : failures) {
      System.out.println("  " + failure);
    }
    return EXIT_ERROR;
  }

  /// The per-cell pass/fail decision behind {@link #gate}, pulled out so the choice of strict versus tie-sound
  /// comparison -- and which checks run at which limits -- has exactly one implementation and can be driven directly
  /// from a test instead of only through a full `gate` run, where a correct pair of arms never exercises the fail
  /// path at all. Pure: no printing, no I/O, no exit codes. Returns the failure reason to report for this cell, or
  /// `null` when it passes.
  ///
  /// <p>The one exception to "pure": a sortedness violation is a hard invariant break, not a soft mismatch, so it is
  /// reported by letting {@link SortedMergeDriver#assertSortedByTs} throw rather than by returning a reason string --
  /// exactly as `gate` did before this method existed.
  @VisibleForTesting
  @Nullable
  static String cellVerdict(Result off, Result on, OrderBy orderBy, boolean collectRows) {
    if (off._numRows != on._numRows) {
      return "row count OFF=" + off._numRows + " ON=" + on._numRows;
    }

    if (orderBy == OrderBy.TS_VAL) {
      // OrderBy.TS_VAL, the pre-existing shape: output is byte-identical to before OrderBy existed.
      if (off._rowDigest != on._rowDigest) {
        return "multiset digest differs (OFF=" + off._rowDigest + " ON=" + on._rowDigest + ")";
      }
      if (collectRows) {
        if (!SortedMergeDriver.canonicalize(off._rows).equals(SortedMergeDriver.canonicalize(on._rows))) {
          return "canonical multiset differs";
        }
        // Both arms, not just ON. The OFF arm's combine returns a priority-queue-backed block, so its row
        // order is worth asserting rather than assuming; the TS_ONLY branch already checks both and passes,
        // which is the evidence that this is a real invariant and not a spurious failure waiting to happen.
        SortedMergeDriver.assertSortedByTs(off._rows);
        SortedMergeDriver.assertSortedByTs(on._rows);
      }
      return null;
    }

    // OrderBy.TS_ONLY: the order is not total, so the full-row digest -- which is still computed, and is what
    // the caller's armsPickedDifferentTiedRows is derived from -- must not be compared here, because two correct
    // arms are free to return different tied rows. Fall back to the weaker, tie-sound comparison:
    // the multiset of tsCol values (which rows tied on tsCol come back is not determined, but how many
    // carry each distinct value is) plus sortedness, both of which still need materialised rows.
    if (off._tsDigest != on._tsDigest) {
      return "tsCol value digest differs (OFF=" + off._tsDigest + " ON=" + on._tsDigest + ")";
    }
    if (collectRows) {
      if (!SortedMergeDriver.tsColumnMultiset(off._rows).equals(SortedMergeDriver.tsColumnMultiset(on._rows))) {
        return "tsCol value multiset differs";
      }
      SortedMergeDriver.assertSortedByTs(off._rows);
      SortedMergeDriver.assertSortedByTs(on._rows);
    }
    return null;
  }

  /// One configuration, once. Success means the run completed inside the JVM's heap; the caller learns the heap it
  /// needed by bisecting `-Xmx` around the boundary between exit 0 and exit 3.
  private static int runOnce(String[] args) {
    if (args.length < 4) {
      usage();
      return EXIT_ERROR;
    }
    SortedMergeFixture.Overlap overlap = SortedMergeFixture.Overlap.valueOf(args[1]);
    SortedSelectionMergeMode mode = SortedSelectionMergeMode.valueOf(args[2]);
    int limit = Integer.parseInt(args[3]);
    int numSegments = intArg(args, 4, SortedMergeFixture.DEFAULT_NUM_SEGMENTS);
    int rowsPerSegment = intArg(args, 5, SortedMergeFixture.DEFAULT_ROWS_PER_SEGMENT);
    int blockSize = intArg(args, 6, 10_000);
    int maxExecutionThreads = intArg(args, 7, DEFAULT_MAX_EXECUTION_THREADS);
    OrderBy orderBy = orderByArg(args, 8);
    int keyCardinality = intArg(args, 9, SortedMergeFixture.DEFAULT_KEY_CARDINALITY);

    ExecutorService executorService = SortedMergeDriver.newBoundedExecutor(maxExecutionThreads);
    try {
      List<IndexSegment> segments =
          SortedMergeFixture.segments(overlap, numSegments, rowsPerSegment, keyCardinality);
      long startNs = System.nanoTime();
      SortedMergeDriver.Result result =
          SortedMergeDriver.run(
              SortedMergeDriver.prepare(segments, limit, mode, blockSize, maxExecutionThreads, orderBy),
              executorService, false, false);
      System.out.printf(
          "OK overlap=%s mode=%s limit=%d orderBy=%s keyCardinality=%d rows=%d blocks=%d elapsedMs=%d maxHeapMb=%d "
              + "numTasks=%d segmentsProcessed=%d%n", overlap, mode, limit, orderBy, keyCardinality,
          result._numRows, result._numBlocks, (System.nanoTime() - startNs) / 1_000_000,
          Runtime.getRuntime().maxMemory() >> 20, result._numTasks, result._numSegmentsProcessed);
      return 0;
    } finally {
      executorService.shutdownNow();
    }
  }

  private static int intArg(String[] args, int index, int defaultValue) {
    return args.length > index ? Integer.parseInt(args[index]) : defaultValue;
  }

  /// Parses the trailing `orderBy` CLI argument, defaulting to [OrderBy#TS_VAL] when absent so every invocation
  /// written before this parameter existed keeps meaning the shape it always meant.
  private static OrderBy orderByArg(String[] args, int index) {
    return args.length > index ? OrderBy.valueOf(args[index]) : OrderBy.TS_VAL;
  }
}
