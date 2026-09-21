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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/// Sums CPU time across every thread that does combine work, so the two arms can be compared on CPU consumed rather
/// than only on wall-clock time.
///
/// <p><b>Why this exists.</b> `StreamingSelectionOrderByCombineOperator#start` is a no-op and its `processSegments`
/// throws, so the `ON` arm merges entirely on the calling thread. The `OFF` arm fans out across `W` pool threads.
/// Every wall-clock ratio between the two is therefore one thread against `W` on an otherwise idle host, which is the
/// most favourable setting the `OFF` arm can be given. A production server shares a bounded worker pool across
/// concurrent queries, where CPU time per query governs throughput, so an arm that loses on wall clock can still win
/// on CPU. Wall-clock time cannot distinguish those two cases and this meter can.
///
/// <p><b>What is counted.</b> The calling thread plus every thread this meter's [#threadFactory] created, whether or
/// not that thread is currently running combine work. Pool threads blocked waiting for a task accrue no CPU time, so
/// including all of them costs accuracy nothing and avoids per-task bookkeeping. Threads the JVM owns (GC, JIT,
/// compiler) are excluded, which is the main reason to prefer this over `-prof perfnorm`: the figure is combine work
/// rather than whole-process work. Note that this also means GC cost driven by the arms' very different allocation
/// rates does not appear here.
///
/// <p><b>Usage, and why there are two snapshot methods.</b> Bracket the work with [#cpuNanosBeforeWork] and
/// [#cpuNanosAfterWork] and subtract. They are not interchangeable and the asymmetry is the point: reading the
/// per-thread bean is itself work charged to the calling thread, so a single method used at both ends would bill its
/// own bookkeeping to the interval it is supposed to bound. Because the pool has `W` registered threads on the `OFF`
/// arm and typically none on the `ON` arm, that error would be several times larger on `OFF` and would inflate
/// exactly the arm this meter exists to hold to account.
///
/// <p>[#cpuNanosBeforeWork] therefore reads the workers first and the caller last, and [#cpuNanosAfterWork] reads the
/// caller first and the workers last, so both loops fall outside the bracketed interval. The residual error is that
/// worker readings are taken microseconds before the caller's at the start and microseconds after it at the end,
/// across a window in which the workers are idle between invocations and accrue approximately no CPU.
///
/// <p><b>Cost.</b> Each snapshot makes one `getThreadCpuTime` call per pool thread plus one for the caller, so a
/// measured invocation costs roughly `2 * (W + 1)` such calls. Measured 2026-09-20 at FULL overlap, `OFF`, `W = 10`,
/// three iterations: `LIMIT 10` ran at 0.609 ms/op metered against 0.617 ms/op unmetered, and `LIMIT 10000` at 19.682
/// against 18.728. Both differences are far inside the error bars (+/- 1.356 and +/- 18.543), so the wall-clock
/// overhead is not resolvable at this sample size even in the shortest cell in the grid. That is a bound, not a
/// measurement of zero.
///
/// <p><b>Opt-in.</b> Metering is off unless `-Darm4.cpuTime=true` reaches the forked JVM
/// (`-jvmArgs "... -Darm4.cpuTime=true"`). The reason is comparability rather than cost: the 36-cell grid this
/// package produced was run without it, and a metered rerun should be an explicit choice rather than something that
/// silently changes what the default invocation measures. A disabled meter returns 0 without touching the bean.
/// The property shares the `arm4.` prefix with this package's existing `arm4.fixture.*` switches rather than the
/// `pinot.perf.<benchmark>.<flag>` shape used elsewhere in the module, so that one package has one convention.
///
/// <p>When metering is requested but the JVM cannot support it, construction fails rather than quietly reporting
/// zero. A silent zero would look like "the `ON` arm uses no CPU", which is exactly the kind of too-good result the
/// rest of this harness is built to refuse.
///
/// <p><b>Threading.</b> A meter must be read by one thread for its lifetime, because a delta taken across two
/// concurrent invocations would attribute each one's pool CPU to both. That is enforced: the first thread to take a
/// snapshot claims the meter and any other thread taking one fails. Benchmarks using this meter must also pin
/// `@Threads(1)`, so the failure is a backstop rather than the primary control. The thread registry written by the
/// pool's factory is safe to read concurrently.
///
/// <p>Run [#main] to self-check the aggregation against a known workload.
public final class SortedMergeCpuMeter {

  /// System property that turns metering on. Off by default; see the opt-in note in the class javadoc.
  public static final String ENABLED_PROPERTY = "arm4.cpuTime";

  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  /// A registered pool thread and the last non-negative CPU reading seen for it.
  ///
  /// <p>`getThreadCpuTime` returns -1 for a thread that has died, which would otherwise make a delta across an
  /// invocation negative: the thread's CPU counted towards the opening reading and then vanished from the closing
  /// one. Pool threads are not expected to die mid-run, so the fallback guards against a silently wrong number
  /// rather than serving an expected path. Held in a field rather than a map so the timed path allocates nothing;
  /// boxing a `Long` per worker per snapshot would land in `gc.alloc.rate.norm` and, since the arms register
  /// different numbers of workers, would bias the allocation comparison as well.
  private static final class Worker {
    private final Thread _thread;
    private long _lastKnownCpuNanos;

    private Worker(Thread thread) {
      _thread = thread;
    }

    /// Only ever called by the thread that claimed the meter, so the mutation needs no synchronisation.
    private long cpuNanos() {
      long cpuNanos = THREAD_MX_BEAN.getThreadCpuTime(_thread.threadId());
      if (cpuNanos >= 0) {
        _lastKnownCpuNanos = cpuNanos;
        return cpuNanos;
      }
      return _lastKnownCpuNanos;
    }
  }

  private final boolean _enabled;
  private final List<Worker> _workers = new CopyOnWriteArrayList<>();
  private final AtomicInteger _threadCounter = new AtomicInteger();
  private volatile Thread _readingThread;

  public SortedMergeCpuMeter(boolean enabled) {
    if (enabled) {
      if (!THREAD_MX_BEAN.isThreadCpuTimeSupported()) {
        throw new IllegalStateException(
            "CPU-time metering was requested via -D" + ENABLED_PROPERTY + "=true but this JVM does not support "
                + "per-thread CPU time. Re-run without it rather than reporting zeroes.");
      }
      if (!THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
        THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
      }
    }
    _enabled = enabled;
  }

  /// Reads [#ENABLED_PROPERTY]. Use this rather than hardcoding a boolean so that every instrument in this package
  /// turns metering on the same way.
  public static SortedMergeCpuMeter fromSystemProperty() {
    return new SortedMergeCpuMeter(Boolean.getBoolean(ENABLED_PROPERTY));
  }

  public boolean isEnabled() {
    return _enabled;
  }

  /// Returns a thread factory that registers every thread it creates with this meter. The executor under measurement
  /// must be built with this factory, or its threads contribute nothing and the `OFF` arm reads as single-threaded.
  ///
  /// <p>Threads are daemons so a leaked pool cannot keep a forked JMH JVM alive past the trial.
  public ThreadFactory threadFactory(String namePrefix) {
    return runnable -> {
      Thread thread = new Thread(runnable, namePrefix + "-" + _threadCounter.getAndIncrement());
      thread.setDaemon(true);
      if (_enabled) {
        _workers.add(new Worker(thread));
      }
      return thread;
    };
  }

  /// Opening snapshot. Reads the pool threads first and the calling thread last, so this call's own cost is billed
  /// before the interval opens. See the class javadoc.
  public long cpuNanosBeforeWork() {
    if (!_enabled) {
      return 0L;
    }
    claimReadingThread();
    long total = sumWorkerCpuNanos();
    return total + callerCpuNanos();
  }

  /// Closing snapshot. Reads the calling thread first and the pool threads last, so this call's own cost is billed
  /// after the interval closes. Subtract [#cpuNanosBeforeWork] from this to get the interval's CPU time.
  public long cpuNanosAfterWork() {
    if (!_enabled) {
      return 0L;
    }
    claimReadingThread();
    long total = callerCpuNanos();
    return total + sumWorkerCpuNanos();
  }

  private void claimReadingThread() {
    Thread current = Thread.currentThread();
    Thread claimed = _readingThread;
    if (claimed == null) {
      _readingThread = current;
    } else if (claimed != current) {
      throw new IllegalStateException(
          "SortedMergeCpuMeter was claimed by thread '" + claimed.getName() + "' but is being read by '"
              + current.getName() + "'. A delta taken across concurrent invocations attributes the same pool CPU to "
              + "both, so the reported figure would be wrong rather than merely noisy. Pin @Threads(1), or give each "
              + "benchmark thread its own meter and its own executor.");
    }
  }

  private static long callerCpuNanos() {
    return Math.max(0L, THREAD_MX_BEAN.getCurrentThreadCpuTime());
  }

  private long sumWorkerCpuNanos() {
    long total = 0L;
    for (Worker worker : _workers) {
      total += worker.cpuNanos();
    }
    return total;
  }

  /// Self-check for the aggregation, run directly rather than as a unit test because `pinot-perf` has no test source
  /// tree. Verifies that a disabled meter reads zero, that an enabled meter with no pool threads still bills the
  /// caller, that pool CPU is attributed, and that a busy pool reports more CPU than wall-clock time.
  ///
  /// <p>Run: `java -cp pinot-perf/target/classes:... org.apache.pinot.perf.sortedmerge.SortedMergeCpuMeter`
  public static void main(String[] args)
      throws Exception {
    SortedMergeCpuMeter disabled = new SortedMergeCpuMeter(false);
    check("disabled meter reads zero", disabled.cpuNanosAfterWork() - disabled.cpuNanosBeforeWork() == 0);

    SortedMergeCpuMeter callerOnly = new SortedMergeCpuMeter(true);
    long before = callerOnly.cpuNanosBeforeWork();
    burnCpu(200);
    long callerDelta = callerOnly.cpuNanosAfterWork() - before;
    check("caller CPU is counted with no pool threads, got " + callerDelta + "ns", callerDelta > 0);

    int numThreads = 4;
    long busyMillis = 250;
    SortedMergeCpuMeter pooled = new SortedMergeCpuMeter(true);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads, pooled.threadFactory("cpu-meter-selfcheck"));
    try {
      // Warm the pool so thread creation is outside the measured interval, matching how the benchmark uses it.
      runConcurrently(executor, numThreads, 1);
      long poolBefore = pooled.cpuNanosBeforeWork();
      long wallStartNanos = System.nanoTime();
      runConcurrently(executor, numThreads, busyMillis);
      long wallNanos = System.nanoTime() - wallStartNanos;
      long poolDelta = pooled.cpuNanosAfterWork() - poolBefore;
      check("pool CPU is counted, got " + poolDelta + "ns", poolDelta > 0);
      // N threads busy for the same wall-clock window must report more CPU than that window, which no single-threaded
      // accounting error could produce. The bound is deliberately loose (1.5x rather than near 4x) so the check does
      // not fail on a loaded host.
      check("pool CPU " + poolDelta + "ns exceeds wall " + wallNanos + "ns with " + numThreads + " busy threads",
          poolDelta > wallNanos * 3 / 2);
    } finally {
      executor.shutdownNow();
    }
    System.out.println("[arm4] SortedMergeCpuMeter self-check passed");
  }

  private static void runConcurrently(ExecutorService executor, int numThreads, long busyMillis)
      throws InterruptedException {
    CountDownLatch done = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executor.execute(() -> {
        try {
          burnCpu(busyMillis);
        } finally {
          done.countDown();
        }
      });
    }
    check("self-check tasks completed", done.await(60, TimeUnit.SECONDS));
  }

  /// Spins rather than sleeping: the point is to consume CPU time, which a sleeping thread does not.
  private static void burnCpu(long millis) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    long sink = 0;
    while (System.nanoTime() < deadline) {
      sink += System.nanoTime();
    }
    if (sink == Long.MIN_VALUE) {
      throw new IllegalStateException("unreachable, defeats dead-code elimination");
    }
  }

  private static void check(String what, boolean condition) {
    if (!condition) {
      throw new AssertionError("[arm4] SortedMergeCpuMeter self-check failed: " + what);
    }
  }
}
