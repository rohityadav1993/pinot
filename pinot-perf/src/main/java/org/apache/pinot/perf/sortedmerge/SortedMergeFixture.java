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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/// Segment fixture for the arm-4 memory-crossover benchmark: a table of physically sorted OFFLINE segments whose key
/// ranges overlap one another by a controlled depth.
///
/// <p>The hypothesis under test is that the streaming merge retains
/// {@code min(limit + offset, MAX_DOC_PER_CALL) * overlapDepth} rows, so overlap depth is the multiplier and therefore
/// a first-class parameter here rather than an incidental property of the data.
///
/// <p>The opposing arm is <b>not</b> a single {@code O(limit + offset)} accumulator, which is what an earlier version
/// of this comment claimed. {@code MinMaxValueBasedSelectionOrderByCombineOperator} builds one bounded top-K per
/// segment operator (`SelectionOrderByOperator`, capped at {@code offset + limit}) across {@code W} concurrent tasks
/// and queues finished blocks in an unbounded {@code LinkedBlockingQueue} for a single consumer, so it retains roughly
/// {@code W * min(limit + offset, rowsPerSegment)} plus the merged accumulator. Depth alone therefore does not
/// determine the winner -- it is depth against {@code W} -- which is why {@code maxExecutionThreads} is pinned
/// explicitly by {@link SortedMergeDriver} rather than left to resolve from the host's core count.
///
/// <p>Segments are cached at two levels. Within a JVM, segments are built once per (overlap, shape) key and cached
/// statically for the lifetime of the process: JMH re-runs {@code @Setup(Level.Trial)} for every parameter
/// combination, which without this cache would rebuild five million rows several dozen times and dominate the run.
/// Across JVMs -- notably the heap-bisection instrument, which forks a fresh JVM per attempt and so never benefits
/// from the in-JVM cache -- the built segments are additionally persisted on disk under a per-key directory and
/// reloaded with {@link ImmutableSegmentLoader} in {@link ReadMode#mmap} on the next invocation instead of being
/// regenerated. A directory is only trusted if it carries a `.arm4-complete` marker written after every segment in it
/// was built successfully, and matching the exact (overlap, segment count, rows per segment, total row count) being
/// requested; every expected segment is also re-loaded and its row count re-summed before the fixture is handed back.
/// Any mismatch -- missing marker, wrong segment count, a segment that fails to load, or a row-count mismatch -- is
/// treated as a corrupt or partial fixture: it is deleted in full and rebuilt from scratch, never partially repaired.
///
/// <p>Because the fixture is now expected to outlive a single run, on-disk cleanup is opt-in rather than automatic.
/// By default (`-Darm4.fixture.keep` unset, or set to anything other than `false`) the JVM shutdown hook destroys the
/// in-memory {@link IndexSegment}s (releasing their mmap handles) but leaves the on-disk files in place so a later
/// JVM can reuse them. Passing `-Darm4.fixture.keep=false` restores the old behavior of deleting the on-disk fixture
/// at shutdown. Setting `-Darm4.fixture.purge=true` forces every key to be rebuilt from scratch on this run,
/// regardless of what is already on disk, without requiring the caller to find and `rm -rf` the temp directory
/// themselves; [#purgeAll()] is the equivalent programmatic call. The full default fixture (3 overlap modes x 100
/// segments x 50,000 rows, one un-dictionary-encoded ~64-character payload column) is multi-gigabyte on disk, so a
/// long-lived host running many bisections should budget for that footprint under {@link FileUtils#getTempDirectory}.
///
/// <p>Thread-safe: the in-JVM cache is a {@link ConcurrentHashMap} and building is serialized per key. Cross-JVM
/// access is additionally serialized per key by a sibling `.lock` file next to each key's output directory (see
/// [#loadOrBuild]), so at most one process at a time is ever inside the validate-or-build critical section for a
/// given key. [#destroyAll()] and [#purgeAll()] honor the same per-key lock before deleting a key's directory (see
/// [#deleteAllKeysUnderLock]), so a `-Darm4.fixture.keep=false` teardown or a [#purgeAll()] call can never delete a
/// directory a concurrent [#loadOrBuild] is still writing into. The defensive validation in [#tryLoadExisting]
/// remains in place as well, since the lock only stops two processes from writing the same key concurrently, not a
/// fixture that was left corrupt by a process that crashed while holding the lock.
///
/// <p>`keyCardinality` (K) is a second axis: it controls how many consecutive rows within a segment share one
/// {@link #TS_COL} value (see [#records]), rather than how many segments a value spans. K=1 --
/// [#DEFAULT_KEY_CARDINALITY] -- reproduces the original unique-within-segment {@link #TS_COL} exactly, byte for
/// byte, which is why it is also the cardinality that keeps the cache key and completion marker unchanged in form;
/// see [#cacheKey(Overlap, int, int, int)] and [#tryLoadExisting].
///
/// <p><b>K is only approximately orthogonal to {@link Overlap}, and stops being so as K approaches the per-segment
/// stride.</b> Dividing by K compresses the key space, so segment ranges that were distinct can converge. With the
/// default 100 x 50,000 shape the measured depths are:
///
/// ```
/// K          DISJOINT   PARTIAL   FULL     distinct keys per segment
/// 1          1          10        100      50000
/// 100        1          10        100      500
/// 10000      1          11        100      5
/// 100000     2          29        100      1
/// ```
///
/// DISJOINT holds to depth 1 until K exceeds `rowsPerSegment`; PARTIAL, whose stride is only `rowsPerSegment / 10`,
/// starts drifting an order of magnitude earlier and is already 11 rather than 10 at K=10,000. FULL is unaffected at
/// any K because every segment shares one base. Nothing rejects a K that collapses the intended overlap, because
/// there is no single threshold that is wrong for every shape; instead depth is always *measured* rather than
/// assumed -- see [#measuredDepth] and guard G3 -- and both the build and reuse log lines print it. Report results
/// against the measured depth for the (overlap, K) pair actually run, never against the depth the {@link Overlap}
/// constant documents for K=1.
public final class SortedMergeFixture {
  private SortedMergeFixture() {
  }

  public static final String RAW_TABLE_NAME = "arm4Table";
  public static final String TS_COL = "tsCol";
  public static final String VAL_COL = "valCol";
  public static final String PAYLOAD_COL = "payloadCol";

  public static final int DEFAULT_NUM_SEGMENTS = 100;
  public static final int DEFAULT_ROWS_PER_SEGMENT = 50_000;

  /// System property controlling whether the on-disk fixture survives JVM shutdown. Anything other than `false`
  /// (including unset) keeps the files so a later JVM -- e.g. the next heap-bisection attempt -- can reuse them.
  public static final String KEEP_PROPERTY = "arm4.fixture.keep";

  /// System property that, when `true`, forces every key to be rebuilt from scratch on this run regardless of what
  /// is already on disk. Equivalent to calling [#purgeAll()] before the first [#segments] call.
  public static final String PURGE_PROPERTY = "arm4.fixture.purge";

  private static final String MARKER_FILE_NAME = ".arm4-complete";
  private static final String MARKER_KEY_OVERLAP = "overlap";
  private static final String MARKER_KEY_NUM_SEGMENTS = "numSegments";
  private static final String MARKER_KEY_ROWS_PER_SEGMENT = "rowsPerSegment";
  private static final String MARKER_KEY_TOTAL_ROWS = "totalRows";
  private static final String MARKER_KEY_KEY_CARDINALITY = "keyCardinality";

  /// The `keyCardinality` value every pre-existing on-disk fixture and every call site written before this
  /// parameter existed implicitly means: {@link #TS_COL} unique within a segment. A missing
  /// [#MARKER_KEY_KEY_CARDINALITY] property is read as this value (see [#tryLoadExisting]), and the cache key omits
  /// its suffix at this value (see [#cacheKey(Overlap, int, int, int)]) so the three large fixtures already on disk
  /// (`DISJOINT_100x50000`, `PARTIAL_100x50000`, `FULL_100x50000`) keep being reused rather than rebuilt.
  public static final int DEFAULT_KEY_CARDINALITY = 1;

  /// Payload filler sized so each projected row carries a ~64 character String. Row copies deep-copy onto the heap via
  /// `RowBasedBlockValueFetcher`, so a narrow all-INT schema would understate both arms and compress the difference
  /// between them. The column is stored raw (no dictionary) both because a five-million-entry dictionary would
  /// dominate segment build time and because a raw forward index is the realistic shape for a wide payload.
  private static final String PAYLOAD_FILLER = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

  public static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(TS_COL, FieldSpec.DataType.LONG)
      .addSingleValueDimension(VAL_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(PAYLOAD_COL, FieldSpec.DataType.STRING)
      .build();

  public static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSortedColumn(TS_COL)
          .setNoDictionaryColumns(List.of(PAYLOAD_COL)).build();

  /// How far the per-segment ranges of {@link #TS_COL} overlap one another, which is the multiplier in the retention
  /// model. Depth is the number of segments whose `[min, max]` range contains a given value.
  public enum Overlap {
    /// Segment `i` covers `[i*R, (i+1)*R)`. Depth 1: the merge only ever has one live cursor with candidate rows.
    DISJOINT,
    /// Segments start every `R/10` rows, so any value is straddled by about ten segments. Depth ~10.
    PARTIAL,
    /// Every segment covers the same range. Depth equals the segment count.
    FULL
  }

  /// System property pointing the on-disk fixture at a different directory. For tests only -- see [#baseDir()].
  @VisibleForTesting
  static final String BASE_DIR_PROPERTY = "arm4.fixture.dir";

  private static final Map<String, List<IndexSegment>> SEGMENT_CACHE = new ConcurrentHashMap<>();
  private static final Map<String, Integer> DEPTH_CACHE = new ConcurrentHashMap<>();

  static {
    // Registered here rather than in a main() so that it also fires in the JVM JMH forks, which is where the fixture
    // is actually built. It always destroys the in-memory segments (releasing mmap handles); whether it also deletes
    // the on-disk files is controlled by KEEP_PROPERTY -- see the class javadoc.
    Runtime.getRuntime().addShutdownHook(new Thread(SortedMergeFixture::destroyAll, "arm4-fixture-cleanup"));
  }

  @VisibleForTesting
  static File baseDir() {
    // Overridable only so a test can point the fixture somewhere disposable. Without this, any test touching
    // purgeAll() or destroyAll() would delete the real multi-gigabyte fixtures that published results were measured
    // against, since both delete every key under this directory. Unset in every non-test path, so the default -- and
    // therefore the on-disk cache every previous run built -- is unchanged.
    String override = System.getProperty(BASE_DIR_PROPERTY);
    return override != null ? new File(override) : new File(FileUtils.getTempDirectory(), "pinot-arm4-sorted-merge");
  }

  /// As [#segments(Overlap, int, int, int)] with [#DEFAULT_KEY_CARDINALITY], for callers written before the
  /// cardinality parameter existed.
  public static List<IndexSegment> segments(Overlap overlap, int numSegments, int rowsPerSegment) {
    return segments(overlap, numSegments, rowsPerSegment, DEFAULT_KEY_CARDINALITY);
  }

  /// Returns the cached segments for `overlap`, building them on first use. The returned list is shared and must not
  /// be destroyed by callers; use [#destroyAll()] at JVM teardown instead.
  ///
  /// <p>`keyCardinality` (K) controls how many consecutive rows share a {@link #TS_COL} value -- see [#records] --
  /// which is what makes the tail-to-sort path's per-run heap hold more than one row. K=1, the default, reproduces
  /// today's unique-within-segment {@link #TS_COL}.
  public static List<IndexSegment> segments(Overlap overlap, int numSegments, int rowsPerSegment,
      int keyCardinality) {
    String key = cacheKey(overlap, numSegments, rowsPerSegment, keyCardinality);
    return SEGMENT_CACHE.computeIfAbsent(key,
        k -> loadOrBuild(overlap, numSegments, rowsPerSegment, keyCardinality, k));
  }

  /// As [#measuredDepth(Overlap, int, int, int)] with [#DEFAULT_KEY_CARDINALITY].
  public static int measuredDepth(Overlap overlap, int numSegments, int rowsPerSegment) {
    return measuredDepth(overlap, numSegments, rowsPerSegment, DEFAULT_KEY_CARDINALITY);
  }

  /// Returns the measured overlap depth of the fixture: the number of segments whose {@link #TS_COL} range contains
  /// the midpoint of the global range. Guard G3 -- the hypothesis is parameterised on this number, so it is measured
  /// from segment metadata rather than assumed from the generator.
  public static int measuredDepth(Overlap overlap, int numSegments, int rowsPerSegment, int keyCardinality) {
    segments(overlap, numSegments, rowsPerSegment, keyCardinality);
    Integer depth = DEPTH_CACHE.get(cacheKey(overlap, numSegments, rowsPerSegment, keyCardinality));
    if (depth == null) {
      throw new IllegalStateException("No measured depth recorded for " + overlap);
    }
    return depth;
  }

  /// The on-disk/in-JVM cache key. Deliberately unchanged in form at [#DEFAULT_KEY_CARDINALITY] -- the three large
  /// fixtures already on disk (`DISJOINT_100x50000`, `PARTIAL_100x50000`, `FULL_100x50000`) were built under this
  /// exact key with no cardinality suffix, and at K=1 the rows generated today are byte-identical to what
  /// [#records] always produced, so those directories must keep validating. Any other `keyCardinality` gets a
  /// `_k<N>` suffix so a fixture built at one cardinality can never be mistaken for one built at another.
  private static String cacheKey(Overlap overlap, int numSegments, int rowsPerSegment, int keyCardinality) {
    String base = overlap + "_" + numSegments + "x" + rowsPerSegment;
    return keyCardinality == DEFAULT_KEY_CARDINALITY ? base : base + "_k" + keyCardinality;
  }

  /// Reuses a validated on-disk fixture if one exists for `key`, otherwise builds it fresh. This is the cross-JVM
  /// half of the cache described in the class javadoc; see [#tryLoadExisting] for the validation rules.
  ///
  /// <p>The whole validate-or-build sequence runs under an exclusive lock on a sibling `<key>.lock` file, acquired
  /// before the `outDir.isDirectory()` check and held through the return of [#build]. The lock file cannot live
  /// inside `outDir` because both [#build] and every failure branch of [#tryLoadExisting] delete `outDir` wholesale;
  /// a lock file inside it would either vanish out from under its own holder or be recreated as a distinct inode that
  /// a waiting process is not holding. Ordering the acquire before the directory check is what makes waiting
  /// productive rather than redundant: a process that blocks here is unblocked only after some other process's build
  /// (or purge) has finished and released the lock, so it re-enters the check and validation fresh and takes the
  /// reuse path instead of racing to rebuild what was just built. [#PURGE_PROPERTY] is handled inside the lock for
  /// the same reason -- otherwise a purge could race a concurrent build on the same key.
  private static List<IndexSegment> loadOrBuild(Overlap overlap, int numSegments, int rowsPerSegment,
      int keyCardinality, String key) {
    if (numSegments < 1 || rowsPerSegment < 10) {
      throw new IllegalArgumentException("Need at least one segment of at least ten rows, got " + key);
    }
    if (keyCardinality < 1) {
      // 0 divides by zero deep inside records() with an opaque ArithmeticException, and a negative value would
      // silently corrupt tsCol's ascending order within a segment. Reject both loudly, at the same place the other
      // shape parameters are validated, rather than let either surface as a confusing failure three calls down.
      throw new IllegalArgumentException("keyCardinality must be >= 1, got " + keyCardinality + " for " + key);
    }
    File outDir = new File(baseDir(), key);
    // FileLock is scoped to the JVM (not the thread), so a second lock() on the same file from this JVM would throw
    // OverlappingFileLockException instead of blocking. That can't happen here: within a JVM, SEGMENT_CACHE's
    // computeIfAbsent already serializes every call for this exact key before this method is ever entered, and a
    // different key maps to a different lock file, so no two invocations of this method in the same JVM ever open
    // the same lock file concurrently.
    try (FileChannel lockChannel = FileChannel.open(lockFile(key).toPath(), StandardOpenOption.CREATE,
        StandardOpenOption.WRITE);
        FileLock lock = lockChannel.lock()) {
      if (Boolean.getBoolean(PURGE_PROPERTY)) {
        System.out.printf("[arm4] %s: purging %s before build because -D%s=true%n", key, outDir, PURGE_PROPERTY);
        FileUtils.deleteQuietly(outDir);
      } else if (outDir.isDirectory()) {
        long startNs = System.nanoTime();
        List<IndexSegment> reused =
            tryLoadExisting(outDir, overlap, numSegments, rowsPerSegment, keyCardinality, key);
        if (reused != null) {
          int depth = measureDepth(reused);
          DEPTH_CACHE.put(key, depth);
          System.out.printf("[arm4] reused %s in %.1fs, measured overlap depth %d%n", key,
              (System.nanoTime() - startNs) / 1e9, depth);
          return reused;
        }
      }
      return build(overlap, numSegments, rowsPerSegment, keyCardinality, key, outDir);
    } catch (OverlappingFileLockException e) {
      // The mirror of the case deleteAllKeysUnderLock() handles: a purgeAll() or shutdown-hook teardown on another
      // thread of this JVM already holds this key's lock. Blocking is impossible (FileLock is JVM-scoped), and
      // building into a directory that is being deleted is exactly the corruption this lock exists to prevent, so
      // fail with a message that names the cause instead of the bare IllegalStateException the JDK throws.
      throw new IllegalStateException(
          "Cannot build fixture " + key + ": another thread in this JVM is purging or destroying the fixture "
              + "directory. Do not call purgeAll() concurrently with a fixture build.", e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to acquire cross-process lock for " + key, e);
    }
  }

  /// The cross-process lock file for `key`: a sibling of `outDir`, never inside it, and never deleted on release
  /// (see class javadoc and [#loadOrBuild]). `baseDir()` must exist before this is opened, since `FileChannel.open`
  /// does not create parent directories.
  private static File lockFile(String key) {
    File base = baseDir();
    if (!base.isDirectory() && !base.mkdirs() && !base.isDirectory()) {
      throw new RuntimeException("Failed to create fixture base directory " + base);
    }
    return new File(base, key + ".lock");
  }

  /// Attempts to load a pre-existing on-disk fixture for `key`. Returns `null` -- after loudly logging why, and
  /// deleting `outDir` -- if the marker is missing or does not match the requested parameters, if any expected
  /// segment directory fails to load, or if the total row count does not match what was asked for. A partial or
  /// mismatched fixture is never partially repaired: it is wiped and left for [#build] to regenerate.
  private static List<IndexSegment> tryLoadExisting(File outDir, Overlap overlap, int numSegments,
      int rowsPerSegment, int keyCardinality, String key) {
    long expectedTotalRows = (long) numSegments * rowsPerSegment;
    File markerFile = new File(outDir, MARKER_FILE_NAME);
    if (!markerFile.isFile()) {
      System.out.printf("[arm4] %s: found %s without a completion marker (%s); treating as incomplete and "
          + "rebuilding%n", key, outDir, markerFile.getName());
      FileUtils.deleteQuietly(outDir);
      return null;
    }

    Properties marker = new Properties();
    try (FileInputStream in = new FileInputStream(markerFile)) {
      marker.load(in);
    } catch (Exception e) {
      System.out.printf("[arm4] %s: failed to read completion marker at %s (%s); rebuilding%n", key, markerFile,
          e.getMessage());
      FileUtils.deleteQuietly(outDir);
      return null;
    }

    // A missing keyCardinality property means the marker predates this parameter, which is exactly the case where
    // it must be interpreted as DEFAULT_KEY_CARDINALITY -- otherwise every marker written before today would fail
    // validation and the three large existing fixtures would be silently rebuilt.
    String actualKeyCardinality = marker.getProperty(MARKER_KEY_KEY_CARDINALITY, String.valueOf(
        DEFAULT_KEY_CARDINALITY));
    String expectedMarker =
        overlap + "|" + numSegments + "|" + rowsPerSegment + "|" + expectedTotalRows + "|" + keyCardinality;
    String actualMarker = marker.getProperty(MARKER_KEY_OVERLAP) + "|" + marker.getProperty(MARKER_KEY_NUM_SEGMENTS)
        + "|" + marker.getProperty(MARKER_KEY_ROWS_PER_SEGMENT) + "|" + marker.getProperty(MARKER_KEY_TOTAL_ROWS)
        + "|" + actualKeyCardinality;
    if (!expectedMarker.equals(actualMarker)) {
      System.out.printf("[arm4] %s: completion marker at %s does not match requested params (expected %s, found "
          + "%s); rebuilding%n", key, markerFile, expectedMarker, actualMarker);
      FileUtils.deleteQuietly(outDir);
      return null;
    }

    List<IndexSegment> segments = new ArrayList<>(numSegments);
    try {
      for (int i = 0; i < numSegments; i++) {
        String segmentName = key + "_seg_" + i;
        File segmentDir = new File(outDir, segmentName);
        if (!segmentDir.isDirectory()) {
          throw new IllegalStateException("Missing expected segment directory " + segmentDir);
        }
        segments.add(ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap));
      }
    } catch (Exception e) {
      System.out.printf("[arm4] %s: on-disk fixture at %s failed to load (%s); deleting and rebuilding%n", key,
          outDir, e.getMessage());
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
      FileUtils.deleteQuietly(outDir);
      return null;
    }

    long actualTotalRows = 0;
    for (IndexSegment segment : segments) {
      actualTotalRows += segment.getSegmentMetadata().getTotalDocs();
    }
    if (actualTotalRows != expectedTotalRows) {
      System.out.printf("[arm4] %s: on-disk fixture at %s has %d total rows, expected %d; deleting and rebuilding%n",
          key, outDir, actualTotalRows, expectedTotalRows);
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
      FileUtils.deleteQuietly(outDir);
      return null;
    }

    return segments;
  }

  private static List<IndexSegment> build(Overlap overlap, int numSegments, int rowsPerSegment, int keyCardinality,
      String key, File outDir) {
    FileUtils.deleteQuietly(outDir);
    List<IndexSegment> segments = new ArrayList<>(numSegments);
    long startNs = System.nanoTime();
    try {
      for (int i = 0; i < numSegments; i++) {
        segments.add(
            buildSegment(outDir, key + "_seg_" + i, records(overlap, i, rowsPerSegment, keyCardinality)));
      }
    } catch (Exception e) {
      // Release the segments already loaded in this attempt before giving up. Each one holds an mmap handle on a
      // multi-gigabyte fixture, and the bisection instrument forks a fresh JVM per attempt, so leaking them on a
      // failed build leaks per attempt rather than once. Mirrors the cleanup tryLoadExisting() already does on each
      // of its failure branches.
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
      throw new RuntimeException("Failed to build the " + overlap + " fixture", e);
    }
    int depth = measureDepth(segments);
    DEPTH_CACHE.put(key, depth);
    writeCompletionMarker(outDir, overlap, numSegments, rowsPerSegment, keyCardinality);
    System.out.printf("[arm4] built %s in %.1fs, measured overlap depth %d%n", key, (System.nanoTime() - startNs) / 1e9,
        depth);
    return segments;
  }

  /// Writes the `.arm4-complete` marker only after every segment has been built successfully, so a build that is
  /// interrupted mid-way (crash, kill, or a concurrent build racing on the same directory) never leaves behind a
  /// directory that [#tryLoadExisting] would trust.
  private static void writeCompletionMarker(File outDir, Overlap overlap, int numSegments, int rowsPerSegment,
      int keyCardinality) {
    Properties marker = new Properties();
    marker.setProperty(MARKER_KEY_OVERLAP, overlap.toString());
    marker.setProperty(MARKER_KEY_NUM_SEGMENTS, String.valueOf(numSegments));
    marker.setProperty(MARKER_KEY_ROWS_PER_SEGMENT, String.valueOf(rowsPerSegment));
    marker.setProperty(MARKER_KEY_TOTAL_ROWS, String.valueOf((long) numSegments * rowsPerSegment));
    marker.setProperty(MARKER_KEY_KEY_CARDINALITY, String.valueOf(keyCardinality));
    File markerFile = new File(outDir, MARKER_FILE_NAME);
    try (FileOutputStream out = new FileOutputStream(markerFile)) {
      marker.store(out, "arm4 sorted-merge fixture completion marker; do not edit by hand");
    } catch (Exception e) {
      throw new RuntimeException("Failed to write completion marker " + markerFile, e);
    }
  }

  /// Generates one segment's rows. {@link #TS_COL} is ascending within every segment for all three overlap modes, so
  /// every segment is physically sorted and the streaming merge's per-segment forward scan is available; only the
  /// starting offset differs. {@link #VAL_COL} is globally unique so that the order-by `(tsCol, valCol)` is a total
  /// order and the top-K boundary is unambiguous -- without that, two correct operators may legitimately disagree
  /// about which of several fully tied rows fall inside the limit. That total order is only exercised by
  /// {@link SortedMergeDriver.OrderBy#TS_VAL}, so `valCol` stays globally unique regardless of `keyCardinality`.
  ///
  /// <p>`keyCardinality` (K) controls how many consecutive rows share a {@link #TS_COL} value: `tsCol = (base + j) /
  /// K`, so each distinct key value covers a run of K consecutive rows. At K=1 -- [#DEFAULT_KEY_CARDINALITY] -- this
  /// is `base + j`, i.e. exactly what this method always produced before K existed, which is why the on-disk cache
  /// key does not change at that value (see [#cacheKey(Overlap, int, int, int)]). K>1 exists so that a primary-value
  /// run can be longer than one row: at K=1 every run is exactly one row, so {@link SortedMergeFixture.Overlap} depth
  /// aside, the tail-to-sort path's per-run heap structurally never holds more than one row, which makes the `ON`
  /// arm's worst-case retention unreachable regardless of how the fixture is otherwise shaped.
  private static List<GenericRow> records(Overlap overlap, int segmentIndex, int rowsPerSegment,
      int keyCardinality) {
    long base;
    switch (overlap) {
      case DISJOINT:
        base = (long) segmentIndex * rowsPerSegment;
        break;
      case PARTIAL:
        base = (long) segmentIndex * (rowsPerSegment / 10);
        break;
      case FULL:
        base = 0L;
        break;
      default:
        throw new IllegalArgumentException("Unknown overlap: " + overlap);
    }
    List<GenericRow> records = new ArrayList<>(rowsPerSegment);
    for (int j = 0; j < rowsPerSegment; j++) {
      GenericRow record = new GenericRow();
      record.putValue(TS_COL, (base + j) / keyCardinality);
      record.putValue(VAL_COL, segmentIndex * rowsPerSegment + j);
      record.putValue(PAYLOAD_COL, PAYLOAD_FILLER + "_" + segmentIndex + "_" + j);
      records.add(record);
    }
    return records;
  }

  private static IndexSegment buildSegment(File outDir, String segmentName, List<GenericRow> records)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(outDir.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(outDir, segmentName), ReadMode.mmap);
  }

  /// Guard G3. Counts the segments whose {@link #TS_COL} `[min, max]` range contains the midpoint of the global range.
  private static int measureDepth(List<IndexSegment> segments) {
    long globalMin = Long.MAX_VALUE;
    long globalMax = Long.MIN_VALUE;
    List<long[]> ranges = new ArrayList<>(segments.size());
    for (IndexSegment segment : segments) {
      ColumnMetadata metadata = segment.getSegmentMetadata().getColumnMetadataFor(TS_COL);
      long min = ((Number) metadata.getMinValue()).longValue();
      long max = ((Number) metadata.getMaxValue()).longValue();
      ranges.add(new long[]{min, max});
      globalMin = Math.min(globalMin, min);
      globalMax = Math.max(globalMax, max);
    }
    long midpoint = globalMin + (globalMax - globalMin) / 2;
    int depth = 0;
    for (long[] range : ranges) {
      if (range[0] <= midpoint && midpoint <= range[1]) {
        depth++;
      }
    }
    return depth;
  }

  /// Destroys every in-JVM-cached segment (releasing their mmap handles) and, only if `-Darm4.fixture.keep=false`,
  /// also deletes the on-disk fixture. Call once per JVM, at teardown. The default (property unset or not `false`)
  /// leaves the on-disk files in place so the next JVM -- e.g. the next heap-bisection attempt -- can reuse them; see
  /// the class javadoc and [#purgeAll()].
  public static void destroyAll() {
    for (List<IndexSegment> segments : SEGMENT_CACHE.values()) {
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
    }
    SEGMENT_CACHE.clear();
    DEPTH_CACHE.clear();
    boolean keep = !"false".equalsIgnoreCase(System.getProperty(KEEP_PROPERTY));
    if (!keep) {
      System.out.printf("[arm4] deleting on-disk fixture at %s because -D%s=false%n", baseDir(), KEEP_PROPERTY);
      deleteAllKeysUnderLock();
    }
  }

  /// Explicitly purges the on-disk fixture: destroys every in-JVM-cached segment and unconditionally deletes the
  /// base directory, regardless of {@link #KEEP_PROPERTY}. This is the supported route for a guaranteed-clean run
  /// instead of a caller having to find and `rm -rf` the temp directory themselves.
  public static void purgeAll() {
    for (List<IndexSegment> segments : SEGMENT_CACHE.values()) {
      for (IndexSegment segment : segments) {
        segment.destroy();
      }
    }
    SEGMENT_CACHE.clear();
    DEPTH_CACHE.clear();
    System.out.printf("[arm4] purging on-disk fixture at %s%n", baseDir());
    deleteAllKeysUnderLock();
  }

  /// Deletes every key's on-disk directory under {@link #baseDir()}, but never a `.lock` file, and never a key's
  /// directory without first holding that key's lock. Used by [#destroyAll()] and [#purgeAll()] instead of a bare
  /// `FileUtils.deleteQuietly(baseDir())`, which -- now that [#loadOrBuild] serializes on a per-key lock -- would
  /// otherwise let one process wipe a directory that another process's [#loadOrBuild] is actively validating or
  /// building, exactly the cross-process race this class's locking exists to prevent. Lock files themselves are
  /// never deleted, for the same orphaned-inode reason [#loadOrBuild] never deletes them on release.
  private static void deleteAllKeysUnderLock() {
    File base = baseDir();
    File[] entries = base.listFiles();
    if (entries == null) {
      return;
    }
    for (File entry : entries) {
      if (entry.getName().endsWith(".lock")) {
        continue;
      }
      try (FileChannel lockChannel = FileChannel.open(lockFile(entry.getName()).toPath(), StandardOpenOption.CREATE,
          StandardOpenOption.WRITE);
          FileLock lock = lockChannel.lock()) {
        FileUtils.deleteQuietly(entry);
      } catch (OverlappingFileLockException e) {
        // Another thread in THIS JVM holds the key's lock, which means loadOrBuild is mid-validation or mid-build on
        // it. FileLock is JVM-scoped, so that collision throws here rather than blocking, and it is not an IOException
        // so it would otherwise escape uncaught -- out of a shutdown hook, aborting the loop and leaving the
        // remaining keys unexamined. Skipping is also the correct outcome on its merits: a directory being built is
        // exactly what must not be deleted.
        System.out.printf("[arm4] not deleting %s: another thread in this JVM is building it%n", entry);
      } catch (IOException e) {
        throw new RuntimeException("Failed to acquire lock while deleting fixture entry " + entry, e);
      }
    }
  }
}
