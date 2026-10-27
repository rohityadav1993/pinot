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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.perf.sortedmerge.SortedMergeFixture.ConsumingShape;
import org.apache.pinot.perf.sortedmerge.SortedMergeFixture.Overlap;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests the teardown half of the fixture's cross-process locking.
///
/// <p>The build half -- two processes racing [SortedMergeFixture#segments] for one key -- needs two JVMs and so is
/// not reachable from a unit test. What *is* reachable, and is the half that actually regressed, is the collision
/// between a teardown and a build inside one JVM: `FileLock` is scoped to the JVM rather than the thread, so a
/// second acquisition of the same lock from the same JVM throws {@link java.nio.channels.OverlappingFileLockException}
/// instead of blocking. That exception extends `IllegalStateException`, not `IOException`, so before the handling
/// these tests pin it escaped uncaught -- out of a shutdown hook, abandoning the rest of the cleanup loop.
///
/// <p>Every test here points [SortedMergeFixture#baseDir()] at a temporary directory first, and asserts the override
/// took effect before anything destructive runs. Without that, calling [SortedMergeFixture#purgeAll()] would delete
/// the real multi-gigabyte fixtures that published benchmark results were measured against.
public class SortedMergeFixtureTest {
  private static final String LOCKED_KEY = "LOCKED_1x10";
  private static final String UNLOCKED_KEY = "UNLOCKED_1x10";

  private File _baseDir;
  private String _previousOverride;

  @BeforeMethod
  public void pointFixtureAtADisposableDirectory()
      throws IOException {
    _baseDir = Files.createTempDirectory("arm4-fixture-test").toFile();
    _previousOverride = System.setProperty(SortedMergeFixture.BASE_DIR_PROPERTY, _baseDir.getAbsolutePath());
    // The guard that makes the destructive tests below safe to run at all.
    assertEquals(SortedMergeFixture.baseDir().getAbsolutePath(), _baseDir.getAbsolutePath(),
        "the base directory override must be in effect before any test deletes anything");
  }

  @AfterMethod
  public void restoreTheRealFixtureDirectory() {
    if (_previousOverride == null) {
      System.clearProperty(SortedMergeFixture.BASE_DIR_PROPERTY);
    } else {
      System.setProperty(SortedMergeFixture.BASE_DIR_PROPERTY, _previousOverride);
    }
    FileUtils.deleteQuietly(_baseDir);
  }

  @Test
  public void purgeSkipsAKeyWhoseLockIsHeldAndStillClearsTheRest()
      throws IOException {
    File lockedDir = keyDirectory(LOCKED_KEY);
    File unlockedDir = keyDirectory(UNLOCKED_KEY);

    // Holding the lock stands in for a build in flight: the collision is JVM-scoped, not thread-scoped, so taking it
    // on this thread reproduces exactly what a concurrent builder on another thread would cause.
    try (FileChannel channel = FileChannel.open(lockFileFor(LOCKED_KEY).toPath(), StandardOpenOption.CREATE,
        StandardOpenOption.WRITE);
        FileLock lock = channel.lock()) {
      SortedMergeFixture.purgeAll();
    }

    assertTrue(lockedDir.isDirectory(),
        "a key whose lock is held is being built, and deleting it is the corruption the lock exists to prevent");
    assertFalse(unlockedDir.exists(),
        "the collision must not abort the loop: keys after the locked one still have to be deleted");
  }

  @Test
  public void purgeNeverDeletesLockFiles()
      throws IOException {
    keyDirectory(UNLOCKED_KEY);
    File lockFile = lockFileFor(UNLOCKED_KEY);
    assertTrue(lockFile.createNewFile());

    SortedMergeFixture.purgeAll();

    // Unlinking a lock file lets one process keep a handle on the orphaned inode while another creates a fresh file,
    // so the two would lock different objects and the exclusion would silently stop working.
    assertTrue(lockFile.isFile(), "lock files must outlive the directories they guard");
  }

  @Test
  public void purgeDeletesEveryUnlockedKey()
      throws IOException {
    File first = keyDirectory("FIRST_1x10");
    File second = keyDirectory("SECOND_1x10");

    SortedMergeFixture.purgeAll();

    assertFalse(first.exists());
    assertFalse(second.exists());
  }

  @Test
  public void consumingSegmentIsAppendedLastAndKeepsValColUniqueAcrossTheMix() {
    // A deliberately different row count (40, against 100 offline) is the point: valCol must stay unique because the
    // consuming segment numbers from numSegments * rowsPerSegment, not from anything derived from its own size.
    int numSegments = 3;
    int rowsPerSegment = 100;
    int consumingRows = 40;
    try {
      List<IndexSegment> segments =
          SortedMergeFixture.segments(Overlap.PARTIAL, numSegments, rowsPerSegment, 1, consumingRows);

      assertEquals(segments.size(), numSegments + 1);
      IndexSegment consuming = segments.get(numSegments);
      assertTrue(consuming instanceof MutableSegmentImpl, "the consuming segment must be mutable, and last");
      assertEquals(consuming.getSegmentMetadata().getTotalDocs(), consumingRows);

      long previousMax = Long.MIN_VALUE;
      for (IndexSegment segment : segments) {
        DataSourceMetadata valCol = segment.getDataSource(SortedMergeFixture.VAL_COL).getDataSourceMetadata();
        long min = ((Number) valCol.getMinValue()).longValue();
        long max = ((Number) valCol.getMaxValue()).longValue();
        assertTrue(min > previousMax, "valCol ranges must be disjoint, but " + min + " <= " + previousMax);
        previousMax = max;
      }
      // PARTIAL ranges here are [0,99], [10,109], [20,119] and, for the consuming segment at index 3, [30,69]: all
      // four contain the midpoint 59. The mutable segment is measured too, which is what used to throw.
      assertEquals(SortedMergeFixture.measuredDepth(Overlap.PARTIAL, numSegments, rowsPerSegment, 1, consumingRows),
          numSegments + 1);
      assertEquals(SortedMergeFixture.measuredDepth(Overlap.PARTIAL, numSegments, rowsPerSegment, 1, 0), numSegments);
    } finally {
      SortedMergeFixture.purgeAll();
    }
  }

  @Test
  public void inDomainConsumingKeysStayInsideTheOfflineKeyRangeAndCountTowardDepth() {
    // 40 consuming rows over the offline domain [0, 99]: tsSource = j * 100 / 40, so the keys run 0..97. TAIL, by
    // contrast, would put them at 300..339, beyond every offline key.
    try {
      List<IndexSegment> segments =
          SortedMergeFixture.segments(Overlap.FULL, 3, 100, 1, 40, ConsumingShape.IN_DOMAIN);
      DataSourceMetadata ts = segments.get(3).getDataSource(SortedMergeFixture.TS_COL).getDataSourceMetadata();
      assertEquals(((Number) ts.getMinValue()).longValue(), 0L);
      assertEquals(((Number) ts.getMaxValue()).longValue(), 97L);
      DataSourceMetadata val = segments.get(3).getDataSource(SortedMergeFixture.VAL_COL).getDataSourceMetadata();
      assertEquals(((Number) val.getMinValue()).longValue(), 300L, "valCol must still start above the offline range");
      assertEquals(
          SortedMergeFixture.measuredDepth(Overlap.FULL, 3, 100, 1, 40, ConsumingShape.IN_DOMAIN), 4);
      // The default shape is a different cache entry and still trails the offline keys.
      List<IndexSegment> tail = SortedMergeFixture.segments(Overlap.FULL, 3, 100, 1, 40);
      DataSourceMetadata tailTs = tail.get(3).getDataSource(SortedMergeFixture.TS_COL).getDataSourceMetadata();
      assertEquals(((Number) tailTs.getMaxValue()).longValue(), 39L);
    } finally {
      SortedMergeFixture.purgeAll();
    }
  }

  @Test
  public void inDomainIsRejectedForOverlapsWithoutASingleKeyDomain() {
    try {
      assertThrows(IllegalArgumentException.class,
          () -> SortedMergeFixture.segments(Overlap.PARTIAL, 3, 100, 1, 40, ConsumingShape.IN_DOMAIN));
      assertThrows(IllegalArgumentException.class,
          () -> SortedMergeFixture.segments(Overlap.DISJOINT, 3, 100, 1, 40, ConsumingShape.IN_DOMAIN));
    } finally {
      SortedMergeFixture.purgeAll();
    }
  }

  @Test
  public void sweepDeletesDeadOwnersDirectoriesAndNeverLiveOnes()
      throws IOException {
    // A directory whose owning pid no longer exists is what an OOM-killed bisection attempt leaves behind. One owned by
    // this very (live) process stands in for a concurrent JVM's segment, which must survive the sweep.
    Path parent = Files.createTempDirectory("arm4-sweep-test-");
    try {
      Path dead = Files.createDirectory(parent.resolve("999999999-x"));
      Files.writeString(dead.resolve("f"), "x");
      Path live = Files.createDirectory(parent.resolve(ProcessHandle.current().pid() + "-x"));
      SortedMergeFixture.sweepAbandonedConsumingDirs(parent);
      assertFalse(Files.exists(dead), "a directory whose owner is gone must be swept");
      assertTrue(Files.isDirectory(live), "a live owner's directory must never be swept");
    } finally {
      FileUtils.deleteQuietly(parent.toFile());
    }
  }

  @Test
  public void sweepNeverFollowsASymlinkOutOfTheParent()
      throws IOException {
    Path parent = Files.createTempDirectory("arm4-sweep-test-");
    Path victim = Files.createTempDirectory("arm4-sweep-victim-");
    try {
      Path precious = Files.writeString(victim.resolve("precious"), "keep me");
      Path link = parent.resolve("999999999-link");
      try {
        Files.createSymbolicLink(link, victim);
      } catch (UnsupportedOperationException | IOException e) {
        throw new SkipException("symlinks are not supported here: " + e);
      }
      SortedMergeFixture.sweepAbandonedConsumingDirs(parent);
      assertTrue(Files.isSymbolicLink(link), "a symlinked entry must be skipped, not deleted");
      assertTrue(Files.exists(precious), "the target's contents must survive the sweep");
    } finally {
      // Deleted without following the link, so the victim is cleaned up explicitly below.
      SortedMergeFixture.deleteTreeNoFollow(parent);
      FileUtils.deleteQuietly(victim.toFile());
    }
  }

  @Test
  public void deleteTreeRemovesALinkInsideATreeAsALinkAndLeavesItsTarget()
      throws IOException {
    Path tree = Files.createTempDirectory("arm4-delete-test-");
    Path victim = Files.createTempDirectory("arm4-delete-victim-");
    try {
      Path precious = Files.writeString(victim.resolve("precious"), "keep me");
      Files.writeString(tree.resolve("own"), "x");
      try {
        Files.createSymbolicLink(tree.resolve("link"), victim);
      } catch (UnsupportedOperationException | IOException e) {
        throw new SkipException("symlinks are not supported here: " + e);
      }
      SortedMergeFixture.deleteTreeNoFollow(tree);
      assertFalse(Files.exists(tree, LinkOption.NOFOLLOW_LINKS), "the tree itself must be gone");
      assertTrue(Files.exists(precious), "a link in the tree must be deleted as a link");
    } finally {
      SortedMergeFixture.deleteTreeNoFollow(tree);
      FileUtils.deleteQuietly(victim.toFile());
    }
  }

  @Test
  public void privateDirIsCreatedOwnerOnlyAndAnUnsafeExistingOneIsRefused()
      throws IOException {
    Path root = Files.createTempDirectory("arm4-private-test-");
    try {
      Path dir = SortedMergeFixture.ensurePrivateDir(root, "p");
      assertEquals(SortedMergeFixture.ensurePrivateDir(root, "p"), dir, "an existing safe directory is reused");
      if (!root.getFileSystem().supportedFileAttributeViews().contains("posix")) {
        throw new SkipException("not a POSIX filesystem");
      }
      assertEquals(PosixFilePermissions.toString(Files.getPosixFilePermissions(dir)), "rwx------");
      Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwx---"));
      assertThrows(IOException.class, () -> SortedMergeFixture.ensurePrivateDir(root, "p"));
      Path target = Files.createDirectory(root.resolve("target"));
      try {
        Files.createSymbolicLink(root.resolve("l"), target);
      } catch (UnsupportedOperationException | IOException e) {
        throw new SkipException("symlinks are not supported here: " + e);
      }
      assertThrows(IOException.class, () -> SortedMergeFixture.ensurePrivateDir(root, "l"));
    } finally {
      SortedMergeFixture.deleteTreeNoFollow(root);
    }
  }

  @Test
  public void valColIsAPermutationWithinEachSegmentAndNeverMonotone() {
    for (int n : new int[]{10, 40, 50, 100, 50_000, 1_000_000}) {
      boolean[] seen = new boolean[n];
      boolean monotone = true;
      long previous = -1;
      for (int j = 0; j < n; j++) {
        long offset = SortedMergeFixture.valOffset(j, n);
        assertTrue(offset >= 0 && offset < n, "offset in range for n=" + n);
        assertFalse(seen[(int) offset], "offset repeated for n=" + n);
        seen[(int) offset] = true;
        monotone &= offset > previous;
        previous = offset;
      }
      assertFalse(monotone, "valCol offsets must not ascend for n=" + n);
    }
    assertThrows(IllegalArgumentException.class, () -> SortedMergeFixture.valOffset(0, 2 * 7919));
  }

  @Test
  public void offlineAndConsumingValColAreUnsortedAndGloballyUnique() {
    try {
      List<IndexSegment> segments = SortedMergeFixture.segments(Overlap.FULL, 3, 100, 1, 40);
      assertEquals(segments.size(), 4);
      long expectedMin = 0;
      for (int i = 0; i < segments.size(); i++) {
        IndexSegment segment = segments.get(i);
        DataSourceMetadata metadata = segment.getDataSource(SortedMergeFixture.VAL_COL).getDataSourceMetadata();
        boolean consuming = i == 3;
        int rows = consuming ? 40 : 100;
        if (!consuming) {
          // The real outcome, not the generator's intent: the data source is what the planner asks.
          assertFalse(metadata.isSorted(), "offline valCol must not be flagged sorted, segment " + i);
          assertEquals(metadata.getCardinality(), rows, "every valCol value distinct within segment " + i);
        }
        // Ranges are contiguous and disjoint across the whole mix, so uniqueness holds globally.
        assertEquals(((Number) metadata.getMinValue()).longValue(), expectedMin, "min of segment " + i);
        assertEquals(((Number) metadata.getMaxValue()).longValue(), expectedMin + rows - 1, "max of segment " + i);
        expectedMin += rows == 40 ? 40 : 100;
      }
      SortedMergeFixture.requireValColUnsorted(segments.subList(0, 3), "test");
    } finally {
      SortedMergeFixture.purgeAll();
    }
  }

  @Test
  public void zeroConsumingRowsReturnsExactlyTheOfflineSegments() {
    try {
      List<IndexSegment> offline = SortedMergeFixture.segments(Overlap.FULL, 2, 50, 1);
      assertSame(SortedMergeFixture.segments(Overlap.FULL, 2, 50, 1, 0), offline);
    } finally {
      SortedMergeFixture.purgeAll();
    }
  }

  /// Creates a key directory holding one file, so an assertion that it was deleted is about real content rather than
  /// an empty directory that might vanish for another reason.
  private File keyDirectory(String key)
      throws IOException {
    File dir = new File(_baseDir, key);
    assertTrue(dir.mkdirs());
    assertTrue(new File(dir, "segment.placeholder").createNewFile());
    return dir;
  }

  private File lockFileFor(String key) {
    return new File(_baseDir, key + ".lock");
  }
}
