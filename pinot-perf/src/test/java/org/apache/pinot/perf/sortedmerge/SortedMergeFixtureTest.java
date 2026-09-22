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
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
