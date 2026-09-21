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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;


/// Tests the comparison primitives the arm-4 correctness gate is built from.
///
/// <p>These exist because the gate is an *assertion* mechanism, and an assertion mechanism that silently stops
/// asserting is worse than none at all: it keeps printing `gate OK` while checking nothing, and every number
/// measured under it inherits a confidence it has not earned. Running the gate by hand shows the pass path works.
/// It cannot show the fail path works, because on correct operators the fail path never runs. That is what these
/// cover: each primitive is given input it is supposed to reject, and is required to reject it.
///
/// <p>See {@link SortedMergeMemoryProbe} for the gate itself and {@link SortedMergeDriver.OrderBy} for why the
/// no-tiebreaker shape needs the weaker, tie-sound comparison rather than the full-row one.
public class SortedMergeGateTest {
  private static Object[] row(long ts, int val) {
    return new Object[]{ts, val, "payload-" + ts + "-" + val};
  }

  private static List<Object[]> rows(long... tsValues) {
    List<Object[]> rows = new ArrayList<>(tsValues.length);
    for (int i = 0; i < tsValues.length; i++) {
      rows.add(row(tsValues[i], i));
    }
    return rows;
  }

  private static long tsDigest(List<Object[]> rows) {
    long digest = 0L;
    for (Object[] r : rows) {
      digest += SortedMergeDriver.tsHash(r);
    }
    return digest;
  }

  @Test
  public void assertSortedByTsAcceptsAscendingIncludingTies() {
    SortedMergeDriver.assertSortedByTs(rows(1L, 2L, 2L, 2L, 5L, 9L));
    SortedMergeDriver.assertSortedByTs(rows());
    SortedMergeDriver.assertSortedByTs(rows(7L));
  }

  @Test
  public void assertSortedByTsRejectsADescendingStep() {
    // One inversion, in the middle, is the shape a partially broken merge produces. The check has to catch it
    // rather than only catching wholesale reversal.
    assertThrows(IllegalStateException.class, () -> SortedMergeDriver.assertSortedByTs(rows(1L, 2L, 9L, 5L, 9L)));
  }

  @Test
  public void tsColumnMultisetIgnoresOrderButNotContent() {
    List<Object[]> ascending = rows(1L, 2L, 2L, 5L);
    List<Object[]> shuffled = new ArrayList<>(ascending);
    Collections.reverse(shuffled);
    assertEquals(SortedMergeDriver.tsColumnMultiset(shuffled), SortedMergeDriver.tsColumnMultiset(ascending),
        "the multiset must not depend on the order rows arrive in");

    // The tie-soundness argument is that *which* tied row comes back is free but *how many* rows carry each value
    // is not. So a swap of one value for another must be caught even though the row count is unchanged.
    assertNotEquals(SortedMergeDriver.tsColumnMultiset(rows(1L, 2L, 2L, 6L)),
        SortedMergeDriver.tsColumnMultiset(ascending), "a changed tsCol value must not compare equal");
    assertNotEquals(SortedMergeDriver.tsColumnMultiset(rows(1L, 2L, 2L, 2L)),
        SortedMergeDriver.tsColumnMultiset(ascending), "a changed multiplicity must not compare equal");
  }

  @Test
  public void tsDigestIsCommutativeOverRows() {
    List<Object[]> ascending = rows(3L, 8L, 8L, 11L);
    List<Object[]> shuffled = new ArrayList<>(ascending);
    Collections.reverse(shuffled);
    assertEquals(tsDigest(shuffled), tsDigest(ascending),
        "the digest is summed, so it must be independent of arrival order");
  }

  @Test
  public void tsDigestDiscriminatesOnValuesAndMultiplicities() {
    // This is the property the gate leans on above FULL_MULTISET_LIMIT, where rows are not retained and the digest
    // is the only comparison left. If it stopped discriminating, those cells would silently degrade to a row count.
    long baseline = tsDigest(rows(3L, 8L, 8L, 11L));
    assertNotEquals(tsDigest(rows(3L, 8L, 9L, 11L)), baseline, "a changed value must change the digest");
    assertNotEquals(tsDigest(rows(3L, 8L, 8L, 8L)), baseline, "a changed multiplicity must change the digest");
    assertNotEquals(tsDigest(rows(3L, 8L, 8L)), baseline, "a dropped row must change the digest");
  }

  @Test
  public void tsDigestIgnoresColumnsOutsideTheSortKey() {
    // The whole point of the tsCol-only digest: two arms that legitimately chose different tied rows differ in
    // valCol and payload but must still agree here, or the gate would report a false failure under TS_ONLY.
    List<Object[]> armOne = List.of(row(4L, 100), row(4L, 101));
    List<Object[]> armTwo = List.of(row(4L, 900), row(4L, 901));
    assertEquals(tsDigest(armTwo), tsDigest(armOne));
    assertNotEquals(SortedMergeDriver.canonicalize(armTwo), SortedMergeDriver.canonicalize(armOne),
        "the rows really are different, so the full-row comparison should reject what the tsCol digest accepts");
  }

  @Test
  public void tsHashSeparatesStoredTypes() {
    // A LONG emitted where the other arm emits INT is a real difference the harness must not widen away.
    assertNotEquals(SortedMergeDriver.tsHash(new Object[]{4}), SortedMergeDriver.tsHash(new Object[]{4L}));
  }
}
