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
import org.apache.pinot.perf.sortedmerge.SortedMergeDriver.OrderBy;
import org.apache.pinot.perf.sortedmerge.SortedMergeDriver.Result;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
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

  /// Hand-builds a [Result] with exactly the fields [SortedMergeMemoryProbe#cellVerdict] reads.
  /// `rowDigest` and `tsDigest` are supplied directly rather than recomputed from `rows`, because the whole point
  /// of testing the verdict in isolation is that it trusts whatever digest a Result carries -- recomputing here
  /// would just be testing the digest functions again, which {@link #tsDigestIsCommutativeOverRows} and its
  /// neighbours already do.
  private static Result result(long numRows, long rowDigest, long tsDigest, List<Object[]> rows) {
    Result result = new Result();
    result._numRows = numRows;
    result._rowDigest = rowDigest;
    result._tsDigest = tsDigest;
    result._rows = rows;
    return result;
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

  /// The gate's own orchestration -- not just the primitives above -- needs coverage: on correct operators the fail
  /// path of [SortedMergeMemoryProbe#cellVerdict] never runs, so nothing short of hand-built mismatched Results
  /// exercises it. A regression that flipped the `strict` condition or dropped a comparison would keep every real
  /// run passing while checking the wrong thing.
  @Test
  public void differingRowCountsFailUnderBothShapes() {
    // Row count is checked before either shape-specific comparison and before touching `_rows` at all, so
    // `collectRows=false` here proves that ordering rather than accidentally exercising a later branch.
    Result off = result(3, 0L, 0L, null);
    Result on = result(4, 0L, 0L, null);
    assertEquals(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_VAL, false),
        "row count OFF=3 ON=4");
    assertEquals(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_ONLY, false),
        "row count OFF=3 ON=4");
  }

  @Test
  public void tsValRejectsDifferingFullRowDigest() {
    // Under TS_VAL the full-row digest is a total-order proxy: two arms that disagree on it disagree on the
    // answer, full stop.
    Result off = result(2, 100L, 50L, rows(1L, 2L));
    Result on = result(2, 200L, 50L, rows(1L, 2L));
    assertEquals(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_VAL, true),
        "multiset digest differs (OFF=100 ON=200)");
  }

  @Test
  public void tsValAcceptsMatchingRowsAsThePass() {
    // The TS_VAL happy path: rowDigest agrees, the collected rows canonicalize to the same multiset, and both
    // arms are sorted by ts. This is the branch every correct arm4 run under TS_VAL actually exercises, so it
    // earns direct coverage rather than being merely implied by the failure tests around it.
    Result off = result(4, 100L, 100L, rows(1L, 2L, 2L, 5L));
    Result on = result(4, 100L, 100L, new ArrayList<>(rows(1L, 2L, 2L, 5L)));
    assertNull(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_VAL, true));
  }

  @Test
  public void tsValRejectsCanonicalMultisetMismatchDespiteEqualRowDigest() {
    // _rowDigest is a sum of per-row hashes, not collision-proof by construction, so digest-equal-but-different-rows
    // is exactly the case the canonical multiset comparison exists to catch once the cheaper digest check has
    // already passed. Hand-setting equal digests here forces execution past that first check and into this one.
    Result off = result(2, 100L, 100L, rows(1L, 2L));
    Result on = result(2, 100L, 100L, List.of(row(1L, 99), row(2L, 88)));
    assertEquals(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_VAL, true), "canonical multiset differs");
  }

  @Test
  public void tsOnlyRejectsDifferingTsColumnDigest() {
    // Under TS_ONLY the full-row digest is not consulted at all, so this is the first and cheapest line of
    // defense -- available even above FULL_MULTISET_LIMIT, where rows are never retained.
    Result off = result(2, 999L, 10L, rows(1L, 2L));
    Result on = result(2, 999L, 20L, rows(1L, 2L));
    assertEquals(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_ONLY, true),
        "tsCol value digest differs (OFF=10 ON=20)");
  }

  @Test
  public void tsOnlyAcceptsArmsThatPickedDifferentTiedRows() {
    // This is the whole point of the tie-sound relaxation: both arms return ts=4 twice, so the tsCol multiset and
    // digest agree, but they chose different rows to satisfy that tie -- different valCol, different payload. A
    // strict, full-row comparison would falsely fail this cell; TS_ONLY must not even look at the full-row digest.
    List<Object[]> offRows = List.of(row(4L, 100), row(4L, 101));
    List<Object[]> onRows = List.of(row(4L, 900), row(4L, 901));
    // Deliberately different and, on their own, meaningless under TS_ONLY -- set to prove they are never consulted.
    long offRowDigest = 111L;
    long onRowDigest = 222L;
    long tsDigest = tsDigest(offRows);
    assertEquals(tsDigest(onRows), tsDigest, "the tied rows must share a tsCol digest for this case to be valid");
    Result off = result(2, offRowDigest, tsDigest, offRows);
    Result on = result(2, onRowDigest, tsDigest, onRows);
    assertNull(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_ONLY, true),
        "arms that only differ in which tied row they kept must pass under TS_ONLY");
  }

  @Test
  public void tsValRejectsTheSameTiedRowsPairThatTsOnlyAccepts() {
    // Same rows as tsOnlyAcceptsArmsThatPickedDifferentTiedRows, same shared tsCol digest -- but TS_VAL is a total
    // order, so agreeing on tsCol is not enough; the arms must agree on the full row, and here they do not.
    List<Object[]> offRows = List.of(row(4L, 100), row(4L, 101));
    List<Object[]> onRows = List.of(row(4L, 900), row(4L, 901));
    long tsDigest = tsDigest(offRows);
    Result off = result(2, 111L, tsDigest, offRows);
    Result on = result(2, 222L, tsDigest, onRows);
    assertEquals(SortedMergeMemoryProbe.cellVerdict(off, on, OrderBy.TS_VAL, true),
        "multiset digest differs (OFF=111 ON=222)");
  }

  @Test
  public void aboveFullMultisetLimitVerdictRestsOnDigestAndRowCountOnly() {
    // Above FULL_MULTISET_LIMIT rows are never retained (collectRows=false, _rows stays null), so the verdict must
    // neither dereference `_rows` nor silently degrade to a bare row-count check: a digest mismatch here is the
    // only signal left for a cell at the exact scale this harness exists to probe, and it must still be caught.
    Result offMismatch = result(1_000_000, 1L, 1L, null);
    Result onMismatch = result(1_000_000, 2L, 1L, null);
    assertEquals(SortedMergeMemoryProbe.cellVerdict(offMismatch, onMismatch, OrderBy.TS_VAL, false),
        "multiset digest differs (OFF=1 ON=2)");
    Result offTsMismatch = result(1_000_000, 1L, 1L, null);
    Result onTsMismatch = result(1_000_000, 1L, 2L, null);
    assertEquals(SortedMergeMemoryProbe.cellVerdict(offTsMismatch, onTsMismatch, OrderBy.TS_ONLY, false),
        "tsCol value digest differs (OFF=1 ON=2)");

    // And the matching pass case: same digest, no rows to compare, no NPE.
    Result offPass = result(1_000_000, 5L, 5L, null);
    Result onPass = result(1_000_000, 5L, 5L, null);
    assertNull(SortedMergeMemoryProbe.cellVerdict(offPass, onPass, OrderBy.TS_VAL, false));
    assertNull(SortedMergeMemoryProbe.cellVerdict(offPass, onPass, OrderBy.TS_ONLY, false));
  }
}
