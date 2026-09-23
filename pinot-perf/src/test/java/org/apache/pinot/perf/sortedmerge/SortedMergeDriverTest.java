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
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.perf.sortedmerge.SortedMergeDriver.OrderBy;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests the query-shape axis of [SortedMergeDriver.OrderBy], which had zero coverage before this test.
///
/// <p>A published benchmark suite used {@link SortedMergeDriver.OrderBy#TS_ONLY} to test the prediction that
/// dropping the tiebreaker tightens the leaf's scan block to the limit, and that prediction *failed*. A harness bug
/// that silently emitted the {@link SortedMergeFixture#VAL_COL} tiebreaker under both shapes -- i.e. built the same
/// query context for {@code TS_ONLY} as for {@link SortedMergeDriver.OrderBy#TS_VAL} -- would produce output
/// indistinguishable from that genuine refutation: both would show the "no tail to sort" optimization failing to
/// engage. This test exists to tell those two worlds apart by asserting, directly on the built
/// {@link QueryContext}, exactly which ORDER BY expressions each shape produces, so a refutation is trusted only
/// once it is known the axis itself was actually exercised.
public class SortedMergeDriverTest {
  private static final int LIMIT = 10;
  /// Any non-positive value leaves the block size unset: buildQueryContext only calls
  /// setSortedSelectionMergeBlockSize when it is positive, and the shape of the ORDER BY does not depend on it.
  private static final int BLOCK_SIZE = -1;
  private static final int MAX_EXECUTION_THREADS = 2;

  @Test
  public void tsValOrderByIsTsColThenValColTotalOrder() {
    // Catches a regression that drops the tiebreaker, silently collapsing TS_VAL into the TS_ONLY shape and
    // defeating the total-order guarantee the fixture's globally unique valCol is there to provide.
    QueryContext queryContext = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_VAL);
    List<OrderByExpressionContext> orderBy = queryContext.getOrderByExpressions();
    assertEquals(orderBy.size(), 2, "TS_VAL must order by exactly two expressions");
    assertEquals(identifier(orderBy.get(0)), SortedMergeFixture.TS_COL);
    assertEquals(identifier(orderBy.get(1)), SortedMergeFixture.VAL_COL);
  }

  @Test
  public void tsOnlyOrderByIsTsColAlone() {
    // Catches a regression that keeps emitting the valCol tiebreaker under TS_ONLY. That would make TS_ONLY
    // structurally identical to TS_VAL, so the leaf's sortedColumnsPrefixSize would never cover the whole ORDER BY
    // and the tightened per-cursor scan block this shape exists to exercise would never engage -- silently, since
    // the run would still complete and look like a normal (if unfavorable) result rather than a broken axis.
    QueryContext queryContext = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_ONLY);
    List<OrderByExpressionContext> orderBy = queryContext.getOrderByExpressions();
    assertEquals(orderBy.size(), 1, "TS_ONLY must drop the tiebreaker and order by exactly one expression");
    assertEquals(identifier(orderBy.get(0)), SortedMergeFixture.TS_COL);
  }

  @Test
  public void selectListIsIdenticalAndThreeWideUnderBothShapes() {
    // The harness documentation claims the projected row width never varies with the ORDER BY shape, and that
    // claim is load-bearing for interpreting benchmark results (e.g. the row-hash digest in SortedMergeDriver): if
    // the SELECT list silently changed between shapes, digests and row shapes from different cells would stop being
    // comparable without anyone noticing. Assert it rather than trust it.
    QueryContext tsVal = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_VAL);
    QueryContext tsOnly = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_ONLY);

    List<String> expectedSelectList =
        List.of(SortedMergeFixture.TS_COL, SortedMergeFixture.VAL_COL, SortedMergeFixture.PAYLOAD_COL);
    assertEquals(selectIdentifiers(tsVal), expectedSelectList, "TS_VAL must select tsCol, valCol, payloadCol");
    assertEquals(selectIdentifiers(tsOnly), expectedSelectList, "TS_ONLY must select the same three columns");
    assertEquals(tsVal.getSelectExpressions().size(), 3);
    assertEquals(tsOnly.getSelectExpressions().size(), 3);
  }

  @Test
  public void noArgOrderByOverloadDefaultsToTsVal() {
    // Catches a regression in the legacy four-arg overload that callers written before OrderBy existed still use:
    // if its hardcoded default silently drifted from TS_VAL, those callers would start exercising a different
    // query shape than the one they were written and validated against, with no compiler signal.
    QueryContext defaulted = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS);
    QueryContext explicitTsVal = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_VAL);

    List<OrderByExpressionContext> defaultedOrderBy = defaulted.getOrderByExpressions();
    List<OrderByExpressionContext> explicitOrderBy = explicitTsVal.getOrderByExpressions();
    assertEquals(defaultedOrderBy.size(), explicitOrderBy.size());
    for (int i = 0; i < defaultedOrderBy.size(); i++) {
      assertEquals(identifier(defaultedOrderBy.get(i)), identifier(explicitOrderBy.get(i)));
    }
  }

  @Test
  public void orderByDirectionIsAscendingUnderBothShapes() {
    // Catches a regression that flips the sort direction, which would silently invert which rows fall inside the
    // LIMIT boundary while leaving the ORDER BY expression list itself looking correct.
    QueryContext tsVal = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_VAL);
    for (OrderByExpressionContext orderByExpression : tsVal.getOrderByExpressions()) {
      assertEquals(orderByExpression.isAsc(), true, "TS_VAL must order ascending: " + orderByExpression);
    }

    QueryContext tsOnly = SortedMergeDriver.buildQueryContext(LIMIT, SortedSelectionMergeMode.ON, BLOCK_SIZE,
        MAX_EXECUTION_THREADS, OrderBy.TS_ONLY);
    for (OrderByExpressionContext orderByExpression : tsOnly.getOrderByExpressions()) {
      assertEquals(orderByExpression.isAsc(), true, "TS_ONLY must order ascending: " + orderByExpression);
    }
  }

  private static String identifier(OrderByExpressionContext orderByExpression) {
    return orderByExpression.getExpression().getIdentifier();
  }

  private static List<String> selectIdentifiers(QueryContext queryContext) {
    return queryContext.getSelectExpressions().stream().map(ExpressionContext::getIdentifier)
        .collect(Collectors.toList());
  }
}
