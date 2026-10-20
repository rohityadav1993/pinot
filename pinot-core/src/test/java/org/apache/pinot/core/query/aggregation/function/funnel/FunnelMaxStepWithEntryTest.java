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
package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepWithEntryAggregationFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// `FUNNEL_MAX_STEP_WITH_ENTRY` must agree with `FUNNEL_MAX_STEP` on the deepest step and report the start of the
/// window that first reached it.
public class FunnelMaxStepWithEntryTest {
  private static final long MIN = Long.MIN_VALUE;
  private static final long W = 100;

  private static List<ExpressionContext> args(int numSteps, String... extra) {
    List<ExpressionContext> arguments = new ArrayList<>(List.of(ExpressionContext.forIdentifier("ts"),
        ExpressionContext.forLiteral(Literal.longValue(W)), ExpressionContext.forLiteral(Literal.intValue(numSteps))));
    for (int i = 0; i < numSteps; i++) {
      arguments.add(ExpressionContext.forIdentifier("step" + i));
    }
    for (String e : extra) {
      arguments.add(ExpressionContext.forLiteral(Literal.stringValue(e)));
    }
    return arguments;
  }

  /// Events as {timestamp, step} pairs.
  private static PriorityQueue<FunnelStepEvent> events(long[]... events) {
    PriorityQueue<FunnelStepEvent> queue = new PriorityQueue<>();
    for (long[] e : events) {
      queue.add(new FunnelStepEvent(e[0], (int) e[1]));
    }
    return queue;
  }

  @DataProvider
  public Object[][] cases() {
    return new Object[][]{
        // single attempt
        {3, new String[0], events(new long[]{10, 0}, new long[]{20, 1}, new long[]{30, 2}), 3, 10},
        {3, new String[0], events(new long[]{10, 0}, new long[]{20, 1}), 2, 10},
        // retry where the later attempt goes deeper: entry is the later attempt
        {3, new String[0], events(new long[]{10, 0}, new long[]{500, 0}, new long[]{510, 1}, new long[]{520, 2}), 3,
            500},
        {3, new String[0], events(new long[]{10, 0}, new long[]{500, 0}, new long[]{510, 1}), 2, 500},
        // ties: the earliest window that reached the deepest step wins
        {3, new String[0], events(new long[]{10, 0}, new long[]{20, 1}, new long[]{500, 0}, new long[]{510, 1}), 2,
            10},
        {2, new String[0], events(new long[]{10, 0}, new long[]{500, 0}), 1, 10},
        // no step 1: deepest step 0, placeholder entry
        {3, new String[0], events(new long[]{10, 1}, new long[]{20, 2}), 0, MIN},
        {3, new String[0], events(), 0, MIN},
        // strict order breaks the chain; the retry that goes deeper is the entry
        {3, new String[]{"STRICT_ORDER"}, events(new long[]{10, 0}, new long[]{20, 2}, new long[]{30, 1},
            new long[]{300, 0}, new long[]{310, 1}, new long[]{320, 2}), 3, 300},
        {3, new String[]{"STRICT_ORDER"}, events(new long[]{10, 0}, new long[]{20, 2}, new long[]{30, 1}), 1, 10},
        // strict increase ignores the same-timestamp event
        {2, new String[]{"STRICT_INCREASE"}, events(new long[]{10, 0}, new long[]{10, 1}, new long[]{30, 1}), 2, 10},
        // strict deduplication
        {3, new String[]{"STRICT_DEDUPLICATION"}, events(new long[]{10, 0}, new long[]{20, 1}, new long[]{30, 1},
            new long[]{40, 2}, new long[]{400, 0}, new long[]{410, 1}, new long[]{420, 2}), 3, 400},
        // maximum step duration breaks the window between step events; later attempt reaches step 2
        {3, new String[]{"MAXSTEPDURATION=15"}, events(new long[]{10, 0}, new long[]{20, 1}, new long[]{60, 2},
            new long[]{70, 0}, new long[]{80, 1}, new long[]{90, 2}), 3, 70},
        {3, new String[]{"MAXSTEPDURATION=15"}, events(new long[]{10, 0}, new long[]{20, 1}, new long[]{60, 2}), 2, 10},
    };
  }

  @Test(dataProvider = "cases")
  public void testMaxStepAndEntry(int numSteps, String[] extra, PriorityQueue<FunnelStepEvent> events,
      int expectedStep, long expectedEntry) {
    FunnelMaxStepWithEntryAggregationFunction withEntry =
        new FunnelMaxStepWithEntryAggregationFunction(args(numSteps, extra), false);
    FunnelMaxStepAggregationFunction maxStep = new FunnelMaxStepAggregationFunction(args(numSteps, extra), false);
    LongArrayList result = withEntry.extractFinalResult(events);
    assertEquals(result, LongArrayList.wrap(new long[]{expectedStep, expectedEntry}));
    assertEquals((int) maxStep.extractFinalResult(events), expectedStep);
    // the input accumulator is not consumed
    assertEquals(withEntry.extractFinalResult(events), result);
  }

  @Test
  public void testNullIntermediate() {
    FunnelMaxStepWithEntryAggregationFunction f = new FunnelMaxStepWithEntryAggregationFunction(args(2), false);
    assertEquals(f.extractFinalResult(null), LongArrayList.wrap(new long[]{0, MIN}));
  }

  @Test
  public void testMergeIntermediateThenExtract() {
    FunnelMaxStepWithEntryAggregationFunction f = new FunnelMaxStepWithEntryAggregationFunction(args(2), false);
    PriorityQueue<FunnelStepEvent> a = events(new long[]{10, 0});
    PriorityQueue<FunnelStepEvent> b = events(new long[]{20, 1}, new long[]{500, 0}, new long[]{510, 1});
    assertEquals(f.extractFinalResult(f.merge(a, b)), LongArrayList.wrap(new long[]{2, 10}));
  }

  @Test
  public void testMergeFinalResult() {
    FunnelMaxStepWithEntryAggregationFunction f = new FunnelMaxStepWithEntryAggregationFunction(args(3), false);
    assertEquals(f.mergeFinalResult(LongArrayList.wrap(new long[]{1, 5}), LongArrayList.wrap(new long[]{2, 9})),
        LongArrayList.wrap(new long[]{2, 9}));
    assertEquals(f.mergeFinalResult(LongArrayList.wrap(new long[]{2, 9}), LongArrayList.wrap(new long[]{2, 5})),
        LongArrayList.wrap(new long[]{2, 5}));
  }
}
