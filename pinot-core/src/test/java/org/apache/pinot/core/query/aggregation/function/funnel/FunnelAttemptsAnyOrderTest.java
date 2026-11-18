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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderUtils;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelAttemptsAnyOrderAggregationFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.MAX_ATTEMPT_START;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.MIN_ATTEMPT_START;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.funnelAttemptDepth;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.funnelAttemptStart;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.pack;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// `FUNNEL_ATTEMPTS_ANY_ORDER` against hand-worked cases and against [FunnelSessionsOracle], the port of the
/// requirement doc's `funnel_sessions`. Results are read only through the decode scalars.
///
/// Steps are 0-based in the function's events (`A` = 0) and 1-based in the oracle. Events with equal timestamps are
/// ordered by step, as the base's event queue orders them; the doc only says "sorted by timestamp", and the order
/// matters only for a non-entry step sharing the timestamp of the step 1 that opens an attempt.
public class FunnelAttemptsAnyOrderTest {
  private static final int A = 0;
  private static final int B = 1;
  private static final int C = 2;

  private static List<ExpressionContext> args(long cw, int numSteps, String... extra) {
    List<ExpressionContext> arguments = new ArrayList<>(List.of(ExpressionContext.forIdentifier("ts"),
        ExpressionContext.forLiteral(Literal.longValue(cw)), ExpressionContext.forLiteral(Literal.intValue(numSteps))));
    for (int i = 0; i < numSteps; i++) {
      arguments.add(ExpressionContext.forIdentifier("step" + i));
    }
    for (String e : extra) {
      arguments.add(ExpressionContext.forLiteral(Literal.stringValue(e)));
    }
    return arguments;
  }

  private static FunnelAttemptsAnyOrderAggregationFunction function(long cw, int numSteps) {
    return new FunnelAttemptsAnyOrderAggregationFunction(args(cw, numSteps), false);
  }

  /// Events as {timestamp, step} pairs.
  private static PriorityQueue<FunnelStepEvent> events(long[]... events) {
    PriorityQueue<FunnelStepEvent> queue = new PriorityQueue<>();
    for (long[] e : events) {
      queue.add(new FunnelStepEvent(e[0], (int) e[1]));
    }
    return queue;
  }

  private static long[] e(long timestamp, int step) {
    return new long[]{timestamp, step};
  }

  /// Decodes a final result into {start, depth} pairs with the scalar functions.
  private static List<List<Long>> decode(long[] attempts) {
    List<List<Long>> decoded = new ArrayList<>(attempts.length);
    for (long attempt : attempts) {
      decoded.add(List.of(funnelAttemptStart(attempt), (long) funnelAttemptDepth(attempt)));
    }
    return decoded;
  }

  private static List<List<Long>> attempts(long[]... startAndDepth) {
    List<List<Long>> attempts = new ArrayList<>(startAndDepth.length);
    for (long[] a : startAndDepth) {
      attempts.add(List.of(a[0], a[1]));
    }
    return attempts;
  }

  private static List<List<Long>> oracle(long cw, int numSteps, PriorityQueue<FunnelStepEvent> events) {
    List<FunnelStepEvent> sorted = new ArrayList<>(events);
    Collections.sort(sorted);
    List<long[]> rowMatches = new ArrayList<>(sorted.size());
    for (FunnelStepEvent event : sorted) {
      rowMatches.add(new long[]{event.getTimestamp(), event.getStep() + 1});
    }
    List<List<Long>> attempts = new ArrayList<>();
    for (FunnelSessionsOracle.Session session : FunnelSessionsOracle.funnelSessions(cw, numSteps, rowMatches)) {
      attempts.add(List.of(session._start, (long) session.depth()));
    }
    return attempts;
  }

  @DataProvider
  public Object[][] handWorkedCases() {
    long t = 1_700_000_000_000L;
    return new Object[][]{
        // ADR 0014 worked example: A@8 absorbed, B@12 after the first attempt and not an entry step
        {"ADR 0014 worked example", 10L, 3,
            events(e(0, A), e(2, C), e(3, B), e(8, A), e(12, B), e(25, A), e(27, B)),
            attempts(new long[]{0, 3}, new long[]{25, 2})},
        {"event at exactly start + cw is outside the attempt", 10L, 2, events(e(0, A), e(10, B)),
            attempts(new long[]{0, 1})},
        {"entry step inside an open attempt is absorbed", 10L, 2, events(e(0, A), e(5, A), e(12, B)),
            attempts(new long[]{0, 1})},
        {"entry step at start + cw opens the next attempt", 10L, 2, events(e(0, A), e(10, A), e(15, B)),
            attempts(new long[]{0, 1}, new long[]{10, 2})},
        {"steps 2..N in any order", 10L, 3, events(e(0, A), e(1, C), e(2, B)), attempts(new long[]{0, 3})},
        {"a later step without the earlier ones does not convert", 10L, 3, events(e(0, A), e(1, C)),
            attempts(new long[]{0, 1})},
        {"events before any entry step are ignored", 10L, 3, events(e(0, B), e(1, C), e(5, A), e(6, B)),
            attempts(new long[]{5, 2})},
        {"no entry step", 10L, 3, events(e(0, B), e(1, C)), attempts()},
        {"no events", 10L, 3, events(), attempts()},
        {"several attempts", 10L, 3,
            events(e(0, A), e(1, B), e(20, A), e(40, A), e(41, B), e(42, C)),
            attempts(new long[]{0, 2}, new long[]{20, 1}, new long[]{40, 3})},
        {"no early end on full conversion", 10L, 2, events(e(0, A), e(1, B), e(5, A)), attempts(new long[]{0, 2})},
        {"numSteps = 1", 10L, 1, events(e(0, A), e(5, A), e(10, A), e(25, A)),
            attempts(new long[]{0, 1}, new long[]{10, 1}, new long[]{25, 1})},
        {"equal timestamps: the entry step sorts first", 10L, 2, events(e(5, B), e(5, A)),
            attempts(new long[]{5, 2})},
        {"epoch milliseconds", 3_600_000L, 3, events(e(t, A), e(t + 1_000, C), e(t + 3_599_999, B)),
            attempts(new long[]{t, 3})},
        {"negative timestamps", 10L, 2, events(e(-25, A), e(-20, B), e(-15, A)),
            attempts(new long[]{-25, 2}, new long[]{-15, 1})}
    };
  }

  @Test(dataProvider = "handWorkedCases")
  public void testHandWorkedCases(String name, long cw, int numSteps, PriorityQueue<FunnelStepEvent> events,
      List<List<Long>> expected) {
    FunnelAttemptsAnyOrderAggregationFunction f = function(cw, numSteps);
    long[] result = f.extractFinalResult(events);
    assertEquals(decode(result), expected, name);
    assertEquals(oracle(cw, numSteps, events), expected, name + " (oracle)");
    // the input accumulator is not consumed
    assertEquals(f.extractFinalResult(events), result, name);
  }

  @Test
  public void testNullIntermediateHasNoAttempt() {
    assertEquals(function(10, 2).extractFinalResult(null), new long[0]);
  }

  /// Random event streams, split into several accumulators at random points and merged with `merge()` as segments
  /// and servers would, then extracted.
  @Test
  public void testRandomStreamsMatchOracle() {
    Random random = new Random(20261118L);
    int numNonEmpty = 0;
    for (int iteration = 0; iteration < 2_000; iteration++) {
      int numSteps = 1 + random.nextInt(5);
      long cw = 1 + random.nextInt(30);
      List<FunnelStepEvent> stream = new ArrayList<>();
      int numEvents = random.nextInt(40);
      for (int i = 0; i < numEvents; i++) {
        stream.add(new FunnelStepEvent(-50 + random.nextInt(200), random.nextInt(numSteps)));
      }
      FunnelAttemptsAnyOrderAggregationFunction f = function(cw, numSteps);

      Collections.shuffle(stream, random);
      int numParts = 1 + random.nextInt(4);
      List<PriorityQueue<FunnelStepEvent>> parts = new ArrayList<>(numParts);
      for (int p = 0; p < numParts; p++) {
        parts.add(new PriorityQueue<>());
      }
      for (FunnelStepEvent event : stream) {
        parts.get(random.nextInt(numParts)).add(event);
      }
      PriorityQueue<FunnelStepEvent> merged = parts.get(0);
      for (int p = 1; p < numParts; p++) {
        merged = f.merge(merged, parts.get(p));
      }
      PriorityQueue<FunnelStepEvent> all = new PriorityQueue<>(stream);

      List<List<Long>> expected = oracle(cw, numSteps, all);
      String context = "iteration " + iteration + ", cw " + cw + ", numSteps " + numSteps + ", events " + stream;
      assertEquals(decode(f.extractFinalResult(merged)), expected, context);
      if (!expected.isEmpty()) {
        numNonEmpty++;
      }
    }
    assertTrue(numNonEmpty > 1_000, "too few streams with attempts: " + numNonEmpty);
  }

  @Test
  public void testMergeFinalResultConcatenatesAndSortsByStart() {
    FunnelAttemptsAnyOrderAggregationFunction f = function(10, 3);
    long[] merged = f.mergeFinalResult(new long[]{pack(10, 1), pack(30, 2)}, new long[]{pack(-5, 3), pack(20, 3)});
    assertEquals(decode(merged), attempts(new long[]{-5, 3}, new long[]{10, 1}, new long[]{20, 3},
        new long[]{30, 2}));
    assertEquals(decode(f.mergeFinalResult(new long[0], new long[]{pack(7, 2)})), attempts(new long[]{7, 2}));
    assertEquals(f.mergeFinalResult(new long[0], new long[0]), new long[0]);
  }

  /// The final result is the stored form of the LONG_ARRAY column, so the leaf can serialize it as is, e.g. when the
  /// sorted streaming GROUP BY returns final results to the MSE.
  @Test
  public void testFinalResultSerializesAsLongArrayColumn()
      throws Exception {
    FunnelAttemptsAnyOrderAggregationFunction f = function(10, 2);
    assertEquals(f.getFinalResultColumnType(), ColumnDataType.LONG_ARRAY);
    Object withAttempts = f.extractFinalResult(events(e(0, A), e(3, B), e(20, A)));
    Object withoutAttempts = f.extractFinalResult(events(e(0, B)));
    DataSchema schema = new DataSchema(new String[]{"f"}, new ColumnDataType[]{ColumnDataType.LONG_ARRAY});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    for (Object finalResult : List.of(withAttempts, withoutAttempts)) {
      builder.startRow();
      DataTableBuilderUtils.setColumn(builder, ColumnDataType.LONG_ARRAY, 0, finalResult);
      builder.finishRow();
    }
    DataTable dataTable = builder.build();
    assertEquals(decode(dataTable.getLongArray(0, 0)), attempts(new long[]{0, 2}, new long[]{20, 1}));
    assertEquals(dataTable.getLongArray(1, 0), new long[0]);
  }

  @DataProvider
  public Object[][] rejectedArguments() {
    return new Object[][]{
        {args(10, 2, "STRICT_ORDER"), "mode"},
        {args(10, 2, "STRICT_DEDUPLICATION"), "mode"},
        {args(10, 2, "STRICT_INCREASE"), "mode"},
        {args(10, 2, "KEEP_ALL"), "mode"},
        {args(10, 2, "MODE=STRICT_ORDER"), "mode"},
        {args(10, 2, "MAXSTEPDURATION=5"), "maxStepDuration"},
        {args(10, 2, "FOO=1"), "FOO"},
        {args(10, 0), "numSteps"},
        {zeroStepsWithOneStepExpression(), "numSteps"},
        {args(10, 64), "numSteps"}
    };
  }

  /// `numSteps = 0` with a step expression, which the base would otherwise read as an extra argument.
  private static List<ExpressionContext> zeroStepsWithOneStepExpression() {
    List<ExpressionContext> arguments = args(10, 0);
    arguments.add(ExpressionContext.forIdentifier("step0"));
    return arguments;
  }

  @Test(dataProvider = "rejectedArguments")
  public void testRejectsArguments(List<ExpressionContext> arguments, String expectedInMessage) {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> new FunnelAttemptsAnyOrderAggregationFunction(arguments, false));
    assertTrue(e.getMessage().contains("FUNNEL_ATTEMPTS_ANY_ORDER") && e.getMessage().contains(expectedInMessage),
        e.getMessage());
  }

  @Test
  public void testMaxStepsAccepted() {
    assertEquals(new FunnelAttemptsAnyOrderAggregationFunction(args(10, 63), false)
        .extractFinalResult(events(e(0, A))), new long[]{pack(0, 1)});
  }

  @Test
  public void testStartOutsidePackableRangeFailsTheQuery() {
    FunnelAttemptsAnyOrderAggregationFunction f = function(10, 2);
    assertEquals(decode(f.extractFinalResult(events(e(MAX_ATTEMPT_START, A), e(MAX_ATTEMPT_START + 1, B)))),
        attempts(new long[]{MAX_ATTEMPT_START, 2}));
    assertEquals(decode(f.extractFinalResult(events(e(MIN_ATTEMPT_START, A)))),
        attempts(new long[]{MIN_ATTEMPT_START, 1}));
    for (long start : new long[]{MAX_ATTEMPT_START + 1, MIN_ATTEMPT_START - 1, Long.MAX_VALUE, Long.MIN_VALUE}) {
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> f.extractFinalResult(events(e(start, A))));
      assertTrue(e.getMessage().contains(Long.toString(start)), e.getMessage());
    }
  }

  /// The attempt boundary must not overflow, neither `start + cw` nor `timestamp - start`.
  @Test
  public void testHugeWindowDoesNotOverflow() {
    FunnelAttemptsAnyOrderAggregationFunction f = function(Long.MAX_VALUE, 2);
    // the attempt ends at 10 + Long.MAX_VALUE, after the last possible timestamp
    assertEquals(decode(f.extractFinalResult(events(e(10, A), e(Long.MAX_VALUE, B)))), attempts(new long[]{10, 2}));
    // Long.MAX_VALUE - (-10) is past the window
    assertEquals(decode(f.extractFinalResult(events(e(-10, A), e(Long.MAX_VALUE, B)))), attempts(new long[]{-10, 1}));
  }

  @Test
  public void testResultIsSortedByStart() {
    long[] result = function(5, 2).extractFinalResult(events(e(40, A), e(-3, A), e(10, A), e(11, B)));
    List<List<Long>> decoded = decode(result);
    List<List<Long>> sorted = new ArrayList<>(decoded);
    sorted.sort(Comparator.comparing(a -> a.get(0)));
    assertEquals(decoded, sorted);
    assertEquals(decoded.size(), 3);
  }
}
