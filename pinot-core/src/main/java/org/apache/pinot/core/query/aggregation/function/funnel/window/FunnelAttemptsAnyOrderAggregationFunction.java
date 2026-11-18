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
package org.apache.pinot.core.query.aggregation.function.funnel.window;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.FunnelAttemptFunctions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.query.QueryThreadContext;


/// `FUNNEL_ATTEMPTS_ANY_ORDER(timestampExpression, conversionWindow, numSteps, step1, ..., stepN)`: the funnel
/// attempts of each key, for counting attempts per bucket of attempt start.
///
/// An attempt starts at an entry-step event and lasts the conversion window; steps 2..N count in any order. Over a
/// key's events in time order:
/// - an attempt opens at a step-1 event and covers `[start, start + conversionWindow)`;
/// - a step-1 event inside an open attempt is absorbed: it neither restarts nor extends it;
/// - the first step-1 event at or after `start + conversionWindow` opens the next attempt;
/// - an attempt does not end early when all steps converted; events before any step 1 are ignored;
/// - an attempt's depth is the number of leading steps it converted: step k converts if steps 1..k were all seen in
///   the attempt, in any order, so the depth is in `[1, numSteps]`.
///
/// An attempt is not an app session (sessions are cut by inactivity). Attempts start at the first step 1 in the
/// scanned rows, with no lookback, so results depend on the query start time. The name `FUNNEL_ATTEMPTS` is reserved
/// for the ordered variant, which would apply the funnel modes inside an attempt.
///
/// The final result is a `LONG` array with one element per attempt, sorted by start, empty when the key has no
/// attempt. Each element is opaque: read it only with the `funnelAttemptStart` and `funnelAttemptDepth` scalar
/// functions ([FunnelAttemptFunctions]). A start outside 57 signed bits fails the query.
///
/// Modes, `maxStepDuration`, other `key=value` arguments and more than 63 steps are rejected. A row that matches
/// several steps counts as its first matching step. The intermediate result, its serialization and `merge()` are the
/// funnel base's. Instances are stateless after construction and thread-safe.
public class FunnelAttemptsAnyOrderAggregationFunction extends FunnelBaseAggregationFunction<long[]> {
  private static final String NAME = "FUNNEL_ATTEMPTS_ANY_ORDER";

  public FunnelAttemptsAnyOrderAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(checkNumSteps(arguments), nullHandlingEnabled);
    for (Mode mode : Mode.values()) {
      Preconditions.checkArgument(!_modes.contains(mode), "%s does not accept a mode, got: %s", NAME, mode);
    }
    Preconditions.checkArgument(_maxStepDuration == 0, "%s does not accept maxStepDuration", NAME);
    Preconditions.checkArgument(_extraArguments.isEmpty(), "%s does not accept the arguments: %s", NAME,
        _extraArguments.keySet());
  }

  /// Runs before the base parses the arguments, which would read the step expressions of a bad `numSteps` as extra
  /// arguments.
  private static List<ExpressionContext> checkNumSteps(List<ExpressionContext> arguments) {
    if (arguments.size() > 2) {
      int numSteps = arguments.get(2).getLiteral().getIntValue();
      Preconditions.checkArgument(numSteps >= 1 && numSteps <= FunnelAttemptFunctions.MAX_ATTEMPT_DEPTH,
          "%s expects numSteps in [1, %s], got: %s", NAME, FunnelAttemptFunctions.MAX_ATTEMPT_DEPTH, numSteps);
    }
    return arguments;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELATTEMPTSANYORDER;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG_ARRAY;
  }

  @Override
  public long[] extractFinalResult(@Nullable PriorityQueue<FunnelStepEvent> stepEvents) {
    if (stepEvents == null || stepEvents.isEmpty()) {
      return new long[0];
    }
    FunnelStepEvent[] events = stepEvents.toArray(new FunnelStepEvent[0]);
    Arrays.sort(events);
    LongArrayList attempts = new LongArrayList();
    boolean open = false;
    long start = 0;
    long seenSteps = 0;
    for (int i = 0; i < events.length; i++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, "FunnelAttemptsAnyOrder#extractFinalResult");
      long timestamp = events[i].getTimestamp();
      int step = events[i].getStep();
      // timestamp >= start, so the unsigned difference is exact
      if (open && Long.compareUnsigned(timestamp - start, _windowSize) >= 0) {
        attempts.add(FunnelAttemptFunctions.pack(start, depth(seenSteps)));
        open = false;
      }
      if (!open) {
        if (step != 0) {
          continue;
        }
        open = true;
        start = timestamp;
        seenSteps = 0;
      }
      seenSteps |= 1L << step;
    }
    if (open) {
      attempts.add(FunnelAttemptFunctions.pack(start, depth(seenSteps)));
    }
    return attempts.toLongArray();
  }

  /// The number of leading steps seen.
  private static int depth(long seenSteps) {
    return Long.numberOfTrailingZeros(~seenSteps);
  }

  /// Concatenates the attempts and sorts them by start. Exact only when all of a key's events are on one server;
  /// otherwise each input was computed from a subset of the key's events and the result is wrong.
  @Override
  public long[] mergeFinalResult(long[] finalResult1, long[] finalResult2) {
    long[] merged = Arrays.copyOf(finalResult1, finalResult1.length + finalResult2.length);
    System.arraycopy(finalResult2, 0, merged, finalResult1.length, finalResult2.length);
    // packed attempts sort by start
    Arrays.sort(merged);
    return merged;
  }
}
