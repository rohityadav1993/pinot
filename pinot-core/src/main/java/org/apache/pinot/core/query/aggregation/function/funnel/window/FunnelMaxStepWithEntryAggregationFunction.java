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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// `FUNNEL_MAX_STEP_WITH_ENTRY`: like [FunnelMaxStepAggregationFunction] (same arguments, modes and maximum step
/// duration) but returns a two-element `LONG` array `[maxStep, entryTs]`.
///
/// - `maxStep` is the deepest step reached, identical to `FUNNEL_MAX_STEP` on the same input.
/// - `entryTs` is the start timestamp of the window that first reached `maxStep`. Only a strict improvement moves the
///   entry, so on ties the earliest window wins. When `maxStep` is 0 there is no entry and `entryTs` is
///   [#NO_ENTRY] (`Long.MIN_VALUE`).
///
/// The intermediate result, its serialization, `merge()`, `fillWindow()`, the per-window step logic and the window
/// scan (`findMaxStep`) are the ordered-funnel base's, shared with `FUNNEL_MAX_STEP`. Because both values come from one
/// scan, they always describe the same chain. Instances are stateless after construction and thread-safe.
public class FunnelMaxStepWithEntryAggregationFunction extends FunnelBaseAggregationFunction<LongArrayList> {
  /// Placeholder for the entry timestamp when no step was reached.
  public static final long NO_ENTRY = MaxStep.NO_ENTRY;

  public FunnelMaxStepWithEntryAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELMAXSTEPWITHENTRY;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG_ARRAY;
  }

  @Override
  public LongArrayList extractFinalResult(@Nullable PriorityQueue<FunnelStepEvent> stepEvents) {
    MaxStep maxStep = findMaxStep(stepEvents);
    return LongArrayList.wrap(new long[]{maxStep.getStep(), maxStep.getEntryTimestamp()});
  }

  /// Keeps the deeper result; on equal depth keeps the earlier entry.
  @Override
  public LongArrayList mergeFinalResult(LongArrayList finalResult1, LongArrayList finalResult2) {
    long step1 = finalResult1.getLong(0);
    long step2 = finalResult2.getLong(0);
    if (step1 != step2) {
      return step1 > step2 ? finalResult1 : finalResult2;
    }
    return finalResult1.getLong(1) <= finalResult2.getLong(1) ? finalResult1 : finalResult2;
  }
}
