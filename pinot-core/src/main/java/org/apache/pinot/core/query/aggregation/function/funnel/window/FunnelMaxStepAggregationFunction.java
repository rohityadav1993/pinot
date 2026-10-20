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

import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FunnelMaxStepAggregationFunction extends FunnelBaseAggregationFunction<Integer> {

  public FunnelMaxStepAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELMAXSTEP;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(@Nullable PriorityQueue<FunnelStepEvent> stepEvents) {
    return findMaxStep(stepEvents).getStep();
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return Math.max(finalResult1, finalResult2);
  }
}
