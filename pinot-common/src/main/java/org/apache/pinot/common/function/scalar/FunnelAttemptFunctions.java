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
package org.apache.pinot.common.function.scalar;

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.annotations.ScalarFunction;


/// Reads the attempts `FUNNEL_ATTEMPTS_ANY_ORDER` returns.
///
/// Each attempt is one opaque `LONG` holding its start timestamp and its depth (the number of leading funnel steps it
/// converted). [#funnelAttemptStart] and [#funnelAttemptDepth] are the only supported way to read it: queries must
/// not decode it with bit functions, so the layout can change. [#pack] is the encoder the aggregation function uses;
/// it is not a scalar function.
///
/// Stateless and thread-safe.
public class FunnelAttemptFunctions {
  private static final int DEPTH_BITS = 6;
  private static final long DEPTH_MASK = (1L << DEPTH_BITS) - 1;

  /// The deepest depth an attempt can hold, so the maximum number of funnel steps.
  public static final int MAX_ATTEMPT_DEPTH = (int) DEPTH_MASK;
  /// The range of attempt start timestamps that can be packed (57 signed bits, leaving one spare bit).
  public static final long MIN_ATTEMPT_START = -(1L << 56);
  public static final long MAX_ATTEMPT_START = (1L << 56) - 1;

  private FunnelAttemptFunctions() {
  }

  /// Packs one attempt. Throws [IllegalArgumentException] when the start or the depth does not fit.
  public static long pack(long start, int depth) {
    Preconditions.checkArgument(start >= MIN_ATTEMPT_START && start <= MAX_ATTEMPT_START,
        "Funnel attempt start %s is outside the supported range [%s, %s]", start, MIN_ATTEMPT_START,
        MAX_ATTEMPT_START);
    Preconditions.checkArgument(depth >= 1 && depth <= MAX_ATTEMPT_DEPTH,
        "Funnel attempt depth %s is outside the supported range [1, %s]", depth, MAX_ATTEMPT_DEPTH);
    return (start << DEPTH_BITS) | depth;
  }

  /// The start timestamp of an attempt returned by `FUNNEL_ATTEMPTS_ANY_ORDER`.
  @ScalarFunction
  public static long funnelAttemptStart(long attempt) {
    return attempt >> DEPTH_BITS;
  }

  /// The depth of an attempt returned by `FUNNEL_ATTEMPTS_ANY_ORDER`: the number of leading funnel steps it
  /// converted, from 1 to the number of steps.
  @ScalarFunction
  public static int funnelAttemptDepth(long attempt) {
    return (int) (attempt & DEPTH_MASK);
  }
}
