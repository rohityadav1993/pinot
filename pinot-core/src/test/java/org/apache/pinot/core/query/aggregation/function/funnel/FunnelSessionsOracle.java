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
import java.util.List;


/// Test oracle for `FUNNEL_ATTEMPTS_ANY_ORDER`: a line-for-line Java port of `funnel_sessions`, the reference
/// algorithm of the Mirador requirement ("POC Funnel count - sorted join", tab "Funnel count sessionisation"), which
/// is the normative definition of an attempt (the doc says "session").
///
/// It shares no code with the function under test on purpose. Keep it a literal port: do not optimize it.
///
/// Stateless and thread-safe.
public final class FunnelSessionsOracle {
  private FunnelSessionsOracle() {
  }

  /// One emitted session: its start and, per step, whether the step converted.
  public static final class Session {
    public final long _start;
    public final boolean[] _converted;

    Session(long start, boolean[] converted) {
      _start = start;
      _converted = converted;
    }

    /// The number of leading converted steps. Conversions always form a prefix, so this is the attempt depth.
    public int depth() {
      int depth = 0;
      while (depth < _converted.length && _converted[depth]) {
        depth++;
      }
      return depth;
    }
  }

  /// `rowMatches` holds `{timestamp, step}` pairs with 1-based steps, sorted by timestamp (the doc's precondition).
  public static List<Session> funnelSessions(long cw, int stepCount, List<long[]> rowMatches) {
    // Assume row_matches is sorted by timestamp
    List<Session> sessions = new ArrayList<>();
    Long start = null;
    boolean[] seen = new boolean[stepCount];

    for (long[] rowMatch : rowMatches) {
      long timestamp = rowMatch[0];
      int step = (int) rowMatch[1];
      if (start != null && timestamp >= start + cw) {
        sessions.add(emitSession(start, seen, stepCount));
        start = null;
      }

      if (start == null) {
        if (step != 1) {
          continue;
        }

        start = timestamp;
        seen = new boolean[stepCount];
      }

      seen[step - 1] = true;
    }

    if (start != null) {
      sessions.add(emitSession(start, seen, stepCount));
    }

    return sessions;
  }

  private static Session emitSession(long start, boolean[] seen, int stepCount) {
    boolean[] converted = new boolean[stepCount];
    boolean complete = true;

    for (int i = 0; i < stepCount; i++) {
      complete = complete && seen[i];
      converted[i] = complete;
    }

    return new Session(start, converted);
  }
}
