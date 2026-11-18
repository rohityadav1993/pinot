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

import org.apache.pinot.common.function.FunctionRegistry;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.MAX_ATTEMPT_DEPTH;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.MAX_ATTEMPT_START;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.MIN_ATTEMPT_START;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.funnelAttemptDepth;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.funnelAttemptStart;
import static org.apache.pinot.common.function.scalar.FunnelAttemptFunctions.pack;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class FunnelAttemptFunctionsTest {

  @DataProvider
  public Object[][] attempts() {
    return new Object[][]{
        {0L, 1}, {1L, 2}, {-1L, 1}, {1_700_000_000_000L, 3}, {-1_700_000_000_000L, MAX_ATTEMPT_DEPTH},
        {MAX_ATTEMPT_START, 1}, {MAX_ATTEMPT_START, MAX_ATTEMPT_DEPTH},
        {MIN_ATTEMPT_START, 1}, {MIN_ATTEMPT_START, MAX_ATTEMPT_DEPTH}
    };
  }

  @Test(dataProvider = "attempts")
  public void testRoundTrip(long start, int depth) {
    long attempt = pack(start, depth);
    assertEquals(funnelAttemptStart(attempt), start);
    assertEquals(funnelAttemptDepth(attempt), depth);
  }

  @Test
  public void testLimits() {
    assertEquals(MAX_ATTEMPT_DEPTH, 63);
    assertEquals(MAX_ATTEMPT_START, (1L << 56) - 1);
    assertEquals(MIN_ATTEMPT_START, -(1L << 56));
  }

  /// `FUNNEL_ATTEMPTS_ANY_ORDER` sorts its result by start by sorting the packed values.
  @Test
  public void testPackedOrderIsStartOrder() {
    assertTrue(pack(-5, MAX_ATTEMPT_DEPTH) < pack(-4, 1));
    assertTrue(pack(-1, MAX_ATTEMPT_DEPTH) < pack(0, 1));
    assertTrue(pack(0, MAX_ATTEMPT_DEPTH) < pack(1, 1));
    assertTrue(pack(MIN_ATTEMPT_START, MAX_ATTEMPT_DEPTH) < pack(MAX_ATTEMPT_START, 1));
  }

  @Test
  public void testRejectsValuesThatDoNotFit() {
    assertThrows(IllegalArgumentException.class, () -> pack(MAX_ATTEMPT_START + 1, 1));
    assertThrows(IllegalArgumentException.class, () -> pack(MIN_ATTEMPT_START - 1, 1));
    assertThrows(IllegalArgumentException.class, () -> pack(0, 0));
    assertThrows(IllegalArgumentException.class, () -> pack(0, MAX_ATTEMPT_DEPTH + 1));
  }

  @Test
  public void testRegisteredAsScalarFunctions() {
    assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize("funnelAttemptStart")));
    assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize("funnelAttemptDepth")));
  }
}
