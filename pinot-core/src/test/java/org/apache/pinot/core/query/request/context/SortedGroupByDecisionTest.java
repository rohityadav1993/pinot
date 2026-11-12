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
package org.apache.pinot.core.query.request.context;

import java.util.Map;
import org.apache.pinot.core.query.request.context.SortedGroupByDecision.FallbackReason;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedGroupByMode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class SortedGroupByDecisionTest {

  @Test
  public void testOnEngagesRegardlessOfTheSortedRatio() {
    SortedGroupByDecision decision = SortedGroupByDecision.fromSegmentCounts(SortedGroupByMode.ON, 4, 1, 0.8);
    assertTrue(decision.isEngaged());
    assertNull(decision.getFallbackReason());
    assertNull(decision.getFallbackDetail());
    assertEquals(decision.getNumSegments(), 4);
    assertEquals(decision.getNumSortedSegments(), 1);
    assertEquals(decision.getDescription(), "ON: 1/4 segments sorted (ratio 0.25)");
    assertEquals(decision.toMetadata(), Map.of("sortedGroupByEngaged", "true"));
    assertEquals(decision.getBlockSize(), SortedGroupByDecision.BLOCK_SIZE);
  }

  @Test
  public void testAutoComparesTheRatioWithTheThreshold() {
    SortedGroupByDecision engaged = SortedGroupByDecision.fromSegmentCounts(SortedGroupByMode.AUTO, 5, 4, 0.8);
    assertTrue(engaged.isEngaged());
    assertEquals(engaged.getDescription(), "AUTO: 4/5 segments sorted (ratio 0.80 >= threshold 0.80)");

    SortedGroupByDecision notEngaged = SortedGroupByDecision.fromSegmentCounts(SortedGroupByMode.AUTO, 6, 3, 0.8);
    assertFalse(notEngaged.isEngaged());
    assertEquals(notEngaged.getFallbackReason(), FallbackReason.TOO_FEW_SORTED_SEGMENTS);
    assertEquals(notEngaged.getFallbackDetail(), "AUTO: 3/6 segments sorted (ratio 0.50 < threshold 0.80)");
    assertEquals(notEngaged.getDescription(), notEngaged.getFallbackDetail());
    assertEquals(notEngaged.toMetadata(), Map.of("sortedGroupByFallbackReason", notEngaged.getFallbackDetail()));
  }

  @Test
  public void testAutoWithoutSegmentsDoesNotEngage() {
    assertFalse(SortedGroupByDecision.fromSegmentCounts(SortedGroupByMode.AUTO, 0, 0, 0.8).isEngaged());
  }

  @Test
  public void testQueryShapeFallbackHasNoSegmentCounts() {
    SortedGroupByDecision decision = SortedGroupByDecision.queryShapeFallback(SortedGroupByMode.ON, "HAVING");
    assertFalse(decision.isEngaged());
    assertEquals(decision.getRequestedMode(), SortedGroupByMode.ON);
    assertEquals(decision.getFallbackReason(), FallbackReason.QUERY_SHAPE);
    assertEquals(decision.getFallbackDetail(), "HAVING");
    assertEquals(decision.getNumSegments(), -1);
    assertNull(decision.getDescription());
    assertEquals(decision.toMetadata(), Map.of("sortedGroupByFallbackReason", "HAVING"));
  }

  @Test
  public void testWithBlockSizeKeepsTheDecision() {
    SortedGroupByDecision decision = SortedGroupByDecision.fromSegmentCounts(SortedGroupByMode.ON, 2, 2, 0.8);
    SortedGroupByDecision small = decision.withBlockSize(3);
    assertEquals(small.getBlockSize(), 3);
    assertEquals(decision.getBlockSize(), SortedGroupByDecision.BLOCK_SIZE);
    assertTrue(small.isEngaged());
    assertEquals(small.getDescription(), decision.getDescription());
  }
}
