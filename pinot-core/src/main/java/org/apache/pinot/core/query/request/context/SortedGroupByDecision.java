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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedGroupByMode;


/// The plan-time outcome of a requested sorted streaming GROUP BY: which mode was asked for, whether the plan maker
/// engaged it, and if not why, plus the segment counts behind the decision.
///
/// It is created once by the plan maker, before any plan node is built, and never changes afterwards. The operators
/// read [#isEngaged()] and [#getBlockSize()]; the server reports [#toMetadata()] to the caller. A query that never
/// requested the sorted GROUP BY has no decision at all (see [QueryContext#getSortedGroupByDecision()]).
///
/// **Block size.** The number of finished groups per emitted block is a constant, [#BLOCK_SIZE], with no query option
/// or config. Tests may shrink it with [#withBlockSize(int)] to exercise block boundaries.
///
/// Thread-safety: immutable. It is published through [QueryContext] on the query thread that plans the query and read
/// by the operators and worker threads started after that, which the executor hand-off makes visible.
public final class SortedGroupByDecision {
  /// Maximum number of finished groups per block emitted by the sorted streaming GROUP BY.
  public static final int BLOCK_SIZE = 10_000;

  /// Why a requested sorted streaming GROUP BY was not engaged. The text of the decision carries the detail.
  public enum FallbackReason {
    /// The query shape rules it out (keys, ORDER BY, LIMIT, HAVING, grouping sets, filtered aggregations).
    QUERY_SHAPE,
    /// `AUTO`, and fewer than the threshold ratio of the segments are sorted on the key.
    TOO_FEW_SORTED_SEGMENTS
  }

  private final SortedGroupByMode _requestedMode;
  private final boolean _engaged;
  @Nullable
  private final FallbackReason _fallbackReason;
  /// Human-readable detail of the fallback reason, `null` when engaged
  @Nullable
  private final String _fallbackDetail;
  /// Segment counts behind the decision; -1 when the query shape ruled the request out before segments were looked at
  private final int _numSegments;
  private final int _numSortedSegments;
  @Nullable
  private final String _description;
  private final int _blockSize;

  private SortedGroupByDecision(SortedGroupByMode requestedMode, boolean engaged,
      @Nullable FallbackReason fallbackReason, @Nullable String fallbackDetail, int numSegments, int numSortedSegments,
      @Nullable String description, int blockSize) {
    _requestedMode = requestedMode;
    _engaged = engaged;
    _fallbackReason = fallbackReason;
    _fallbackDetail = fallbackDetail;
    _numSegments = numSegments;
    _numSortedSegments = numSortedSegments;
    _description = description;
    _blockSize = blockSize;
  }

  /// The request was ruled out by the query shape, before any segment was looked at.
  public static SortedGroupByDecision queryShapeFallback(SortedGroupByMode requestedMode, String detail) {
    return new SortedGroupByDecision(requestedMode, false, FallbackReason.QUERY_SHAPE, detail, -1, -1, null,
        BLOCK_SIZE);
  }

  /// The query shape qualifies; decides from the segment counts. `ON` engages; `AUTO` engages when
  /// `numSortedSegments / numSegments` reaches `minSortedRatio`.
  public static SortedGroupByDecision fromSegmentCounts(SortedGroupByMode requestedMode, int numSegments,
      int numSortedSegments, double minSortedRatio) {
    assert requestedMode != SortedGroupByMode.OFF;
    double sortedRatio = numSegments == 0 ? 0 : (double) numSortedSegments / numSegments;
    String description = String.format("%s: %d/%d segments sorted (ratio %.2f", requestedMode, numSortedSegments,
        numSegments, sortedRatio);
    if (requestedMode == SortedGroupByMode.AUTO) {
      boolean engaged = sortedRatio >= minSortedRatio;
      description += String.format(" %s threshold %.2f)", engaged ? ">=" : "<", minSortedRatio);
      return new SortedGroupByDecision(requestedMode, engaged, engaged ? null : FallbackReason.TOO_FEW_SORTED_SEGMENTS,
          engaged ? null : description, numSegments, numSortedSegments, description, BLOCK_SIZE);
    }
    return new SortedGroupByDecision(requestedMode, true, null, null, numSegments, numSortedSegments,
        description + ")", BLOCK_SIZE);
  }

  /// Same decision with another block size, for tests that need small blocks.
  @VisibleForTesting
  public SortedGroupByDecision withBlockSize(int blockSize) {
    return new SortedGroupByDecision(_requestedMode, _engaged, _fallbackReason, _fallbackDetail, _numSegments,
        _numSortedSegments, _description, blockSize);
  }

  public SortedGroupByMode getRequestedMode() {
    return _requestedMode;
  }

  /// Whether the plan uses the sorted streaming GROUP BY.
  public boolean isEngaged() {
    return _engaged;
  }

  @Nullable
  public FallbackReason getFallbackReason() {
    return _fallbackReason;
  }

  /// Why the request was not engaged, in words, or `null` when it was engaged.
  @Nullable
  public String getFallbackDetail() {
    return _fallbackDetail;
  }

  /// Number of segments considered, or -1 when the query shape ruled the request out first.
  public int getNumSegments() {
    return _numSegments;
  }

  /// Number of segments that can be read in document order, or -1 when the query shape ruled the request out first.
  public int getNumSortedSegments() {
    return _numSortedSegments;
  }

  /// The segment counts and ratio behind the decision, e.g. `AUTO: 3/6 segments sorted (ratio 0.50 < threshold 0.80)`,
  /// or `null` when the query shape ruled the request out first.
  @Nullable
  public String getDescription() {
    return _description;
  }

  public int getBlockSize() {
    return _blockSize;
  }

  /// The execution metadata the server reports for this decision: `sortedGroupByEngaged` when engaged, otherwise
  /// `sortedGroupByFallbackReason` with the detail. Keys and values are those of
  /// [MetadataKey#SORTED_GROUP_BY_ENGAGED] and [MetadataKey#SORTED_GROUP_BY_FALLBACK_REASON].
  public Map<String, String> toMetadata() {
    if (_engaged) {
      return Map.of(MetadataKey.SORTED_GROUP_BY_ENGAGED.getName(), "true");
    }
    assert _fallbackDetail != null;
    return Map.of(MetadataKey.SORTED_GROUP_BY_FALLBACK_REASON.getName(), _fallbackDetail);
  }
}
