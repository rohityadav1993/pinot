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
package org.apache.pinot.core.plan;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.StreamingSortedGroupByOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;


/// Plan node of the sorted streaming GROUP BY on a single segment, plus the eligibility rules that decide whether the
/// whole query may use it.
///
/// **Why eligibility is query-wide.** The combine merges per-segment key-ordered streams, so every segment must supply
/// one. [org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2] therefore evaluates [#getFallbackReason] once, from
/// metadata alone, before any plan node is built, and either engages the sorted path for every segment or for none.
///
/// **Extension points (ticket 04).** The per-segment rules live in [#getSegmentFallbackReason]; relaxing "sorted,
/// immutable, null-free" there (hash-then-sort for unsorted or consuming segments, a separate null group) is where
/// those segments would be admitted, together with a different per-segment operator built in [#run()]. The query-shape
/// rules live in [#getQueryFallbackReason].
///
/// Thread-safety: stateless apart from its inputs.
public class StreamingSortedGroupByPlanNode implements PlanNode {
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;

  public StreamingSortedGroupByPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    _segmentContext = segmentContext;
    _queryContext = queryContext;
  }

  @Override
  public Operator<GroupByResultsBlock> run() {
    FilterPlanNode filterPlanNode = new FilterPlanNode(_segmentContext, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();
    // No star-tree: it pre-aggregates and would not give document-id (key) order
    AggregationInfo aggregationInfo =
        AggregationFunctionUtils.buildAggregationInfoWithoutStarTree(_segmentContext, _queryContext,
            _queryContext.getAggregationFunctions(), filterOperator);
    return new StreamingSortedGroupByOperator(_queryContext, aggregationInfo,
        _segmentContext.getIndexSegment().getSegmentMetadata().getTotalDocs());
  }

  /// Returns why the sorted streaming GROUP BY cannot be used for this query over these segments, or `null` if it can.
  /// Reads query shape and segment metadata only; acquires no segment.
  @Nullable
  public static String getFallbackReason(List<SegmentContext> segmentContexts, QueryContext queryContext) {
    String reason = getQueryFallbackReason(queryContext);
    if (reason != null) {
      return reason;
    }
    String column = queryContext.getGroupByExpressions().get(0).getIdentifier();
    for (SegmentContext segmentContext : segmentContexts) {
      reason = getSegmentFallbackReason(segmentContext.getIndexSegment(), queryContext, column);
      if (reason != null) {
        return reason;
      }
    }
    return null;
  }

  /// Query-shape rules: a single plain column key, and nothing that trims or filters groups at the leaf.
  @Nullable
  static String getQueryFallbackReason(QueryContext queryContext) {
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (!QueryContextUtils.isAggregationQuery(queryContext) || groupByExpressions == null) {
      return "not a GROUP BY query";
    }
    if (groupByExpressions.size() != 1) {
      return "multiple group-by expressions";
    }
    if (groupByExpressions.get(0).getType() != ExpressionContext.Type.IDENTIFIER) {
      return "group-by expression is not a plain column";
    }
    if (queryContext.isGroupingSets()) {
      return "grouping sets";
    }
    if (queryContext.hasFilteredAggregations()) {
      return "filtered aggregations";
    }
    if (queryContext.getHavingFilter() != null) {
      return "HAVING";
    }
    // The existing path trims groups at the leaf in two cases, and the streaming path never trims, so both fall back:
    // - ORDER BY: segment and server group trimming.
    // - LIMIT without ORDER BY: the combine table stops accepting new groups once it holds LIMIT of them.
    // A LIMIT at or above numGroupsLimit cannot bind, because the existing path never returns more than
    // numGroupsLimit groups anyway. An MSE leaf only has a smaller LIMIT when the planner pushed one down.
    if (queryContext.getOrderByExpressions() != null) {
      return "ORDER BY may trim groups at the leaf";
    }
    if (queryContext.getLimit() < queryContext.getNumGroupsLimit()) {
      return "LIMIT " + queryContext.getLimit() + " may trim groups at the leaf";
    }
    if (queryContext.getLimit() == 0) {
      return "LIMIT 0";
    }
    return null;
  }

  /// Per-segment rules: an immutable segment whose metadata says the key column is sorted, single-value and
  /// dictionary-encoded, and (with null handling on) known to hold no nulls.
  @Nullable
  static String getSegmentFallbackReason(IndexSegment segment, QueryContext queryContext, String column) {
    if (segment instanceof MutableSegment) {
      return "consuming segment";
    }
    DataSource dataSource = segment.getDataSource(column, queryContext.getSchema());
    if (!dataSource.getDataSourceMetadata().isSorted()) {
      return "segment not sorted on " + column;
    }
    ColumnContext columnContext = ColumnContext.fromDataSource(dataSource);
    if (!columnContext.isSingleValue()) {
      return "multi-value group-by column";
    }
    if (!columnContext.isDictionaryEncoded()) {
      return "group-by column is not dictionary-encoded";
    }
    // A null row holds the default value, which sits wherever that value sorts, so it breaks the run order.
    if (queryContext.isNullHandlingEnabled() && !SelectionPlanNode.isColumnFlaggedNonNull(segment, column)) {
      return "null handling is enabled and " + column + " may hold nulls";
    }
    return null;
  }
}
