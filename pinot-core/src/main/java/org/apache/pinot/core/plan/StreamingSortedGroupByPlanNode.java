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
import org.apache.pinot.core.operator.query.HashSortedGroupByOperator;
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
/// **Two kinds of per-segment stream.** A segment that is *sorted-streamable* (immutable, physically sorted on the key,
/// single-value, dictionary-encoded; see [#isSortedStreamable]) gets [StreamingSortedGroupByOperator], which reads it
/// in document order and never builds a hash table. Every other segment, unsorted, consuming or without a dictionary,
/// gets [HashSortedGroupByOperator], which runs the existing hash aggregation and sorts the resulting groups by key.
/// So no segment disqualifies the query; segments only differ in cost, which is what `AUTO` weighs through
/// [#countSortedStreamable]. Null keys are handled by both.
///
/// **Extension points.** The query-shape rules live in [#getQueryFallbackReason]; a composite key needs those relaxed
/// and the per-segment operators extended.
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
    String column = _queryContext.getGroupByExpressions().get(0).getIdentifier();
    IndexSegment indexSegment = _segmentContext.getIndexSegment();
    if (!isSortedStreamable(indexSegment, _queryContext, column)) {
      return new HashSortedGroupByOperator(_queryContext, new GroupByPlanNode(_segmentContext, _queryContext).run(),
          indexSegment);
    }
    FilterPlanNode filterPlanNode = new FilterPlanNode(_segmentContext, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();
    // No star-tree: it pre-aggregates and would not give document-id (key) order
    AggregationInfo aggregationInfo =
        AggregationFunctionUtils.buildAggregationInfoWithoutStarTree(_segmentContext, _queryContext,
            _queryContext.getAggregationFunctions(), filterOperator);
    return new StreamingSortedGroupByOperator(_queryContext, aggregationInfo, indexSegment,
        indexSegment.getSegmentMetadata().getTotalDocs());
  }

  /// Returns why the sorted streaming GROUP BY cannot be used for this query, or `null` if it can. Reads the query
  /// shape only: every segment can join the merge, through the sorted or the hash-then-sort operator.
  @Nullable
  public static String getFallbackReason(QueryContext queryContext) {
    return getQueryFallbackReason(queryContext);
  }

  /// Number of `segmentContexts` that are [sorted-streamable][#isSortedStreamable] on the group-by column. Metadata
  /// only; acquires no segment.
  public static int countSortedStreamable(List<SegmentContext> segmentContexts, QueryContext queryContext) {
    String column = queryContext.getGroupByExpressions().get(0).getIdentifier();
    int numSorted = 0;
    for (SegmentContext segmentContext : segmentContexts) {
      if (isSortedStreamable(segmentContext.getIndexSegment(), queryContext, column)) {
        numSorted++;
      }
    }
    return numSorted;
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

  /// Whether `segment` can feed the merge straight from its document order: immutable, with metadata saying the key
  /// column is sorted, single-value and dictionary-encoded. Null keys do not matter: with null handling on the operator
  /// splits them out by the null bitmap. Reads metadata only.
  public static boolean isSortedStreamable(IndexSegment segment, QueryContext queryContext, String column) {
    if (segment instanceof MutableSegment) {
      return false;
    }
    DataSource dataSource = segment.getDataSource(column, queryContext.getSchema());
    if (!dataSource.getDataSourceMetadata().isSorted()) {
      return false;
    }
    ColumnContext columnContext = ColumnContext.fromDataSource(dataSource);
    return columnContext.isSingleValue() && columnContext.isDictionaryEncoded();
  }
}
