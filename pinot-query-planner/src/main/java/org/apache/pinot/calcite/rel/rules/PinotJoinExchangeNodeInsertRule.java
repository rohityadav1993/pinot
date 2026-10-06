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
package org.apache.pinot.calcite.rel.rules;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotKWayMergeSortExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.query.context.PlannerContext;


/// Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
public class PinotJoinExchangeNodeInsertRule extends RelOptRule {
  public static final PinotJoinExchangeNodeInsertRule INSTANCE =
      new PinotJoinExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(Join.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    if (PinotRuleUtils.isExchange(join.getLeft()) || PinotRuleUtils.isExchange(join.getRight())) {
      return false;
    }
    // When the k-way merge is unavailable, the sorted merge join input is a Sort over the exchange (see
    // createSortExchangeForMergeJoin). That exact shape on both sides means the join has already been processed;
    // without this guard the rule would re-fire on its own output forever. The check is exact (collation, distribution
    // keys and unbounded fetch), so a user subquery with ORDER BY ... LIMIT 2147483647 is not mistaken for it.
    if (PinotHintOptions.JoinHintOptions.useSortedMergeJoinStrategy(join)) {
      JoinInfo joinInfo = join.analyzeCondition();
      return !(isMergeJoinFallbackInput(PinotRuleUtils.unboxRel(join.getLeft()), joinInfo.leftKeys)
          && isMergeJoinFallbackInput(PinotRuleUtils.unboxRel(join.getRight()), joinInfo.rightKeys));
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode left = PinotRuleUtils.unboxRel(join.getInput(0));
    RelNode right = PinotRuleUtils.unboxRel(join.getInput(1));
    JoinInfo joinInfo = join.analyzeCondition();
    Map<String, String> joinHintOptions = PinotHintOptions.JoinHintOptions.getJoinHintOptions(join);
    PinotHintOptions.DistributionType leftDistributionType;
    PinotHintOptions.DistributionType rightDistributionType;
    if (joinHintOptions != null) {
      leftDistributionType = PinotHintOptions.JoinHintOptions.getLeftDistributionType(joinHintOptions);
      rightDistributionType = PinotHintOptions.JoinHintOptions.getRightDistributionType(joinHintOptions);
    } else {
      leftDistributionType = null;
      rightDistributionType = null;
    }
    RelNode newLeft;
    RelNode newRight;
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      if (leftDistributionType == null) {
        // By default, use local distribution for the left side
        leftDistributionType = PinotHintOptions.DistributionType.LOCAL;
      }
      newLeft = createExchangeForLookupJoin(leftDistributionType, joinInfo.leftKeys, left);
      Preconditions.checkArgument(rightDistributionType == null,
          "Right distribution type hint is not supported for lookup join");
      newRight = right;
    } else if (PinotHintOptions.JoinHintOptions.useSortedMergeJoinStrategy(join)) {
      Preconditions.checkArgument(!joinInfo.leftKeys.isEmpty(), "Sorted merge join requires equi-join keys");
      PinotHintOptions.JoinHintOptions.checkSortedMergeJoinType(join.getJoinType());
      // The merge join needs both inputs hash-distributed on the join keys so that matching keys meet on the same
      // worker. A distribution type hint would silently break that (broadcast in particular), so reject it rather
      // than ignore it, mirroring how the lookup join branch rejects a right distribution type hint.
      Preconditions.checkArgument(leftDistributionType == null && rightDistributionType == null,
          "Distribution type hints are not supported for sorted merge join");
      // Force pre-partitioned exchange when colocated join hint is provided
      Boolean prePartitioned = PinotHintOptions.JoinHintOptions.isColocatedByJoinKeys(join);
      boolean mergeSupported = isKWayMergeSupported(call);
      newLeft = createSortExchangeForMergeJoin(joinInfo.leftKeys, left, prePartitioned, mergeSupported);
      newRight = createSortExchangeForMergeJoin(joinInfo.rightKeys, right, prePartitioned, mergeSupported);
    } else if (joinInfo.leftKeys.isEmpty() && join.getJoinType() == JoinRelType.FULL
        && leftDistributionType == null && rightDistributionType == null) {
      // FULL OUTER JOIN with no equi keys: use hash with empty key to explicitly route all data to one destination.
      // DispatchablePlanVisitor sets requireSingleton on the join stage so WorkerManager picks a single random
      // worker, avoiding hotspots.
      newLeft = PinotLogicalExchange.create(left, RelDistributions.hash(List.of()));
      newRight = PinotLogicalExchange.create(right, RelDistributions.hash(List.of()));
    } else {
      // Hash join
      // Force pre-partitioned exchange when colocated join hint is provided
      Boolean prePartitioned = PinotHintOptions.JoinHintOptions.isColocatedByJoinKeys(join);
      // TODO: Validate if the configured distribution types are valid
      if (leftDistributionType == null) {
        leftDistributionType = inferLeftDistributionType(joinInfo, join.getJoinType());
      }
      newLeft = createExchangeForHashJoin(leftDistributionType, joinInfo.leftKeys, left, prePartitioned);
      if (rightDistributionType == null) {
        rightDistributionType = inferRightDistributionType(joinInfo, join.getJoinType());
      }
      newRight = createExchangeForHashJoin(rightDistributionType, joinInfo.rightKeys, right, prePartitioned);
    }

    // TODO: Consider creating different JOIN Rel for each join strategy
    if (join instanceof LogicalAsofJoin) {
      // Note that we don't use the MATCH_CONDITION in an ASOF JOIN to determine the distribution, only the join keys
      // in the ON clause of the ASOF JOIN.
      call.transformTo(((LogicalAsofJoin) join).copy(join.getTraitSet(), List.of(newLeft, newRight)));
    } else {
      call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(),
          join.isSemiJoinDone()));
    }
  }

  private static PinotLogicalExchange createExchangeForLookupJoin(PinotHintOptions.DistributionType distributionType,
      List<Integer> keys, RelNode child) {
    switch (distributionType) {
      case LOCAL:
        // NOTE: We use SINGLETON to represent local distribution. Add keys to the exchange because we might want to
        //       switch it to HASH distribution to increase parallelism. See MailboxAssignmentVisitor for details.
        return PinotLogicalExchange.create(child, RelDistributions.SINGLETON, keys, null);
      case HASH:
        Preconditions.checkArgument(!keys.isEmpty(), "Hash distribution requires join keys");
        return PinotLogicalExchange.create(child, RelDistributions.hash(keys));
      case RANDOM:
        return PinotLogicalExchange.create(child, RelDistributions.RANDOM_DISTRIBUTED);
      default:
        throw new IllegalArgumentException("Unsupported distribution type: " + distributionType + " for lookup join");
    }
  }

  /// For non-equi RIGHT JOINs (no equi keys), the default RANDOM(left)+BROADCAST(right) is incorrect because each
  /// worker independently tracks matched right rows via a local BitSet. Workers that receive only a subset of left rows
  /// will incorrectly emit right rows as unmatched. Invert to BROADCAST(left)+RANDOM(right) so each worker has the
  /// full left table and can correctly determine which of its local right rows are unmatched.
  private static PinotHintOptions.DistributionType inferLeftDistributionType(JoinInfo joinInfo,
      JoinRelType joinType) {
    if (!joinInfo.leftKeys.isEmpty()) {
      return PinotHintOptions.DistributionType.HASH;
    }
    if (joinType == JoinRelType.RIGHT) {
      return PinotHintOptions.DistributionType.BROADCAST;
    }
    return PinotHintOptions.DistributionType.RANDOM;
  }

  private static PinotHintOptions.DistributionType inferRightDistributionType(JoinInfo joinInfo,
      JoinRelType joinType) {
    if (!joinInfo.rightKeys.isEmpty()) {
      return PinotHintOptions.DistributionType.HASH;
    }
    if (joinType == JoinRelType.RIGHT) {
      return PinotHintOptions.DistributionType.RANDOM;
    }
    return PinotHintOptions.DistributionType.BROADCAST;
  }

  private static PinotLogicalExchange createExchangeForHashJoin(PinotHintOptions.DistributionType distributionType,
      List<Integer> keys, RelNode child, @Nullable Boolean prePartitioned) {
    switch (distributionType) {
      case LOCAL:
        // NOTE: We use SINGLETON to represent local distribution. Add keys to the exchange because we might want to
        //       switch it to HASH distribution to increase parallelism. See MailboxAssignmentVisitor for details.
        return PinotLogicalExchange.create(child, RelDistributions.SINGLETON, keys, prePartitioned);
      case HASH:
        Preconditions.checkArgument(!keys.isEmpty(), "Hash distribution requires join keys");
        return PinotLogicalExchange.create(child, RelDistributions.hash(keys), prePartitioned);
      case BROADCAST:
        return PinotLogicalExchange.create(child, RelDistributions.BROADCAST_DISTRIBUTED, prePartitioned);
      case RANDOM:
        return PinotLogicalExchange.create(child, RelDistributions.RANDOM_DISTRIBUTED, prePartitioned);
      default:
        throw new IllegalArgumentException("Unsupported distribution type: " + distributionType + " for hash join");
    }
  }

  /// Builds the input of a sorted merge join: hash-distributed on the join keys and sorted on them (ASC NULLS LAST)
  /// within each worker, which is the contract of the sorted merge join operator.
  ///
  /// When the broker verified that every worker understands the merge receive node, each sender sorts its own rows and
  /// the receiver merges the sorted streams, so the join input is never fully re-sorted. Otherwise (rolling upgrade or
  /// rollback), a plain exchange is followed by a full sort on the receiving worker.
  ///
  /// The sort is unbounded: a missing fetch would let the sort operator apply the broker response limit and silently
  /// drop rows, so it carries an explicit `Integer.MAX_VALUE` fetch.
  private static RelNode createSortExchangeForMergeJoin(List<Integer> joinKeys, RelNode input,
      @Nullable Boolean prePartitioned, boolean mergeSupported) {
    RelCollation collation = getJoinKeyCollation(joinKeys);
    RelDistribution distribution = RelDistributions.hash(joinKeys);
    RexNode unbounded = input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(Integer.MAX_VALUE));
    if (mergeSupported) {
      RelNode sortedInput = LogicalSort.create(input, collation, null, unbounded);
      return PinotKWayMergeSortExchange.create(sortedInput, distribution, collation, prePartitioned);
    }
    PinotLogicalExchange exchange = PinotLogicalExchange.create(input, distribution, prePartitioned);
    return LogicalSort.create(exchange, collation, null, unbounded);
  }

  /// ASC NULLS LAST collation on the distinct join key columns, in first-occurrence order. `ON a.x = b.y AND a.x = b.z`
  /// yields left keys `[x, x]`, but a collation lists each field once. Ordering by the distinct columns is equivalent:
  /// the operator compares tuples over all key columns in order, and a repeated column never changes the lexicographic
  /// order (a tuple equal on the first occurrence is equal on the repeats).
  private static RelCollation getJoinKeyCollation(List<Integer> joinKeys) {
    List<RelFieldCollation> fieldCollations = new ArrayList<>(joinKeys.size());
    for (int key : new LinkedHashSet<>(joinKeys)) {
      fieldCollations.add(new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
          RelFieldCollation.NullDirection.LAST));
    }
    return RelCollations.of(fieldCollations);
  }

  /// Whether the rel is exactly the gate-off input of [#createSortExchangeForMergeJoin]: an unbounded Sort (no offset,
  /// `Integer.MAX_VALUE` fetch) on the join key collation over a plain [PinotLogicalExchange] hash distributed on the
  /// join keys. User plans never have that shape: the sort exchange inserted under a user Sort is a
  /// [org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange] hashed on no keys.
  private static boolean isMergeJoinFallbackInput(RelNode rel, List<Integer> joinKeys) {
    if (!(rel instanceof Sort)) {
      return false;
    }
    Sort sort = (Sort) rel;
    if (sort.offset != null || sort.fetch == null || RexLiteral.intValue(sort.fetch) != Integer.MAX_VALUE
        || !sort.getCollation().equals(RelCollationTraitDef.INSTANCE.canonize(getJoinKeyCollation(joinKeys)))) {
      return false;
    }
    RelNode input = PinotRuleUtils.unboxRel(sort.getInput());
    return input instanceof PinotLogicalExchange && ((PinotLogicalExchange) input).getDistribution()
        .equals(RelDistributions.hash(joinKeys));
  }

  /// Same lookup as the window rule: the broker puts the version-gate result in the [PlannerContext]. Defaults to
  /// `false`, the safe plain exchange and sort shape, when no context is available.
  private static boolean isKWayMergeSupported(RelOptRuleCall call) {
    RelOptPlanner planner = call.getPlanner();
    if (planner != null) {
      Context context = planner.getContext();
      if (context != null) {
        PlannerContext plannerContext = context.unwrap(PlannerContext.class);
        if (plannerContext != null) {
          return plannerContext.getEnvConfig().isKWayMergeSupported();
        }
      }
    }
    return false;
  }
}
