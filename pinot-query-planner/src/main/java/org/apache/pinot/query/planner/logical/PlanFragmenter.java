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
package org.apache.pinot.query.planner.logical;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


/**
 * PlanFragmenter is an implementation of {@link PlanNodeVisitor} to fragment a {@link SubPlan} into multiple
 * {@link PlanFragment}s.
 *
 * The fragmenting process is as follows:
 * 1. Traverse the plan tree in a depth-first manner;
 * 2. For each node, if it is a PlanFragment splittable ExchangeNode, split it into {@link MailboxReceiveNode} and
 * {@link MailboxSendNode} pair;
 * 3. Assign current PlanFragment ID to {@link MailboxReceiveNode};
 * 4. Increment current PlanFragment ID by one and assign it to the {@link MailboxSendNode}.
 */
public class PlanFragmenter implements PlanNodeVisitor<PlanNode, PlanFragmenter.Context>,
                                       EquivalentStagesReplacer.OnSubstitution {
  private final Int2ObjectOpenHashMap<PlanFragment> _planFragmentMap = new Int2ObjectOpenHashMap<>();
  private final Int2ObjectOpenHashMap<IntList> _childPlanFragmentIdsMap = new Int2ObjectOpenHashMap<>();

  private final IdentityHashMap<MailboxSendNode, ExchangeNode> _mailboxSendToExchangeNodeMap = new IdentityHashMap<>();
  private final IdentityHashMap<MailboxReceiveNode, ExchangeNode> _mailboxReceiveToExchangeNodeMap =
      new IdentityHashMap<>();

  // ROOT PlanFragment ID is 0, current PlanFragment ID starts with 1, next PlanFragment ID starts with 2.
  private int _nextPlanFragmentId = 2;

  public Context createContext() {
    // ROOT PlanFragment ID is 0, current PlanFragment ID starts with 1.
    return new Context(1);
  }

  public Int2ObjectOpenHashMap<PlanFragment> getPlanFragmentMap() {
    return _planFragmentMap;
  }

  public Int2ObjectOpenHashMap<IntList> getChildPlanFragmentIdsMap() {
    return _childPlanFragmentIdsMap;
  }

  private PlanNode process(PlanNode node, Context context) {
    node.setStageId(context._currentPlanFragmentId);
    node.getInputs().replaceAll(planNode -> planNode.visit(this, context));
    return node;
  }

  @Override
  public void onSubstitution(int receiver, int oldSender, int newSender) {
    // Change the sender of the receiver to the new sender
    IntList senders = _childPlanFragmentIdsMap.get(receiver);
    senders.rem(oldSender);
    if (!senders.contains(newSender)) {
      senders.add(newSender);
    }

    // Remove the old sender and its children from the plan fragment map
    _planFragmentMap.remove(oldSender);

    IntList fragmentsToRemove = new IntArrayList();
    fragmentsToRemove.add(oldSender);
    while (!fragmentsToRemove.isEmpty()) {
      int orphan = fragmentsToRemove.removeInt(fragmentsToRemove.size() - 1);
      IntList children = _childPlanFragmentIdsMap.remove(orphan);
      if (children != null) {
        fragmentsToRemove.addAll(children);
      }
      _planFragmentMap.remove(orphan);
    }
  }

  @Override
  public PlanNode visitAggregate(AggregateNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitFilter(FilterNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitJoin(JoinNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitMailboxReceive(MailboxReceiveNode node, Context context) {
    throw new UnsupportedOperationException("MailboxReceiveNode should not be visited by PlanNodeFragmenter");
  }

  @Override
  public PlanNode visitMailboxSend(MailboxSendNode node, Context context) {
    throw new UnsupportedOperationException("MailboxSendNode should not be visited by PlanNodeFragmenter");
  }

  @Override
  public PlanNode visitProject(ProjectNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitSort(SortNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitTableScan(TableScanNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitValue(ValueNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitWindow(WindowNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitSetOp(SetOpNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitExchange(ExchangeNode node, Context context) {
    if (!isPlanFragmentSplitter(node)) {
      return process(node, context);
    }

    // Split the ExchangeNode to a MailboxReceiveNode and a MailboxSendNode, where MailboxReceiveNode is the leave node
    // of the current PlanFragment, and MailboxSendNode is the root node of the next PlanFragment.
    int receiverPlanFragmentId = context._currentPlanFragmentId;
    int senderPlanFragmentId = _nextPlanFragmentId++;
    _childPlanFragmentIdsMap.computeIfAbsent(receiverPlanFragmentId, k -> new IntArrayList()).add(senderPlanFragmentId);

    // Create a new context for the next PlanFragment with MailboxSendNode as the root node.
    PlanNode nextPlanFragmentRoot = node.getInputs().get(0).visit(this, new Context(senderPlanFragmentId));
    PinotRelExchangeType exchangeType = node.getExchangeType();
    RelDistribution.Type distributionType = node.getDistributionType();
    List<Integer> keys = node.getKeys();
    MailboxSendNode mailboxSendNode =
        new MailboxSendNode(senderPlanFragmentId, nextPlanFragmentRoot.getDataSchema(), List.of(nextPlanFragmentRoot),
            receiverPlanFragmentId, exchangeType, distributionType, keys, node.isPrePartitioned(), node.getCollations(),
            node.isSortOnSender(), node.getHashFunction());
    _planFragmentMap.put(senderPlanFragmentId,
        new PlanFragment(senderPlanFragmentId, mailboxSendNode, new ArrayList<>()));
    _mailboxSendToExchangeNodeMap.put(mailboxSendNode, node);

    // Return the MailboxReceiveNode as the leave node of the current PlanFragment.
    MailboxReceiveNode mailboxReceiveNode =
        new MailboxReceiveNode(receiverPlanFragmentId, nextPlanFragmentRoot.getDataSchema(),
            senderPlanFragmentId, exchangeType, distributionType, keys, node.getCollations(), node.isSortOnReceiver(),
            node.isSortOnSender(), mailboxSendNode);
    _mailboxReceiveToExchangeNodeMap.put(mailboxReceiveNode, node);
    return mailboxReceiveNode;
  }

  @Override
  public PlanNode visitExplained(ExplainedNode node, Context context) {
    throw new UnsupportedOperationException("ExplainNode should not be visited by PlanNodeFragmenter");
  }

  public IdentityHashMap<MailboxSendNode, ExchangeNode> getMailboxSendToExchangeNodeMap() {
    return _mailboxSendToExchangeNodeMap;
  }

  public IdentityHashMap<MailboxReceiveNode, ExchangeNode> getMailboxReceiveToExchangeNodeMap() {
    return _mailboxReceiveToExchangeNodeMap;
  }

  private boolean isPlanFragmentSplitter(PlanNode node) {
    return ((ExchangeNode) node).getExchangeType() != PinotRelExchangeType.SUB_PLAN;
  }

  public static class Context {
    private final int _currentPlanFragmentId;

    private Context(int currentPlanFragmentId) {
      _currentPlanFragmentId = currentPlanFragmentId;
    }
  }
}
