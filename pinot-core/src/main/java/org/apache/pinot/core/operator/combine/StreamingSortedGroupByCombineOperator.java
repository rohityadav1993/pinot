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
package org.apache.pinot.core.operator.combine;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Streaming combine for the sorted streaming GROUP BY: a k-way merge, by group key, of the key-ordered streams that
/// the per-segment operators produce ([org.apache.pinot.core.operator.query.StreamingSortedGroupByOperator] for a
/// segment sorted on the key, [org.apache.pinot.core.operator.query.HashSortedGroupByOperator] for any other), built on
/// [SortedGroupByMerge] and [SortedCursorMerge].
///
/// **Merge groups.** The segment operators are split into merge groups by [SortedGroupByPartitioning]: one group per
/// partition of the group-by column when every segment reports the same partition function and a single partition id
/// (the same key can then never be in two groups), otherwise one group holding every segment. Each group is one
/// [SortedGroupByMerge]. A key appears at most once across all output blocks, and the output is ascending by key
/// *within* a group; with several groups the blocks of different groups interleave in arrival order, which no
/// consumer may rely on.
///
/// **Threading.** A single group runs inline on the consumer thread in [#getNextBlock()], like
/// [StreamingSelectionOrderByCombineOperator]. Several groups run as parallel tasks: [#start()] submits up to
/// `numTasks` workers (the base class' task count) that each claim whole groups and push their blocks through the
/// bounded blocking queue (back pressure) to the consumer, which only forwards them. The base class' `Phaser` fence is
/// kept: workers register with it, and [#stop()] cancels them and waits for all of them before releasing any segment,
/// so the segments the consumer thread references outlive every worker. A group's cursors are activated, advanced and
/// released only by the one thread running that group. Per-segment hash aggregation results are materialized into
/// self-contained records inside the per-segment operator, on the thread that ran it, so no worker-thread-local
/// group-key state ever crosses a thread (see `StreamingGroupByCombineOperator#detachFromWorkerThreadState`).
///
/// **Null keys.** With null handling on, a null key sorts last and is an ordinary group of the merge. Partition
/// metadata is stamped from the default value a null row holds, so every null row of a table sits in the partition of
/// that default value, one group, and the groups stay disjoint.
///
/// **Result kind.** When the leaf returns final results ([QueryContext#isServerReturnFinalResult()] or the key
/// unpartitioned variant) each closed group's aggregation columns are converted with `extractFinalResult` and the
/// block schema carries the final column types. Otherwise blocks carry intermediate results. Both are safe because a
/// key is complete when emitted.
///
/// **No limits.** There is no group-count limit and no trimming; memory is bounded by one block per active cursor plus
/// the output batch (and the queue, with several groups).
///
/// Single-use, not thread-safe: [#getNextBlock()], [#start()] and [#stop()] are called from the consumer thread.
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSortedGroupByCombineOperator extends BaseStreamingCombineOperator<GroupByResultsBlock> {
  public static final String EXPLAIN_NAME = "COMBINE_GROUP_BY_SORTED_STREAMING";

  private final boolean _returnFinalResult;
  private final int _blockSize;
  private final SortedGroupByPartitioning.Split _split;
  private final List<SortedGroupByMerge> _merges;
  private final boolean _parallel;
  @Nullable
  private final String _decision;

  // Consumer-thread progress
  private int _numMergesFinished;
  private boolean _done;

  public StreamingSortedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    // Null merger: the base consumption path is replaced and its entry points below fail loud.
    super(null, operators, queryContext, executorService);
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null && !groupByExpressions.isEmpty();
    assert queryContext.getAggregationFunctions() != null;
    _returnFinalResult =
        queryContext.isServerReturnFinalResult() || queryContext.isServerReturnFinalResultKeyUnpartitioned();
    _blockSize = queryContext.getSortedGroupByBlockSize();
    _decision = queryContext.getSortedGroupByDecision();

    _split = SortedGroupByPartitioning.split(operators, groupByExpressions.get(0).getIdentifier(),
        queryContext.getSchema());
    _parallel = _split._groups.size() > 1;
    Comparator<Object[]> comparator = getKeyComparator(queryContext);
    _merges = new ArrayList<>(_split._groups.size());
    for (List<Operator> group : _split._groups) {
      _merges.add(new SortedGroupByMerge(group, queryContext, comparator,
          EXPLAIN_NAME));
    }
  }

  /// The merge comparator: the standard ORDER BY comparator (ascending, nulls last) over the group-by key prefix of a
  /// row, so a composite key needs no new code here. Keys compare by value, never by dictionary id, because each
  /// segment has its own dictionary.
  public static Comparator<Object[]> getKeyComparator(QueryContext queryContext) {
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    List<OrderByExpressionContext> keyOrder = new ArrayList<>(groupByExpressions.size());
    for (ExpressionContext groupByExpression : groupByExpressions) {
      keyOrder.add(new OrderByExpressionContext(groupByExpression, true, true));
    }
    return OrderByComparatorFactory.getComparator(keyOrder, queryContext.isNullHandlingEnabled());
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLongIdempotent("blockSize", _blockSize);
    attributeBuilder.putBool("finalResult", _returnFinalResult);
    attributeBuilder.putLong("numSegments", _numOperators);
    // Independent merge tasks: one per partition, or a single one holding every segment
    attributeBuilder.putLong("numMergeGroups", _merges.size());
    attributeBuilder.putBool("partitioned", _split.isPartitioned());
    attributeBuilder.putBool("parallel", _parallel);
    if (_split._partitionIds != null) {
      List<String> groups = new ArrayList<>(_merges.size());
      for (int i = 0; i < _split._groups.size(); i++) {
        groups.add("partition " + _split._partitionIds.get(i) + ": " + _split._groups.get(i).size() + " segments");
      }
      attributeBuilder.putStringList("mergeGroups", groups);
    } else if (_split._reason != null) {
      attributeBuilder.putString("notPartitionedBecause", _split._reason);
    }
    if (_decision != null) {
      attributeBuilder.putString("decision", _decision);
    }
  }

  /// Starts the parallel merge tasks, if there are several merge groups; a single group runs inline in
  /// [#getNextBlock()].
  @Override
  public void start() {
    if (_parallel) {
      startProcess();
    }
  }

  /// Stops the workers and waits for them (the `Phaser` fence), then releases any segment still acquired (idempotent)
  /// so an early stop cannot leak acquires.
  @Override
  public void stop() {
    _done = true;
    stopProcess();
    for (SortedGroupByMerge merge : _merges) {
      merge.releaseAll();
    }
  }

  /// Worker body: claims whole merge groups and pushes their blocks to the consumer.
  @Override
  protected void processSegments() {
    int groupId;
    while (_processingException.get() == null && (groupId = _nextOperatorId.getAndIncrement()) < _merges.size()) {
      SortedGroupByMerge merge = _merges.get(groupId);
      try {
        GroupByResultsBlock block;
        while ((block = merge.nextBlock()) != null) {
          offerBlock(block);
        }
      } finally {
        merge.releaseAll();
      }
      offerBlock(LAST_RESULTS_BLOCK);
    }
  }

  // NOTE: Throws EarlyTerminationException when interrupted or timed out, like the base class' own hand-off
  private void offerBlock(BaseResultsBlock block) {
    try {
      if (!_blockingQueue.offer(block, _queryContext.getEndTimeMs() - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS)) {
        throw new EarlyTerminationException("Timed out waiting to add results block");
      }
    } catch (InterruptedException e) {
      throw new EarlyTerminationException("Interrupted waiting to add results block");
    }
  }

  @Override
  protected boolean isQuerySatisfied(GroupByResultsBlock resultsBlock, Object tracker) {
    throw new IllegalStateException(
        "StreamingSortedGroupByCombineOperator has no early exit; isQuerySatisfied() must not be called");
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    if (_done) {
      return terminalBlock();
    }
    try {
      BaseResultsBlock block = _parallel ? nextParallelBlock() : nextInlineBlock();
      if (block != null) {
        return block;
      }
      _done = true;
      return terminalBlock();
    } catch (Exception e) {
      _done = true;
      releaseInlineMerge();
      return createExceptionResultsBlockAndAttachExecutionStats(e, "merging sorted group-by results");
    } catch (Throwable t) {
      // An Error must not leave segments acquired for the lifetime of the server.
      _done = true;
      releaseInlineMerge();
      throw t;
    }
  }

  /// The single merge group runs on this thread. Returns `null` when it is complete.
  @Nullable
  private BaseResultsBlock nextInlineBlock() {
    return _merges.get(0).nextBlock();
  }

  /// Forwards the next block a worker produced. Returns `null` when every group is complete.
  @Nullable
  private BaseResultsBlock nextParallelBlock()
      throws InterruptedException {
    long endTimeMs = _queryContext.getEndTimeMs();
    while (_numMergesFinished < _merges.size()) {
      QueryThreadContext.checkTermination(this::getExplainName);
      BaseResultsBlock block = pollResultsBlock(_blockingQueue, endTimeMs);
      if (block == null) {
        throw QueryErrorCode.EXECUTION_TIMEOUT.asException("Timed out while polling results block");
      }
      if (block instanceof ExceptionResultsBlock) {
        // A worker failed: its task already cleared the queue, so this is the last thing it will deliver
        _done = true;
        return checkTerminateExceptionAndAttachExecutionStats(block);
      }
      if (block == LAST_RESULTS_BLOCK) {
        _numMergesFinished++;
        continue;
      }
      return block;
    }
    return null;
  }

  /// Releases the segments of the inline merge. With parallel merges the workers own their groups' segments, so
  /// nothing is released here: [#stop()], which the caller always runs, waits for the workers and then releases.
  private void releaseInlineMerge() {
    if (!_parallel) {
      _merges.get(0).releaseAll();
    }
  }

  private BaseResultsBlock terminalBlock() {
    BaseResultsBlock block = attachExecutionStats(new MetadataResultsBlock());
    for (SortedGroupByMerge merge : _merges) {
      merge.attachSchemaMismatchErrors(block);
    }
    return block;
  }

  @VisibleForTesting
  int getNumCursorsHoldingRows() {
    int numCursors = 0;
    for (SortedGroupByMerge merge : _merges) {
      numCursors += merge.getNumCursorsHoldingRows();
    }
    return numCursors;
  }

  @VisibleForTesting
  int getNumMergeGroups() {
    return _merges.size();
  }
}
