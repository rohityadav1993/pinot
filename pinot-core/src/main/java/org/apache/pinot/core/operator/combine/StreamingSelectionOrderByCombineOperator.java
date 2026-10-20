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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.StreamingSelectionOrderByOperator;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.query.QueryThreadContext;

/// Streaming, lazy combine operator for selection ORDER BY queries whose first order-by expression is an identifier.
///
/// It performs an incremental k-way heap merge across the per-segment operators in `_operators`, returning
/// globally sorted rows in bounded blocks. Each segment is exposed through a [SortedCursorMerge.Cursor] that yields
/// that segment's locally-sorted rows in order:
///
/// - Segments physically sorted on the first order-by column are backed by
///   [StreamingSelectionOrderByOperator], which is pulled lazily one run/block at a time.
/// - Other (e.g. consuming/unsorted) segments are backed by a single materialized top-K block (any
///   [SelectionResultsBlock]-producing operator such as `SelectionOrderByOperator`); the cursor reads that
///   one block and iterates its rows.
///
/// A heap ([SortedCursorMerge]) of cursors ordered by the [OrderByComparatorFactory] comparator on
/// each
/// cursor's current head row drives the merge with an at-most-one-head-per-active-segment invariant (the heap holds the
/// cursors themselves, never all rows, which would degenerate into a full heap-sort that materializes everything). Each
/// cycle pops the global-min cursor, appends its head to the current output block, advances that one cursor by a single
/// row, and re-offers it if it still has a head.
///
/// **Min/max lazy segment activation (pruning).** Cursors are sorted by the first order-by column's min value
/// (ASC) / max value (DESC) reusing the `MinMaxValueContext` idea from
/// [MinMaxValueBasedSelectionOrderByCombineOperator]. A cursor is only activated (its segment acquired and first
/// block read) when the merge frontier reaches its min/max, so once `limit + offset` rows are emitted the
/// remaining segments are never acquired or read. See [#activateEligibleCursors(SortedCursorMerge.Cursor)] for the
/// correctness argument.
/// A consuming ([MutableSegment]) segment is always given no min/max and activates up front: its bounds are a snapshot
/// that rows ingested before activation can outgrow, since its plan (and doc count) is only built at activation.
/// With null handling on, a segment whose leading column is not flagged non-null by
/// [SelectionPlanNode#isColumnFlaggedNonNull] (it has nulls, the segment predates the flag, or it is a consuming
/// segment) is given no min/max and always activates. Flagged segments keep their min/max. With
/// null handling off, min/max are used as stored. The flag is read from segment metadata, not the null value vector:
/// that vector is a mapped buffer, and probing it here would acquire segments pruning is about to skip.
/// Its effect shows in the stats: a never-activated segment scans no docs, so `numSegmentsMatched` excludes it while
/// `numSegmentsProcessed` counts every segment. Their difference also counts activated segments that matched no rows.
/// TODO: report the activated count separately; that needs a new `DataTable.MetadataKey`.
///       See https://github.com/apache/pinot/issues/19924 (item B3)
///
/// **Segment acquire/release lifecycle.** A cursor acquires its
/// `AcquireReleaseColumnsSegmentOperator` on activation and releases it only when its child operator is fully
/// drained (acquire-on-activate / release-on-exhaust), rather than per run. This is intentional: the backing
/// [StreamingSelectionOrderByOperator] retains a buffer-backed `ValueBlock` across `nextBlock()`
/// calls in its tail-to-sort mode, so releasing between interleaved runs could read segment buffers after a release
/// under prefetch. Holding the acquire for the cursor's lifetime guarantees no release happens between a cursor's
/// own reads; min/max pruning bounds the number of simultaneously-active (acquired) segments to the merge frontier.
/// The rows handed out by the child operators are already deep-copied to heap `Object[]` (via
/// `RowBasedBlockValueFetcher`), so they remain valid after the segment is released. Any cursors still
/// acquired when the merge ends early are released by [#finish()] (LIMIT reached, before the final data block is
/// returned) or [#releaseAllCursors()] (error or stop).
///
/// **Streaming paths only.** This operator is installed only where a
/// [org.apache.pinot.core.query.executor.ResultsBlockStreamer] is present (the
/// MSE leaf, driven by [org.apache.pinot.core.operator.streaming.StreamingInstanceResponseOperator], and the
/// gRPC streaming endpoint). The merge emits many bounded [SelectionResultsBlock]s from successive
/// [#getNextBlock()] calls followed by a final [MetadataResultsBlock]. On the blocking single-stage
/// path the caller expects one complete block from a single [#getNextBlock()] call, so nothing here could
/// stream: the merge would accumulate the whole `limit + offset` result while holding every activated cursor's
/// materialized block alive. That path keeps [MinMaxValueBasedSelectionOrderByCombineOperator].
///
/// **Threading.** This operator overrides [#start()]/[#stop()] to no-ops (other than releasing
/// segments) and runs the merge single-threaded and lazily in [#getNextBlock()] on the consumer thread; it does
/// not use the base worker-queue model. The base entry points that it replaces - [#processSegments()] and
/// [#isQuerySatisfied(SelectionResultsBlock, Object)] - are overridden to fail loud. The base
/// `Phaser` (which exists only to fence worker threads against segment release) is intentionally bypassed because
/// all child/segment access is synchronous on the single consumer thread that holds the segment references; no async
/// work may be introduced here without restoring that fence. The instance is single-use (driven once to completion) and
/// is not thread-safe.
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSelectionOrderByCombineOperator extends BaseStreamingCombineOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "COMBINE_SELECT_ORDERBY_STREAMING";

  private static final SortedCursorMerge.BlockRows SELECTION_BLOCK_ROWS = new SortedCursorMerge.BlockRows() {
    @Override
    public DataSchema getDataSchema(BaseResultsBlock block) {
      return ((SelectionResultsBlock) block).getDataSchema();
    }

    @Nullable
    @Override
    public List<Object[]> getRows(BaseResultsBlock block) {
      return ((SelectionResultsBlock) block).getRows();
    }
  };

  private final boolean _asc;
  private final boolean _pruningEnabled;
  /// Whether a cursor whose bound *ties* the merge frontier may be deferred, not only one sorting strictly
  /// beyond it. Only correct under a single order-by expression: see [#sortsBeyond].
  private final boolean _deferTiedCursors;
  private final int _numRowsToKeep;
  private final int _blockSize;
  /// Segments whose metadata marks the leading order-by column sorted; -1 when that expression is not a column
  private final int _numSortedSegments;
  private final Comparator<Object[]> _comparator;
  private final SortedCursorMerge _merge;
  private final SortedCursorMerge.Cursor[] _sortedCursors;

  // Merge progress (single-threaded; mutated only by the consumer thread driving getNextBlock())
  private int _nextToActivate;
  private int _numRowsEmitted;
  private List<Object[]> _outputRows;
  private boolean _done;

  public StreamingSelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    // Pass a null merger: we override the consumption path entirely and never touch the base merger / worker queue.
    // Every base entry point that would dereference it - processSegments() and isQuerySatisfied() - is overridden to
    // throw, so a future change to the base streaming loop fails at that seam instead of with an NPE at query time.
    // The executor is unused here (start() is a no-op) but is still handed to the base so that the worker-queue model
    // stays constructible; passing null would trade one latent NPE for another.
    super(null, operators, queryContext, executorService);
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();
    _blockSize = queryContext.getSortedSelectionMergeBlockSize();

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    assert orderByExpressions != null && !orderByExpressions.isEmpty();
    OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
    _asc = firstOrderByExpression.isAsc();
    _comparator = OrderByComparatorFactory.getComparator(orderByExpressions, queryContext.isNullHandlingEnabled());

    // Frontier pruning needs a physical column to read min/max from. Under SortedSelectionMergeMode.ON the leading
    // order-by may be an expression (forced, for benchmarking), in which case there is no such column: every cursor
    // is activated up front in plan order and the merge is a plain k-way heap over materialized children.
    ExpressionContext firstOrderByExpressionContext = firstOrderByExpression.getExpression();
    String firstOrderByColumn =
        firstOrderByExpressionContext.getType() == ExpressionContext.Type.IDENTIFIER
            ? firstOrderByExpressionContext.getIdentifier() : null;
    _pruningEnabled = firstOrderByColumn != null;
    // A column-0 tie only proves the segment cannot supply an *earlier* row when column 0 is the whole sort key.
    // With two or more expressions a tie on column 0 can hide a row sorting earlier on column 1, so ties must
    // still activate (same gate as MinMaxValueBasedSelectionOrderByCombineOperator's numOrderByExpressions == 1).
    _deferTiedCursors = orderByExpressions.size() == 1;

    // Build one cursor per segment operator and read its first order-by column min/max for lazy activation ordering.
    // Reading DataSourceMetadata does not touch column buffers, so no segment acquire is needed here (mirrors
    // MinMaxValueBasedSelectionOrderByCombineOperator).
    _merge = new SortedCursorMerge(_comparator, StreamingSelectionOrderByCombineOperator::isStreamingOperator,
        SELECTION_BLOCK_ROWS, _numOperators);
    int numSortedSegments = firstOrderByColumn != null ? 0 : -1;
    for (int i = 0; i < _numOperators; i++) {
      Operator<BaseResultsBlock> operator = _operators.get(i);
      if (firstOrderByColumn == null) {
        _merge.addCursor(operator, null, null);
        continue;
      }
      DataSourceMetadata metadata =
          operator.getIndexSegment().getDataSource(firstOrderByColumn, queryContext.getSchema())
              .getDataSourceMetadata();
      Comparable minValue = metadata.getMinValue();
      Comparable maxValue = metadata.getMaxValue();
      // A consuming segment's min/max is a snapshot copied when getDataSource() builds the MutableDataSource, but under
      // prefetch (AcquireReleaseColumnsSegmentOperator) the child plan, and with it the doc count, is only built at
      // activation. Rows indexed in between can fall outside these bounds and be emitted out of order, so treat the
      // bounds as unknown and let activateEligibleCursors force-activate this cursor.
      if (operator.getIndexSegment() instanceof MutableSegment) {
        minValue = null;
        maxValue = null;
      }
      // isNonNull is loaded with the segment, the same class of access as min/max. False means nulls or unknown, so
      // drop the bounds and let activateEligibleCursors force-activate this cursor. Do not read the null vector here.
      if (queryContext.isNullHandlingEnabled()
          && !SelectionPlanNode.isColumnFlaggedNonNull(operator.getIndexSegment(), firstOrderByColumn)) {
        minValue = null;
        maxValue = null;
      }
      _merge.addCursor(operator, minValue, maxValue);
      if (metadata.isSorted()) {
        numSortedSegments++;
      }
    }
    _numSortedSegments = numSortedSegments;
    if (firstOrderByColumn != null) {
      _merge.sortCursorsByBound(_asc);
    }
    _sortedCursors = _merge.getCursors();
    _outputRows = newOutputList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Settings fixed at construction. Explain never drives the merge, so how many segments activate is not shown here.
  /// Query-wide values are idempotent so the broker merges the node across workers only when they agree; the segment
  /// counts are summed. `numSortedSegments` is metadata sortedness, not whether a child streams: a DESC scan without
  /// `allowReverseOrder`, or a leading column holding nulls, still falls back to a materialized child.
  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLongIdempotent("blockSize", _blockSize);
    attributeBuilder.putBool("frontierPruning", _pruningEnabled);
    attributeBuilder.putBool("deferTiedCursors", _deferTiedCursors);
    attributeBuilder.putLong("numSegments", _numOperators);
    if (_numSortedSegments >= 0) {
      attributeBuilder.putLong("numSortedSegments", _numSortedSegments);
    }
  }

  /// Override to a no-op: the merge is single-threaded and lazy in [#getNextBlock()], so we do not spin up the
  /// base worker threads / blocking-queue model.
  @Override
  public void start() {
  }

  /// Override the base worker-queue stop: no worker threads / phaser tasks were started. Release any segments still
  /// acquired (idempotent) so an early stop by the driver cannot leak acquires.
  @Override
  public void stop() {
    _done = true;
    releaseAllCursors();
  }

  /// The base worker-thread entry point must never run here ([#start()] is a no-op). Fail loud if it ever does.
  @Override
  protected void processSegments() {
    throw new IllegalStateException(
        "StreamingSelectionOrderByCombineOperator runs single-threaded; processSegments() must not be called");
  }

  /// Only reachable from the base streaming loop, which this operator replaces; the base implementation would
  /// dereference the null merger. Fail loud rather than NPE if a future change routes back through it.
  @Override
  protected boolean isQuerySatisfied(SelectionResultsBlock resultsBlock, Object tracker) {
    throw new IllegalStateException(
        "StreamingSelectionOrderByCombineOperator tracks its own limit; isQuerySatisfied() must not be called");
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    if (_done) {
      // Streaming mode: terminal metadata block after the last data block. Idempotent if called again.
      return _merge.attachSchemaMismatchErrors(attachExecutionStats(new MetadataResultsBlock()));
    }
    try {
      long endTimeMs = _queryContext.getEndTimeMs();
      // The leader is retained outside the heap: segments are usually near-disjoint on a time-like leading order-by
      // column, so one cursor supplies long runs and re-heaping it per row costs two O(log k) sifts to reach the same
      // cursor again. While it keeps winning we only peek and compare.
      // TODO: move whole runs with one addAll(subList(...)) after a binary search for the cut. Deferred because
      //       advancing _numRowsEmitted by a run steps over the 0x1FFF mask in the termination check below, losing
      //       cancellation/pause/deadline observation; needs an unconditional check per run or a run-length clamp.
      //       See https://github.com/apache/pinot/issues/19924 (item B2)
      SortedCursorMerge.Cursor leader = null;
      while (_numRowsEmitted < _numRowsToKeep) {
        // The merge drains the heap on this thread and, for single-block cursors, may never re-enter a child operator.
        // Without this check nothing would observe cancellation, pause, or the query deadline for up to
        // (limit + offset) iterations -- a regression against MinMaxValueBasedSelectionOrderByCombineOperator, which
        // this operator replaces on the same query shape.
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numRowsEmitted, EXPLAIN_NAME, endTimeMs);
        activateEligibleCursors(leader);
        // Choose the leader strictly AFTER activation, so a cursor just activated can win instead of being skipped
        // for one row.
        if (leader == null) {
          leader = _merge.poll();
          if (leader == null) {
            // All active cursors exhausted (and pruning guarantees the rest cannot contribute).
            break;
          }
        } else {
          SortedCursorMerge.Cursor top = _merge.peek();
          if (top != null && _merge.compare(leader, top) > 0) {
            // Lost the lead; hand it back and take the new minimum. A tie keeps the incumbent, reordering only rows
            // equal on every order-by expression -- the heap's own tie-breaking is arbitrary too.
            _merge.offer(leader);
            leader = _merge.poll();
          }
        }
        _outputRows.add(leader.currentHead());
        _numRowsEmitted++;
        if (_numRowsEmitted >= _numRowsToKeep) {
          // Done: advancing could pull the leader's next block, which on the run path scans a whole run for nothing.
          // Leaving through the loop exit also releases every segment before the final data block is returned, even
          // when this row fills the block: the caller sends that block downstream next, which can block.
          break;
        }
        leader.advance();
        if (leader.currentHead() == null) {
          leader = null;
        }
        if (_outputRows.size() >= _blockSize) {
          // Restore the invariant before handing control back: at every entry to and exit from getNextBlock() the
          // heap holds all active cursors, so nothing else has to know about the retained leader.
          if (leader != null) {
            _merge.offer(leader);
          }
          return flushDataBlock();
        }
      }
      // Merge complete (limit reached or every cursor drained); the leader goes back so finish() sees a consistent
      // heap.
      if (leader != null) {
        _merge.offer(leader);
      }
      finish();
      if (!_outputRows.isEmpty()) {
        // The final partial data block; the next call returns the terminal metadata block.
        return flushDataBlock();
      }
      return _merge.attachSchemaMismatchErrors(attachExecutionStats(new MetadataResultsBlock()));
    } catch (Exception e) {
      _done = true;
      releaseAllCursors();
      return createExceptionResultsBlockAndAttachExecutionStats(e, "merging sorted selection results");
    } catch (Throwable t) {
      // An Error (e.g. OutOfMemoryError while accumulating output rows) must not leave segments acquired for the
      // lifetime of the server. In single-stage mode there is no start()/stop() backstop around this operator.
      _done = true;
      releaseAllCursors();
      throw t;
    }
  }

  /// Activates not-yet-active cursors whose min/max bound can still reach the merge frontier.
  ///
  /// Cursors are visited in min/max order and `_nextToActivate` advances only on a real activation, so a
  /// `break` defers the current cursor to a later call with a risen frontier rather than skipping it. Deferring
  /// is safe because the first order-by column is the primary sort key: a cursor whose range starts past the frontier
  /// holds no row sorting before it. With no frontier known yet, activation is forced. Under a single order-by
  /// expression a cursor whose bound merely *ties* the frontier is deferred too, which is what keeps a long run of
  /// equal segment minima from activating every segment at once; see [#sortsBeyond].
  ///
  /// The frontier is the row about to be emitted. Understating it defers a cursor that could have supplied that row
  /// and the merge emits out of order, where overstating it only activates a segment early. The leader is retained
  /// outside the heap, so that row is the smaller of its head and the heap head: a bound past either is past the
  /// frontier. Checking the leader alone would overstate it whenever the leader has moved past the heap head, which
  /// happens both on entry (the leader just advanced) and within this loop (an activation offers a smaller head).
  private void activateEligibleCursors(@Nullable SortedCursorMerge.Cursor leader) {
    while (_nextToActivate < _sortedCursors.length) {
      SortedCursorMerge.Cursor cursor = _sortedCursors[_nextToActivate];
      if (_pruningEnabled) {
        Comparable bound = _asc ? cursor.getMinValue() : cursor.getMaxValue();
        // Re-read per iteration: each activation below can offer a smaller head into the heap.
        SortedCursorMerge.Cursor top = _merge.peek();
        // A null bound always activates, and with no candidate at all there is no frontier to prune against.
        if (bound != null && (leader != null || top != null) && sortsBeyond(bound, leader, top)) {
          break;
        }
      }
      _merge.activate(cursor);
      _nextToActivate++;
    }
  }

  /// Whether `bound` sorts past the frontier -- the smaller of the two candidates' heads -- on the first order-by
  /// column, so that segment cannot supply the next row. An absent candidate imposes no constraint; a null head cannot
  /// be compared, so this reports `false` and the caller activates. Tests column 0 only, as the pruning bound always
  /// has, rather than the full-row comparator -- which would put every order-by column on the per-row path.
  ///
  /// Under a single order-by expression (`_deferTiedCursors`) a *tie* also defers, which only postpones a
  /// cursor: `_nextToActivate` does not advance, and the caller force-activates once heap and leader drain.
  /// Since `bound` bounds every row in the segment, a deferred cursor holds no row sorting strictly before an
  /// emitted one -- only ties, interchangeable when column 0 is the whole sort key. Note this is *not*
  /// [MinMaxValueBasedSelectionOrderByCombineOperator]'s justification: its bound is a complete top-K's k-th
  /// row, this one is the live frontier. It changes which tied rows are returned, already documented as arbitrary.
  private boolean sortsBeyond(Comparable bound, @Nullable SortedCursorMerge.Cursor leader,
      @Nullable SortedCursorMerge.Cursor top) {
    Object leaderHead = leader != null ? leader.currentHead()[0] : null;
    Object topHead = top != null ? top.currentHead()[0] : null;
    if ((leader != null && leaderHead == null) || (top != null && topHead == null)) {
      return false;
    }
    // Past either head is past the smaller one.
    return (leader != null && sortsBeyond(bound, leaderHead)) || (top != null && sortsBeyond(bound, topHead));
  }

  private boolean sortsBeyond(Comparable bound, Object headValue) {
    // Both come from the same first order-by column: the metadata min/max and the materialized row[0] share the
    // column's stored type, so this comparison is type-safe (same assumption as MinMaxValueBased...).
    int cmp = bound.compareTo(headValue);
    if (_deferTiedCursors) {
      return _asc ? cmp >= 0 : cmp <= 0;
    }
    return _asc ? cmp > 0 : cmp < 0;
  }

  /// Marks the merge done, releases any segments still held by un-drained cursors (e.g. when LIMIT is reached), and
  /// drops their buffered rows. The output rows are already in [#_outputRows], and the final data block can wait on a
  /// downstream send, so up to a block of rows per active cursor would otherwise stay reachable for nothing.
  private void finish() {
    _done = true;
    _merge.finish();
  }

  @VisibleForTesting
  int getNumCursorsHoldingRows() {
    return _merge.getNumCursorsHoldingRows();
  }

  private void releaseAllCursors() {
    _merge.releaseAll();
  }

  /// Returns the accumulated output rows as a sorted [SelectionResultsBlock] and resets the output buffer. The
  /// block carries the comparator so the broker-side n-way reduce stays correct. Execution stats and schema-mismatch
  /// errors are not attached here: they go on the terminal metadata block.
  private BaseResultsBlock flushDataBlock() {
    List<Object[]> rows = _outputRows;
    _outputRows = newOutputList();
    // Rows only reach the output buffer through a cursor's pullBlock(), which captures the schema before accepting any.
    DataSchema dataSchema = _merge.getDataSchema();
    assert dataSchema != null;
    return new SelectionResultsBlock(dataSchema, rows, _comparator, _queryContext);
  }

  /// Whether `operator` (already unwrapped from any acquire/release wrapper) is the lazy streaming child.
  private static boolean isStreamingOperator(Operator operator) {
    return operator instanceof StreamingSelectionOrderByOperator;
  }

  private List<Object[]> newOutputList() {
    int capacity =
        Math.min(_blockSize, Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    return new ArrayList<>(Math.max(1, capacity));
  }
}
