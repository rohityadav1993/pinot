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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiStreamConsumer.StreamHandle;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code SortedMailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serves it out from the
 * {@link #nextBlock()} API in a globally sorted manner.
 *
 * <p>It supports two strategies, selected at construction time:
 * <ul>
 *   <li><b>Accumulate-then-sort</b> (default): every row from every mailbox is buffered, sorted once at EOS, and
 *       returned in a single data block. This is the historical behavior and is used whenever the k-way merge is not
 *       enabled.</li>
 *   <li><b>Streaming k-way merge</b>: when the sender is known to emit each mailbox already sorted on the receiver's
 *       collation, the rows are merged incrementally with a min-heap and emitted in bounded blocks (of at most
 *       {@code blockSize} rows). Global order is preserved across block boundaries because the heap state carries over
 *       between {@link #getNextBlock()} calls.</li>
 * </ul>
 * The k-way merge is enabled only when the {@code streamingSortedMailboxReceive} query option is explicitly
 * {@code TRUE} <b>and</b> {@link MailboxReceiveNode#isSortedOnSender()} is true. All other combinations fall back to
 * the accumulate-then-sort path.
 *
 * <p>Like the rest of the receive operators, this class is driven by a single consumer thread; it is not thread-safe.
 */
public class SortedMailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxReceiveOperator.class);

  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_RECEIVE";

  /**
   * Default upper bound on the number of rows emitted per block in the streaming k-way merge, used when the
   * {@code streamingSortedMailboxReceiveBlockSize} query option is not set. Defined locally to avoid introducing a
   * dependency on {@code pinot-core} (where {@code SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY} lives).
   */
  private static final int DEFAULT_BLOCK_SIZE = 10_000;

  private final DataSchema _dataSchema;
  private final List<RelFieldCollation> _collations;
  private final List<Object[]> _rows = new ArrayList<>();

  // Streaming k-way merge state. The merge-only fields are meaningful only when _useKWayMerge is true.
  private final boolean _useKWayMerge;
  private final int _blockSize;
  private final Comparator<Object[]> _comparator;
  // Built lazily on the first merge call so priming (driving every handle to first-data/EOS/error) happens once.
  private PriorityQueue<Cursor> _heap;
  private boolean _primed;

  private MseBlock _eosBlock;

  public SortedMailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
    Preconditions.checkState(!CollectionUtils.isEmpty(node.getCollations()), "Field collations must be set");
    _dataSchema = node.getDataSchema();
    _collations = node.getCollations();
    // reverse=false => the collation minimum sits at the min-heap head (honors per-field ASC/DESC + null direction).
    _comparator = new SortUtils.SortComparator(_collations, false);
    Boolean hint = QueryOptionsUtils.getStreamingSortedMailboxReceive(context.getOpChainMetadata());
    _useKWayMerge = (hint == Boolean.TRUE) && node.isSortedOnSender();
    Integer blockSize = QueryOptionsUtils.getStreamingSortedMailboxReceiveBlockSize(context.getOpChainMetadata());
    _blockSize = blockSize != null ? blockSize : DEFAULT_BLOCK_SIZE;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    if (_useKWayMerge) {
      return getNextMergedBlock();
    }
    // Collect all the rows from the mailbox and sort them
    while (true) {
      MseBlock block = _multiConsumer.readMseBlockBlocking();
      if (block.isData()) {
        _rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        continue;
      }
      MseBlock.Eos eosBlock = (MseBlock.Eos) block;
      onEos();
      _eosBlock = eosBlock;
      if (eosBlock.isError()) {
        return eosBlock;
      } else {
        if (!_rows.isEmpty()) {
          // TODO: This might not be efficient because we are sorting all the received rows. We should use a k-way merge
          //       when sender side is sorted.
          _rows.sort(new SortUtils.SortComparator(_collations, false));
          return new RowHeapDataBlock(_rows, _dataSchema);
        } else {
          return block;
        }
      }
    }
  }

  /**
   * Streaming k-way merge over the per-sender {@link StreamHandle}s. Emits at most {@link #_blockSize} rows per call,
   * keeping the heap state between calls so global order is preserved across blocks.
   */
  private MseBlock getNextMergedBlock() {
    if (_isEarlyTerminated) {
      // Stop pulling new data; drive every handle to EOS (so receiving stats are folded in) and finish.
      return drainToEos();
    }
    if (!_primed) {
      _heap = new PriorityQueue<>((a, b) -> _comparator.compare(a.head(), b.head()));
      // Prime: drive every handle to its first data block / EOS / error before the first pop, so the heap holds a head
      // for every still-active mailbox and the min is the global min.
      for (StreamHandle<ReceivingMailbox.MseBlockWithStats> handle : streamHandles()) {
        Cursor cursor = refill(handle);
        if (_eosBlock != null) {
          // An error was found while priming; refill already cached it and folded stats.
          return _eosBlock;
        }
        if (cursor != null) {
          _heap.add(cursor);
        }
      }
      _primed = true;
    }

    // Initial capacity is capped at DEFAULT_BLOCK_SIZE so a very large configured _blockSize does not eagerly allocate
    // a huge backing array up front; for larger blocks the list grows amortized as rows are appended.
    List<Object[]> out = new ArrayList<>(Math.min(_blockSize, DEFAULT_BLOCK_SIZE));
    while (out.size() < _blockSize) {
      if (_heap.isEmpty()) {
        onEos();
        _eosBlock = SuccessMseBlock.INSTANCE;
        return out.isEmpty() ? _eosBlock : new RowHeapDataBlock(out, _dataSchema);
      }
      Cursor cursor = _heap.poll();
      out.add(cursor.head());
      cursor._idx++;
      if (cursor.hasCurrent()) {
        // Still has rows in the current block: reseat with the new head.
        _heap.add(cursor);
      } else {
        // Current block exhausted: refill THIS mailbox before the next pop to restore the heap invariant.
        Cursor refilled = refill(cursor._handle);
        if (_eosBlock != null) {
          // An error was found while refilling; short-circuit immediately.
          return _eosBlock;
        }
        if (refilled != null) {
          _heap.add(refilled);
        }
        // else this mailbox reached EOS and is dropped from the merge.
      }
    }
    return new RowHeapDataBlock(out, _dataSchema);
  }

  /**
   * Drives ONE handle to its next non-empty data block. Returns a {@link Cursor} positioned at the first row of that
   * block, or {@code null} when the mailbox reaches a success EOS (dropped from the merge). On an error block, folds
   * the receiving stats via {@link #onEos()}, caches the error in {@link #_eosBlock}, and returns {@code null}.
   */
  @Nullable
  private Cursor refill(StreamHandle<ReceivingMailbox.MseBlockWithStats> handle) {
    while (true) {
      ReceivingMailbox.MseBlockWithStats element = handle.readBlocking();
      MseBlock block = element.getBlock();
      if (block.isError()) {
        onEos();
        _eosBlock = block;
        return null;
      }
      if (block.isSuccess()) {
        return null;
      }
      List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
      if (!rows.isEmpty()) {
        return new Cursor(handle, rows);
      }
      // Defensive: an empty data block carries no head, so read again.
    }
  }

  /**
   * Drains every handle to its terminal element after early termination, folding receiving stats. Returns the cached
   * error block if any handle yields one, otherwise a success EOS.
   */
  private MseBlock drainToEos() {
    for (StreamHandle<ReceivingMailbox.MseBlockWithStats> handle : streamHandles()) {
      while (!handle.isExhausted()) {
        MseBlock block = handle.readBlocking().getBlock();
        if (block.isError()) {
          onEos();
          _eosBlock = block;
          return block;
        }
        // Data or success EOS: success flips isExhausted() to true and ends the loop; data is discarded.
      }
    }
    onEos();
    _eosBlock = SuccessMseBlock.INSTANCE;
    return _eosBlock;
  }

  @Override
  public void close() {
    super.close();
    _rows.clear();
    if (_heap != null) {
      _heap.clear();
    }
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _rows.clear();
    if (_heap != null) {
      _heap.clear();
    }
  }

  /**
   * A cursor over one mailbox's current data block. Holds the handle so the merge can refill this specific mailbox when
   * the block is exhausted. Created only for non-empty blocks, so {@link #head()} is always valid until {@link #_idx}
   * runs past the end.
   */
  private static final class Cursor {
    final StreamHandle<ReceivingMailbox.MseBlockWithStats> _handle;
    final List<Object[]> _rows;
    int _idx;

    Cursor(StreamHandle<ReceivingMailbox.MseBlockWithStats> handle, List<Object[]> rows) {
      _handle = handle;
      _rows = rows;
    }

    Object[] head() {
      return _rows.get(_idx);
    }

    boolean hasCurrent() {
      return _idx < _rows.size();
    }
  }
}
