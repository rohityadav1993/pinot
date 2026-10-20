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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.partition.function.ModuloPartitionFunction;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.query.HashSortedGroupByOperator;
import org.apache.pinot.core.operator.query.StreamingSortedGroupByOperator;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.SortedGroupByDecision;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedGroupByMode;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests the sorted streaming GROUP BY ([StreamingSortedGroupByOperator] per segment and
/// [StreamingSortedGroupByCombineOperator] above it) at the server query seam: the streaming instance plan with a
/// results-block streamer attached, which is the path an MSE leaf takes. Every engaged case is compared with the same
/// query planned with the mode OFF, which runs the existing GROUP BY path.
///
/// Harness helpers for later tickets: [#plan], [#drive], [#assertParity], [#route].
public class StreamingSortedGroupByCombineOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "StreamingSortedGroupByCombineTest");
  private static final String RAW_TABLE_NAME = "testTable";

  private static final String KEY_COL = "key";
  private static final String OTHER_COL = "other";
  private static final String VAL_COL = "val";
  private static final int NUM_SEGMENTS = 5;
  private static final int KEYS_PER_SEGMENT = 60;
  private static final int KEY_STRIDE = 20;
  /// Keys 0..(NUM_SEGMENTS - 1) * KEY_STRIDE + KEYS_PER_SEGMENT - 1: every key but the extremes spans 2-3 segments
  private static final int NUM_DISTINCT_KEYS = (NUM_SEGMENTS - 1) * KEY_STRIDE + KEYS_PER_SEGMENT;

  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_FUNNEL_USERS = 60;
  private static final String AGGS = "COUNT(*), SUM(val), MIN(val), MAX(val)";
  /// A LIMIT at or above numGroupsLimit cannot trim at the leaf; the default LIMIT 10 would (see the gate).
  private static final String BIG_LIMIT = " LIMIT 1000000";
  private static final String QUERY = "SELECT key, " + AGGS + " FROM testTable GROUP BY key" + BIG_LIMIT;

  private static final TableConfig SORTED_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSortedColumn(KEY_COL).build();
  private static final TableConfig UNSORTED_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(KEY_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(OTHER_COL, FieldSpec.DataType.INT)
      .addMetric(VAL_COL, FieldSpec.DataType.LONG)
      .build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private List<IndexSegment> _sortedSegments;
  /// Two segments with 3 keys of 12000 rows each, so every key run spans more than one 10000-doc value block
  private List<IndexSegment> _bigRunSegments;
  private IndexSegment _unsortedSegment;
  /// 4 partitions (Modulo 4 of the key) x 2 sorted segments each, one partition per segment, overlapping keys
  private List<IndexSegment> _partitionedSortedSegments;
  /// 4 partitions x 3 unsorted segments each
  private List<IndexSegment> _partitionedUnsortedSegments;
  /// Sorted segments partitioned by Modulo 8, so they disagree with the Modulo 4 ones
  private IndexSegment _modulo8Segment;
  /// Partitioned by Modulo 4 but holding keys of every partition, so it reports several partitions
  private IndexSegment _spanningSegment;
  /// Segments that carry null keys next to a real key equal to the null default value, built with null handling
  private List<IndexSegment> _nullBearingSortedSegments;
  private IndexSegment _nullBearingUnsortedSegment;
  /// Three sorted segments of funnel events (key = user, other = step type, val = timestamp); every user's events
  /// are spread over all three, so each group spans segments
  private List<IndexSegment> _funnelSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _sortedSegments = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _sortedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "sorted_" + i,
          sortedRows(i * KEY_STRIDE, KEYS_PER_SEGMENT, i * 1000)));
    }
    _bigRunSegments = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      List<GenericRow> rows = new ArrayList<>();
      for (int key = 0; key < 3; key++) {
        for (int r = 0; r < 12_000; r++) {
          rows.add(row(key, r % 7, (long) key * 100 + r % 13 + i));
        }
      }
      _bigRunSegments.add(buildSegment(SORTED_TABLE_CONFIG, "bigRun_" + i, rows));
    }
    List<GenericRow> unsortedRows = sortedRows(0, KEYS_PER_SEGMENT, 7);
    Collections.shuffle(unsortedRows, new Random(42));
    _unsortedSegment = buildSegment(UNSORTED_TABLE_CONFIG, "unsorted_0", unsortedRows);

    _partitionedSortedSegments = new ArrayList<>();
    for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
      for (int j = 0; j < 2; j++) {
        _partitionedSortedSegments.add(buildSegment(partitionedConfig(true, NUM_PARTITIONS),
            "partSorted_" + partition + "_" + j,
            partitionRows(partition, NUM_PARTITIONS, j * 20, j * 20 + 60, j), false));
      }
    }
    _partitionedUnsortedSegments = new ArrayList<>();
    for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
      for (int j = 0; j < 3; j++) {
        List<GenericRow> rows = partitionRows(partition, NUM_PARTITIONS, j * 10, j * 10 + 50, 100 + j);
        Collections.shuffle(rows, new Random(partition * 10 + j));
        _partitionedUnsortedSegments.add(buildSegment(partitionedConfig(false, NUM_PARTITIONS),
            "partUnsorted_" + partition + "_" + j, rows, false));
      }
    }
    _modulo8Segment = buildSegment(partitionedConfig(true, 8), "mod8_0", partitionRows(0, 8, 0, 40, 5), false);
    _spanningSegment = buildSegment(partitionedConfig(true, NUM_PARTITIONS), "spanning_0", sortedRows(0, 12, 6), false);

    _nullBearingSortedSegments = new ArrayList<>();
    _nullBearingSortedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "nullSorted_0",
        nullBearingSortedRows(1, 10, 0), true));
    _nullBearingSortedSegments.add(buildSegment(SORTED_TABLE_CONFIG, "nullSorted_1",
        nullBearingSortedRows(5, 11, 500), true));
    List<GenericRow> nullUnsortedRows = nullBearingSortedRows(3, 9, 900);
    Collections.shuffle(nullUnsortedRows, new Random(7));
    _nullBearingUnsortedSegment = buildSegment(UNSORTED_TABLE_CONFIG, "nullUnsorted_0", nullUnsortedRows, true);

    _funnelSegments = new ArrayList<>();
    List<List<GenericRow>> funnelRows = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    Random funnelRandom = new Random(2024);
    for (int user = 0; user < NUM_FUNNEL_USERS; user++) {
      int numEvents = 1 + funnelRandom.nextInt(10);
      for (int e = 0; e < numEvents; e++) {
        // Step types 1-3 form the funnel, 4 is noise; timestamps collide now and then (STRICT_INCREASE)
        funnelRows.get(funnelRandom.nextInt(3))
            .add(row(user, 1 + funnelRandom.nextInt(4), funnelRandom.nextInt(40) * 5L));
      }
    }
    for (int i = 0; i < 3; i++) {
      // Rows are appended user by user, so each segment is sorted by key
      _funnelSegments.add(buildSegment(SORTED_TABLE_CONFIG, "funnel_" + i, funnelRows.get(i)));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment segment : _sortedSegments) {
      segment.destroy();
    }
    for (IndexSegment segment : _bigRunSegments) {
      segment.destroy();
    }
    _unsortedSegment.destroy();
    for (IndexSegment segment : _partitionedSortedSegments) {
      segment.destroy();
    }
    for (IndexSegment segment : _partitionedUnsortedSegments) {
      segment.destroy();
    }
    _modulo8Segment.destroy();
    _spanningSegment.destroy();
    for (IndexSegment segment : _nullBearingSortedSegments) {
      segment.destroy();
    }
    _nullBearingUnsortedSegment.destroy();
    for (IndexSegment segment : _funnelSegments) {
      segment.destroy();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  private static GenericRow row(int key, int other, long val) {
    GenericRow row = new GenericRow();
    row.putValue(KEY_COL, key);
    row.putValue(OTHER_COL, other);
    row.putValue(VAL_COL, val);
    return row;
  }

  /// Rows sorted by key; key k appears 1 + k % 3 times.
  private static List<GenericRow> sortedRows(int firstKey, int numKeys, int salt) {
    List<GenericRow> rows = new ArrayList<>();
    for (int key = firstKey; key < firstKey + numKeys; key++) {
      int repeats = 1 + key % 3;
      for (int r = 0; r < repeats; r++) {
        rows.add(row(key, (key * 31 + r) % 17, (long) key * 10 + r + salt));
      }
    }
    return rows;
  }

  /// Keys k in [firstKey, endKey) with k % numPartitions == partition, sorted; key k appears 1 + k % 3 times.
  private static List<GenericRow> partitionRows(int partition, int numPartitions, int firstKey, int endKey, int salt) {
    List<GenericRow> rows = new ArrayList<>();
    for (int key = firstKey; key < endKey; key++) {
      if (key % numPartitions != partition) {
        continue;
      }
      int repeats = 1 + key % 3;
      for (int r = 0; r < repeats; r++) {
        rows.add(row(key, (key * 31 + r) % 17, (long) key * 10 + r + salt));
      }
    }
    return rows;
  }

  private static TableConfig partitionedConfig(boolean sorted, int numPartitions) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(KEY_COL, new ColumnPartitionConfig("Modulo", numPartitions))));
    if (sorted) {
      builder.setSortedColumn(KEY_COL);
    }
    return builder.build();
  }

  /// A row whose key is null: it holds the column's default null value, which is also a legal real key.
  private static GenericRow nullKeyRow(int other, long val) {
    GenericRow row = row(0, other, val);
    row.putDefaultNullValue(KEY_COL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    return row;
  }

  /// Physically sorted by key. The leading run holds real keys equal to the null default value, interleaved with null
  /// keys (which hold that same value), followed by `numKeys` ordinary keys.
  private static List<GenericRow> nullBearingSortedRows(int firstKey, int numKeys, int salt) {
    int defaultKey = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
    List<GenericRow> rows = new ArrayList<>();
    rows.add(row(defaultKey, 1, 1000 + salt));
    rows.add(nullKeyRow(2, 2000 + salt));
    rows.add(row(defaultKey, 3, 3000 + salt));
    rows.add(nullKeyRow(4, 4000 + salt));
    rows.add(nullKeyRow(5, 5000 + salt));
    rows.addAll(sortedRows(firstKey, numKeys, salt));
    return rows;
  }

  private static IndexSegment buildSegment(TableConfig tableConfig, String segmentName, List<GenericRow> rows)
      throws Exception {
    return buildSegment(tableConfig, segmentName, rows, false);
  }

  private static IndexSegment buildSegment(TableConfig tableConfig, String segmentName, List<GenericRow> rows,
      boolean nullHandling)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setDefaultNullHandlingEnabled(nullHandling);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(TEMP_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), ReadMode.mmap);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------------------------------------------

  /// What one driven combine produced.
  private static class Result {
    BaseCombineOperator<?> _combine;
    QueryContext _queryContext;
    List<Object[]> _rows = new ArrayList<>();
    int _numDataBlocks;
    DataSchema _schema;
    BaseResultsBlock _terminal;
    boolean _numGroupsLimitReached;
  }

  /// Builds the query context and combine operator exactly as an MSE leaf would get them: the streaming instance plan
  /// resolves the sorted GROUP BY mode against the segments (as `makeStreamingInstancePlan` does), then segment plan
  /// nodes and a [CombinePlanNode] with a streamer are built from the resolved context.
  private static Result plan(List<IndexSegment> segments, String query, int blockSize) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    List<SegmentContext> segmentContexts = new ArrayList<>();
    for (IndexSegment segment : segments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    // Side effect: applies query options and resolves the sorted GROUP BY mode on queryContext. The plan is dropped.
    PLAN_MAKER.makeStreamingInstancePlan(segmentContexts, queryContext, EXECUTOR, block -> {
    });
    if (blockSize > 0) {
      SortedGroupByDecision decision = queryContext.getSortedGroupByDecision();
      if (decision != null) {
        queryContext.setSortedGroupByDecision(decision.withBlockSize(blockSize));
      }
    }
    List<PlanNode> planNodes = new ArrayList<>();
    for (SegmentContext segmentContext : segmentContexts) {
      planNodes.add(PLAN_MAKER.makeStreamingSegmentPlanNode(segmentContext, queryContext));
    }
    Result result = new Result();
    result._queryContext = queryContext;
    result._combine = new CombinePlanNode(planNodes, queryContext, EXECUTOR, block -> {
    }).run();
    return result;
  }

  /// Drives the combine to completion and collects rows. The sorted streaming combine is pulled until its terminal
  /// metadata block; every other GROUP BY combine returns its whole table in one block.
  private static Result drive(Result result) {
    BaseCombineOperator<?> combine = result._combine;
    if (combine instanceof StreamingSortedGroupByCombineOperator) {
      StreamingSortedGroupByCombineOperator streaming = (StreamingSortedGroupByCombineOperator) combine;
      // As StreamingInstanceResponseOperator does: start (spawns the partition tasks, if any), pull, always stop
      streaming.start();
      try {
        while (true) {
          BaseResultsBlock block = combine.nextBlock();
          if (block instanceof MetadataResultsBlock || block instanceof ExceptionResultsBlock) {
            result._terminal = block;
            break;
          }
          collect(result, (GroupByResultsBlock) block);
          assertTrue(result._numDataBlocks < 1_000_000, "Streaming combine did not terminate");
        }
      } finally {
        streaming.stop();
      }
    } else {
      BaseResultsBlock block = combine.nextBlock();
      if (block instanceof GroupByResultsBlock) {
        collect(result, (GroupByResultsBlock) block);
      } else {
        result._terminal = block;
      }
    }
    return result;
  }

  private static void collect(Result result, GroupByResultsBlock block) {
    result._numDataBlocks++;
    result._numGroupsLimitReached |= block.isNumGroupsLimitReached();
    if (result._schema == null) {
      result._schema = block.getDataSchema();
    }
    assertEquals(block.getDataSchema(), result._schema, "All blocks of one combine must share one schema");
    result._rows.addAll(block.getRows());
  }

  private static Result run(List<IndexSegment> segments, String query, int blockSize) {
    return drive(plan(segments, query, blockSize));
  }

  /// Canonical form of a row list, order-insensitive, typed so a boxing or stored-type regression is visible.
  private static List<String> canonical(List<Object[]> rows) {
    return rows.stream().map(row -> {
      StringBuilder sb = new StringBuilder();
      for (Object cell : row) {
        sb.append(cell == null ? "null" : cell.getClass().getSimpleName() + ":" + cell).append('|');
      }
      return sb.toString();
    }).sorted().collect(Collectors.toList());
  }

  /// Runs `query` with the mode ON and OFF and asserts the same rows and schema. Returns the ON result.
  private static Result assertParity(List<IndexSegment> segments, String query, int blockSize) {
    Result on = run(segments, "SET sortedGroupByMode='ON'; " + query, blockSize);
    Result off = run(segments, query, blockSize);
    assertEquals(canonical(on._rows), canonical(off._rows), "ON and OFF rows differ for: " + query);
    if (on._schema != null) {
      // An empty result carries no data block, hence no schema, on the streaming path
      assertEquals(on._schema, off._schema, "ON and OFF schemas differ for: " + query);
    }
    return on;
  }

  /// Engaged, every segment on the sorted fast path.
  private static void assertEngaged(Result result) {
    assertEngaged(result, result._combine.getChildOperators().size(), 0);
  }

  /// Engaged, with the given number of segments on the sorted fast path and on the hash-then-sort path.
  private static void assertEngaged(Result result, int expectedSorted, int expectedHash) {
    assertTrue(result._queryContext.isSortedGroupByEnabled());
    assertNull(fallbackReason(result._queryContext));
    assertEquals(result._combine.getClass(), StreamingSortedGroupByCombineOperator.class);
    int numSorted = 0;
    int numHash = 0;
    for (Object child : result._combine.getChildOperators()) {
      Object unwrapped = unwrap(child);
      if (unwrapped instanceof StreamingSortedGroupByOperator) {
        numSorted++;
      } else if (unwrapped instanceof HashSortedGroupByOperator) {
        numHash++;
      } else {
        throw new AssertionError("Unexpected child " + unwrapped.getClass());
      }
    }
    assertEquals(numSorted, expectedSorted, "sorted-path segments");
    assertEquals(numHash, expectedHash, "hash-then-sort segments");
    assertEquals(result._combine.toExplainString(), StreamingSortedGroupByCombineOperator.EXPLAIN_NAME);
  }

  private static Map<String, Plan.ExplainNode.AttributeValue> explain(Result result) {
    return result._combine.getExplainInfo().getAttributes();
  }

  private static String fallbackReason(QueryContext queryContext) {
    SortedGroupByDecision decision = queryContext.getSortedGroupByDecision();
    return decision == null ? null : decision.getFallbackDetail();
  }

  private static String description(QueryContext queryContext) {
    SortedGroupByDecision decision = queryContext.getSortedGroupByDecision();
    return decision == null ? null : decision.getDescription();
  }

  private static void assertNotEngaged(Result result, String expectedReasonFragment) {
    assertFalse(result._queryContext.isSortedGroupByEnabled());
    assertNotNull(fallbackReason(result._queryContext));
    assertTrue(fallbackReason(result._queryContext).contains(expectedReasonFragment),
        "Reason was: " + fallbackReason(result._queryContext));
    assertFalse(result._combine instanceof StreamingSortedGroupByCombineOperator);
  }

  private static Object unwrap(Object operator) {
    if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
      return ((AcquireReleaseColumnsSegmentOperator) operator).getChildOperators().get(0);
    }
    return operator;
  }

  /// Rows must be strictly ascending by key (the key is column 0): sorted, and no key twice across blocks.
  private static void assertStrictlyAscendingKeys(List<Object[]> rows) {
    for (int i = 1; i < rows.size(); i++) {
      assertTrue((Integer) rows.get(i - 1)[0] < (Integer) rows.get(i)[0], "Keys not strictly ascending at " + i);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Engagement and parity
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testOnMatchesOffAndStreamsKeysInOrderAsIntermediateResults() {
    Result on = assertParity(_sortedSegments, QUERY, 7);
    assertEngaged(on);
    assertEquals(on._rows.size(), NUM_DISTINCT_KEYS);
    assertStrictlyAscendingKeys(on._rows);
    // 7 groups per block over 140 keys
    assertTrue(on._numDataBlocks > 1, "Expected several blocks, got " + on._numDataBlocks);
    // Intermediate results: the aggregation columns are not the final types
    assertEquals(on._schema.getColumnDataType(1), DataSchema.ColumnDataType.LONG);
    assertTrue(on._terminal instanceof MetadataResultsBlock);
  }

  @Test
  public void testOnMatchesOffAsFinalResults() {
    String query = "SET serverReturnFinalResult=true; " + QUERY;
    Result on = assertParity(_sortedSegments, query, 0);
    assertEngaged(on);
    assertTrue(on._queryContext.isServerReturnFinalResult());
    assertEquals(on._rows.size(), NUM_DISTINCT_KEYS);
    assertStrictlyAscendingKeys(on._rows);
    // SUM/MIN/MAX final results are DOUBLE, COUNT is LONG
    assertEquals(on._schema.getColumnDataType(2), DataSchema.ColumnDataType.DOUBLE);
    Result unpartitioned = assertParity(_sortedSegments,
        "SET serverReturnFinalResultKeyUnpartitioned=true; " + QUERY, 0);
    assertEngaged(unpartitioned);
  }

  @Test
  public void testKeysSpanningSegmentsAreMergedWithKnownTotals() {
    Result on = run(_sortedSegments,
        "SET sortedGroupByMode='ON'; SELECT key, COUNT(*) FROM testTable GROUP BY key" + BIG_LIMIT, 0);
    assertEngaged(on);
    for (Object[] row : on._rows) {
      int key = (Integer) row[0];
      int numSegmentsHoldingKey = 0;
      for (int i = 0; i < NUM_SEGMENTS; i++) {
        if (key >= i * KEY_STRIDE && key < i * KEY_STRIDE + KEYS_PER_SEGMENT) {
          numSegmentsHoldingKey++;
        }
      }
      assertTrue(numSegmentsHoldingKey >= 1);
      assertEquals((long) (Long) row[1], (long) numSegmentsHoldingKey * (1 + key % 3), "count of key " + key);
    }
    // Keys 20..59 are in at least two segments; make sure the fixture really overlaps
    assertTrue(on._rows.stream().anyMatch(row -> (Long) row[1] > 3));
  }

  @Test
  public void testRunSpanningValueBlocksIsMergedAcrossBlocks() {
    Result on = assertParity(_bigRunSegments, QUERY, 0);
    assertEngaged(on);
    assertEquals(on._rows.size(), 3);
    for (Object[] row : on._rows) {
      assertEquals((long) (Long) row[1], 24_000L);
    }
  }

  @Test
  public void testFilterAndDifferentAggregationFunctions() {
    // Final results: AVG and DISTINCTCOUNT intermediates are objects without value equality, so compare finals
    assertEngaged(assertParity(_sortedSegments, "SET serverReturnFinalResult=true; "
        + "SELECT key, SUM(val), AVG(val), DISTINCTCOUNT(other) FROM testTable WHERE other > 3 GROUP BY key"
        + BIG_LIMIT, 11));
  }

  @Test
  public void testNoMatchingRowsStillDeliversNoGroups() {
    Result on = assertParity(_sortedSegments,
        "SELECT key, COUNT(*) FROM testTable WHERE val < 0 GROUP BY key" + BIG_LIMIT, 0);
    assertEngaged(on);
    assertTrue(on._rows.isEmpty());
    assertTrue(on._terminal instanceof MetadataResultsBlock);
  }

  /// Without ORDER BY the existing path keeps only LIMIT groups (arbitrary ones) once LIMIT is below numGroupsLimit, so
  /// a small LIMIT must fall back; the streaming path does not trim and could not return the same rows.
  @Test
  public void testFallbackLimitThatWouldTrim() {
    Result on = run(_sortedSegments,
        "SET sortedGroupByMode='ON'; SELECT key, " + AGGS + " FROM testTable GROUP BY key LIMIT 5", 0);
    assertNotEngaged(on, "LIMIT 5");
    // The multi-threaded combine table can overshoot LIMIT by a few groups when workers race to the cap (seen as 6 or
    // 7), so only assert that it trimmed, not exactly where.
    assertTrue(on._rows.size() >= 5 && on._rows.size() < NUM_DISTINCT_KEYS, "rows: " + on._rows.size());
    // The default LIMIT of 10 is a LIMIT too
    assertNotEngaged(run(_sortedSegments,
        "SET sortedGroupByMode='ON'; SELECT key, " + AGGS + " FROM testTable GROUP BY key", 0), "LIMIT 10");
  }

  @Test
  public void testOffByDefaultReportsExistingPath() {
    Result off = run(_sortedSegments, QUERY, 0);
    assertFalse(off._queryContext.isSortedGroupByEnabled());
    assertNull(fallbackReason(off._queryContext), "Never requested, so nothing to explain");
    assertFalse(off._combine instanceof StreamingSortedGroupByCombineOperator);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*sortedGroupByMode.*")
  public void testInvalidModeIsRejected() {
    plan(_sortedSegments, "SET sortedGroupByMode='SOMETIMES'; " + QUERY, 0);
  }

  @Test
  public void testNonStreamingPlanIgnoresTheOption() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SET sortedGroupByMode='ON'; " + QUERY);
    List<SegmentContext> segmentContexts = new ArrayList<>();
    for (IndexSegment segment : _sortedSegments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    PLAN_MAKER.makeInstancePlan(segmentContexts, queryContext, EXECUTOR);
    assertEquals(queryContext.getSortedGroupByMode(), SortedGroupByMode.OFF);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fallbacks: identical results, reported as not engaged
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testFallbackMultiColumnKey() {
    Result on = assertParity(_sortedSegments,
        "SELECT key, other, COUNT(*) FROM testTable GROUP BY key, other" + BIG_LIMIT, 0);
    assertNotEngaged(on, "multiple group-by expressions");
  }

  @Test
  public void testFallbackExpressionOnSortedColumn() {
    Result on = assertParity(_sortedSegments,
        "SELECT key + 1, COUNT(*) FROM testTable GROUP BY key + 1" + BIG_LIMIT, 0);
    assertNotEngaged(on, "not a plain column");
  }

  /// A key no segment is sorted on is no longer a fallback: every segment hashes, sorts its groups and joins the merge.
  @Test
  public void testNonSortedKeyRunsHashThenSortOnEverySegment() {
    Result on = assertParity(_sortedSegments, "SELECT other, COUNT(*) FROM testTable GROUP BY other" + BIG_LIMIT, 4);
    assertEngaged(on, 0, NUM_SEGMENTS);
    assertTrue(on._numDataBlocks > 1, "Expected several blocks");
    List<Object[]> rows = on._rows;
    for (int i = 1; i < rows.size(); i++) {
      assertTrue((Integer) rows.get(i - 1)[0] < (Integer) rows.get(i)[0], "Keys not strictly ascending at " + i);
    }
  }

  @Test
  public void testFallbackOrderByThatWouldTrim() {
    Result on = assertParity(_sortedSegments,
        "SELECT key, " + AGGS + " FROM testTable GROUP BY key ORDER BY key LIMIT 5", 0);
    assertNotEngaged(on, "ORDER BY");
    Result onByAgg = assertParity(_sortedSegments,
        "SELECT key, " + AGGS + " FROM testTable GROUP BY key ORDER BY COUNT(*) DESC, key LIMIT 5", 0);
    assertNotEngaged(onByAgg, "ORDER BY");
  }

  @Test
  public void testFallbackHaving() {
    Result on = assertParity(_sortedSegments,
        "SELECT key, " + AGGS + " FROM testTable GROUP BY key HAVING COUNT(*) > 3" + BIG_LIMIT, 0);
    assertNotEngaged(on, "HAVING");
  }

  @Test
  public void testFallbackGroupingSets() {
    for (String groupBy : new String[]{"GROUPING SETS ((key))", "ROLLUP(key)"}) {
      Result on = run(_sortedSegments, "SET sortedGroupByMode='ON'; SELECT key, COUNT(*), SUM(val) FROM testTable "
          + "GROUP BY " + groupBy + BIG_LIMIT, 0);
      assertNotEngaged(on, "grouping sets");
      Result off = run(_sortedSegments, "SELECT key, COUNT(*), SUM(val) FROM testTable GROUP BY " + groupBy + BIG_LIMIT,
          0);
      assertEquals(canonical(on._rows), canonical(off._rows), groupBy);
    }
  }

  @Test
  public void testFallbackFilteredAggregations() {
    Result on = assertParity(_sortedSegments,
        "SELECT key, COUNT(*), SUM(val) FILTER (WHERE other > 3), MAX(val) FROM testTable GROUP BY key" + BIG_LIMIT, 0);
    assertNotEngaged(on, "filtered aggregations");
  }

  @Test
  public void testFallbackLimitZero() {
    Result on = run(_sortedSegments,
        "SET sortedGroupByMode='ON'; SELECT key, " + AGGS + " FROM testTable GROUP BY key LIMIT 0", 0);
    assertNotEngaged(on, "LIMIT 0");
    assertTrue(on._rows.isEmpty());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // No limits, no trimming
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testNumGroupsLimitIsNotApplied() {
    String limited = "SET numGroupsLimit=10; ";
    // LIMIT must be at least numGroupsLimit or the limit itself would trim, see testFallbackLimitThatWouldTrim
    Result off = run(_sortedSegments, limited + QUERY, 0);
    assertTrue(off._rows.size() < NUM_DISTINCT_KEYS,
        "The existing path must cap groups at numGroupsLimit, else this test is vacuous");
    Result on = run(_sortedSegments, limited + "SET sortedGroupByMode='ON'; " + QUERY, 0);
    assertEngaged(on);
    assertEquals(on._rows.size(), NUM_DISTINCT_KEYS, "Streaming path must return every group");
    Set<Integer> keys = new TreeSet<>();
    for (Object[] row : on._rows) {
      keys.add((Integer) row[0]);
    }
    assertEquals(keys.size(), NUM_DISTINCT_KEYS);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Termination and lifecycle
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testCancellationIsHonoredMidMerge() {
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      Result result = plan(_sortedSegments, "SET sortedGroupByMode='ON'; " + QUERY, 5);
      assertEngaged(result);
      BaseResultsBlock first = result._combine.nextBlock();
      assertTrue(first instanceof GroupByResultsBlock, "Expected a data block, got " + first.getClass());
      QueryThreadContext.get().getExecutionContext().terminate(QueryErrorCode.QUERY_CANCELLATION, "cancelled by test");
      BaseResultsBlock next = result._combine.nextBlock();
      assertTrue(next instanceof ExceptionResultsBlock, "Expected an exception block, got " + next.getClass());
    }
  }

  @Test
  public void testDeadlineIsHonoredDuringTheMerge() {
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      Result result = plan(_sortedSegments, "SET sortedGroupByMode='ON'; " + QUERY, 0);
      result._queryContext.setEndTimeMs(System.currentTimeMillis() - 1000);
      BaseResultsBlock block = result._combine.nextBlock();
      assertTrue(block instanceof ExceptionResultsBlock, "Expected an exception block, got " + block.getClass());
    }
  }

  @Test
  public void testStopIsIdempotentAndEndsTheMerge() {
    Result result = plan(_sortedSegments, "SET sortedGroupByMode='ON'; " + QUERY, 5);
    assertTrue(result._combine.nextBlock() instanceof GroupByResultsBlock);
    ((BaseStreamingCombineOperator<?>) result._combine).stop();
    ((BaseStreamingCombineOperator<?>) result._combine).stop();
    assertTrue(result._combine.nextBlock() instanceof MetadataResultsBlock, "A stopped merge must not resume");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Unsorted, mixed and consuming segments (hash then sort)
  // ---------------------------------------------------------------------------------------------------------------

  /// Sorted, unsorted and consuming segments in one query: sorted ones stream, the others hash and sort, one merge.
  @Test
  public void testSortedUnsortedAndConsumingSegmentsMatchOff()
      throws Exception {
    MutableSegmentImpl consuming =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Set.of(), Set.of(), Set.of(), false, true);
    try {
      List<GenericRow> consumingRows = sortedRows(10, KEYS_PER_SEGMENT, 3);
      Collections.shuffle(consumingRows, new Random(3));
      for (GenericRow row : consumingRows) {
        consuming.index(row, null);
      }
      List<IndexSegment> segments = new ArrayList<>(_sortedSegments);
      segments.add(_unsortedSegment);
      segments.add(consuming);
      for (int blockSize : new int[]{0, 3}) {
        Result on = assertParity(segments, QUERY, blockSize);
        assertEngaged(on, NUM_SEGMENTS, 2);
        assertStrictlyAscendingKeys(on._rows);
        assertEquals(explain(on).get("numMergeGroups").getLong(), 1L);
        assertFalse(explain(on).get("parallel").getBool());
        assertTrue(description(on._queryContext).contains("5/7 segments sorted"),
            description(on._queryContext));
      }
      // Final results as well: a hash-then-sort group is final-converted exactly like a sorted one
      assertEngaged(assertParity(segments, "SET serverReturnFinalResult=true; SELECT key, SUM(val), AVG(val), "
          + "DISTINCTCOUNT(other) FROM testTable GROUP BY key" + BIG_LIMIT, 9), NUM_SEGMENTS, 2);
    } finally {
      consuming.destroy();
    }
  }

  @Test
  public void testOnlyUnsortedAndConsumingSegments()
      throws Exception {
    MutableSegmentImpl consuming =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Set.of(), Set.of(), Set.of(), false, true);
    try {
      for (GenericRow row : sortedRows(30, KEYS_PER_SEGMENT, 3)) {
        consuming.index(row, null);
      }
      Result on = assertParity(List.of(_unsortedSegment, consuming), QUERY, 0);
      assertEngaged(on, 0, 2);
      assertStrictlyAscendingKeys(on._rows);
    } finally {
      consuming.destroy();
    }
  }

  /// The hash path's own numGroupsLimit still applies to a segment it aggregates, and its flag survives the merge.
  @Test
  public void testHashPathKeepsItsNumGroupsLimitFlag() {
    String query = "SET numGroupsLimit=10; " + QUERY;
    Result off = run(List.of(_unsortedSegment), query, 0);
    Result on = run(List.of(_unsortedSegment), "SET sortedGroupByMode='ON'; " + query, 0);
    assertEngaged(on, 0, 1);
    assertEquals(on._rows.size(), 10);
    assertEquals(on._rows.size(), off._rows.size());
    assertTrue(off._numGroupsLimitReached);
    assertTrue(on._numGroupsLimitReached, "The limit flag of the hash aggregation must reach the merged blocks");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // AUTO
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void testAutoEngagesAboveTheSortedRatio() {
    List<IndexSegment> segments = new ArrayList<>(_sortedSegments);
    segments.add(_unsortedSegment);
    // 5 of 6 sorted = 0.83 >= the 0.8 default
    Result auto = run(segments, "SET sortedGroupByMode='AUTO'; " + QUERY, 0);
    assertEngaged(auto, NUM_SEGMENTS, 1);
    assertEquals(explain(auto).get("decision").getString(), description(auto._queryContext));
    assertTrue(explain(auto).get("decision").getString().startsWith("AUTO: 5/6 segments sorted (ratio 0.83 >= "),
        explain(auto).get("decision").getString());
    assertEquals(canonical(auto._rows), canonical(run(segments, QUERY, 0)._rows));

    // The ratio is the one option of the streaming selection merge
    List<IndexSegment> half = new ArrayList<>(_sortedSegments.subList(0, 3));
    half.add(_unsortedSegment);
    half.add(_unsortedSegment);
    half.add(_unsortedSegment);
    Result lowered = run(half, "SET sortedGroupByMode='AUTO'; SET sortedSelectionMergeAutoMinSortedRatio=0.5; " + QUERY,
        0);
    assertEngaged(lowered, 3, 3);
    assertEquals(canonical(lowered._rows), canonical(run(half, QUERY, 0)._rows));
  }

  @Test
  public void testAutoFallsBackBelowTheSortedRatio() {
    List<IndexSegment> segments = new ArrayList<>(_sortedSegments.subList(0, 3));
    segments.add(_unsortedSegment);
    segments.add(_unsortedSegment);
    segments.add(_unsortedSegment);
    Result auto = run(segments, "SET sortedGroupByMode='AUTO'; " + QUERY, 0);
    assertNotEngaged(auto, "AUTO: 3/6 segments sorted (ratio 0.50 < threshold 0.80)");
    assertEquals(description(auto._queryContext), fallbackReason(auto._queryContext));
    // Falling back runs the existing path, with identical results
    assertEquals(canonical(auto._rows), canonical(run(segments, QUERY, 0)._rows));
    // ON engages regardless of the ratio
    assertEngaged(run(segments, "SET sortedGroupByMode='ON'; " + QUERY, 0), 3, 3);
  }

  @Test
  public void testAutoStillHonorsTheQueryShapeRules() {
    Result auto = run(_sortedSegments, "SET sortedGroupByMode='AUTO'; SELECT key, COUNT(*) FROM testTable GROUP BY key "
        + "ORDER BY key LIMIT 5", 0);
    assertNotEngaged(auto, "ORDER BY");
    assertNull(description(auto._queryContext));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Null keys
  // ---------------------------------------------------------------------------------------------------------------

  /// A real key equal to the null default value and null keys, in several segments of every kind: with null handling on
  /// the nulls form a group of their own that sorts last and is merged across segments.
  @Test
  public void testNullKeysAreTheirOwnGroupWithNullHandlingOn()
      throws Exception {
    MutableSegmentImpl consuming =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Set.of(), Set.of(), Set.of(), false, true);
    try {
      for (GenericRow row : nullBearingSortedRows(2, 7, 300)) {
        consuming.index(row, null);
      }
      String nullHandling = "SET enableNullHandling=true; ";
      int defaultKey = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
      List<List<IndexSegment>> cases = new ArrayList<>();
      cases.add(_nullBearingSortedSegments);
      List<IndexSegment> mixed = new ArrayList<>(_nullBearingSortedSegments);
      mixed.add(_nullBearingUnsortedSegment);
      mixed.add(consuming);
      cases.add(mixed);
      cases.add(List.of(_nullBearingUnsortedSegment, consuming));
      for (List<IndexSegment> segments : cases) {
        for (String aggs : new String[]{"COUNT(*), SUM(val), MIN(val), MAX(val)", "COUNT(val), SUM(other)"}) {
          String query = nullHandling + "SELECT key, " + aggs + " FROM testTable GROUP BY key" + BIG_LIMIT;
          Result on = assertParity(segments, query, 0);
          int numSorted = (int) segments.stream().filter(_nullBearingSortedSegments::contains).count();
          assertEngaged(on, numSorted, segments.size() - numSorted);
          // The real default-valued key and the null key are two groups, null last
          long numNullGroups = on._rows.stream().filter(row -> row[0] == null).count();
          long numDefaultGroups = on._rows.stream().filter(row -> Integer.valueOf(defaultKey).equals(row[0])).count();
          assertEquals(numNullGroups, 1L, "exactly one null group in " + query);
          assertEquals(numDefaultGroups, 1L, "exactly one real default-valued group in " + query);
          assertNull(on._rows.get(on._rows.size() - 1)[0], "the null group is emitted last");
        }
      }
      // Known totals: each fixture segment holds 3 null rows and 2 real default-valued rows
      Result counts = run(_nullBearingSortedSegments,
          nullHandling + "SET sortedGroupByMode='ON'; SELECT key, COUNT(*) FROM testTable GROUP BY key" + BIG_LIMIT, 0);
      for (Object[] row : counts._rows) {
        if (row[0] == null) {
          assertEquals((long) (Long) row[1], 6L);
        } else if (Integer.valueOf(defaultKey).equals(row[0])) {
          assertEquals((long) (Long) row[1], 4L);
        }
      }
    } finally {
      consuming.destroy();
    }
  }

  /// With null handling off a null row is just a row holding the default value: one group, as today.
  @Test
  public void testNullKeysWithNullHandlingOffMatchOff() {
    List<IndexSegment> segments = new ArrayList<>(_nullBearingSortedSegments);
    segments.add(_nullBearingUnsortedSegment);
    Result on = assertParity(segments, QUERY, 0);
    assertEngaged(on, 2, 1);
    assertTrue(on._rows.stream().noneMatch(row -> row[0] == null));
    Result explicitOff = assertParity(segments, "SET enableNullHandling=false; " + QUERY, 0);
    assertEngaged(explicitOff, 2, 1);
  }

  /// Null-bearing segments no longer need the non-null flag: the old fallback is gone.
  @Test
  public void testNullHandlingOnNoLongerFallsBackOnAnUnflaggedColumn() {
    // The fixture segments were built with null handling off, so their key column is not flagged non-null
    Result on = assertParity(_sortedSegments, "SET enableNullHandling=true; " + QUERY, 0);
    assertEngaged(on);
    assertStrictlyAscendingKeys(on._rows);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Funnel functions: OFF vs ON vs AUTO
  // ---------------------------------------------------------------------------------------------------------------

  private static final String[] FUNNEL_MODES =
      {"", ", 'STRICT_ORDER'", ", 'STRICT_INCREASE'", ", 'STRICT_DEDUPLICATION'", ", 'KEEP_ALL'",
          ", 'STRICT_ORDER', 'STRICT_INCREASE'", ", 'STRICT_DEDUPLICATION', 'STRICT_INCREASE'"};

  /// Rows with every funnel intermediate cell reduced to something with value equality: its sorted event list and
  /// the final result extracted from it. A final-result row is compared as it is.
  private static List<String> canonicalFunnel(Result result) {
    AggregationFunction[] functions = result._queryContext.getAggregationFunctions();
    List<Object[]> rows = new ArrayList<>();
    for (Object[] row : result._rows) {
      Object[] canonical = row.clone();
      for (int i = 0; i < functions.length; i++) {
        Object cell = row[1 + i];
        if (cell instanceof PriorityQueue) {
          List<Object> events = new ArrayList<>((PriorityQueue<?>) cell);
          events.sort(null);
          canonical[1 + i] = events + " -> " + functions[i].extractFinalResult(cell);
        }
      }
      rows.add(canonical);
    }
    return canonical(rows);
  }

  /// OFF, ON and AUTO give the same rows for `query` over the funnel segments. Returns the ON result.
  private Result assertFunnelParity(String query, int blockSize) {
    Result off = run(_funnelSegments, query, blockSize);
    Result on = run(_funnelSegments, "SET sortedGroupByMode='ON'; " + query, blockSize);
    Result auto = run(_funnelSegments, "SET sortedGroupByMode='AUTO'; " + query, blockSize);
    assertFalse(off._combine instanceof StreamingSortedGroupByCombineOperator);
    assertEngaged(on);
    assertEngaged(auto);
    assertEquals(canonicalFunnel(on), canonicalFunnel(off), "ON differs from OFF for: " + query);
    assertEquals(canonicalFunnel(auto), canonicalFunnel(off), "AUTO differs from OFF for: " + query);
    assertEquals(on._schema, off._schema, "ON and OFF schemas differ for: " + query);
    assertEquals(auto._schema, off._schema, "AUTO and OFF schemas differ for: " + query);
    assertEveryKeyOnce(on._rows);
    assertEquals(on._rows.size(), NUM_FUNNEL_USERS);
    return on;
  }

  private void assertFunnelParityForAllModes(String function) {
    for (String maxStepDuration : new String[]{"", ", 'maxStepDuration=15'"}) {
      for (String mode : FUNNEL_MODES) {
        String call = function + "(val, 60, 3, other = 1, other = 2, other = 3" + mode + maxStepDuration + ")";
        String query = "SELECT key, " + call + ", COUNT(*) FROM testTable GROUP BY key" + BIG_LIMIT;
        // Intermediate results (the funnel event queues), small blocks, and final results
        assertFunnelParity(query, 4);
        assertFunnelParity("SET serverReturnFinalResult=true; " + query, 0);
      }
    }
  }

  @Test
  public void testFunnelMaxStepMatchesOffAcrossSegmentsInEveryMode() {
    assertFunnelParityForAllModes("FUNNEL_MAX_STEP");
    // The fixture must actually exercise several depths, or the parity would be vacuous
    Result final1 = run(_funnelSegments, "SET sortedGroupByMode='ON'; SET serverReturnFinalResult=true; "
        + "SELECT key, FUNNEL_MAX_STEP(val, 60, 3, other = 1, other = 2, other = 3) FROM testTable GROUP BY key"
        + BIG_LIMIT, 0);
    assertTrue(final1._rows.stream().map(row -> row[1]).distinct().count() >= 3,
        "Expected several distinct max steps: " + canonicalFunnel(final1));
  }

  @Test
  public void testFunnelMaxStepWithEntryMatchesOffAcrossSegmentsInEveryMode() {
    assertFunnelParityForAllModes("FUNNEL_MAX_STEP_WITH_ENTRY");
    Result final1 = run(_funnelSegments, "SET sortedGroupByMode='ON'; SET serverReturnFinalResult=true; "
        + "SELECT key, FUNNEL_MAX_STEP_WITH_ENTRY(val, 60, 3, other = 1, other = 2, other = 3) FROM testTable "
        + "GROUP BY key" + BIG_LIMIT, 0);
    assertTrue(final1._rows.stream().map(row -> row[1]).distinct().count() >= 5,
        "Expected several distinct [maxStep, entry] results: " + canonicalFunnel(final1));
  }

  /// Both funnel functions and the plain aggregations in one query, merged over the same groups.
  @Test
  public void testFunnelsNextToPlainAggregationsMatchOff() {
    String query = "SELECT key, COUNT(*), FUNNEL_MAX_STEP(val, 60, 3, other = 1, other = 2, other = 3, 'STRICT_ORDER'), "
        + "SUM(val), FUNNEL_MAX_STEP_WITH_ENTRY(val, 60, 3, other = 1, other = 2, other = 3, 'KEEP_ALL'), MIN(val), "
        + "MAX(val) FROM testTable GROUP BY key" + BIG_LIMIT;
    assertFunnelParity(query, 3);
    assertFunnelParity("SET serverReturnFinalResult=true; " + query, 0);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Per-partition parallel merge
  // ---------------------------------------------------------------------------------------------------------------

  private static void assertEveryKeyOnce(List<Object[]> rows) {
    Set<Object> keys = new TreeSet<>();
    for (Object[] row : rows) {
      assertTrue(keys.add(row[0]), "Key emitted twice: " + row[0]);
    }
  }

  private static long explainLong(Result result, String key) {
    return explain(result).get(key).getLong();
  }

  @Test
  public void testFixturesCarrySinglePartitionMetadata() {
    for (IndexSegment segment : _partitionedSortedSegments) {
      DataSourceMetadata metadata =
          segment.getDataSource(KEY_COL, SCHEMA).getDataSourceMetadata();
      assertNotNull(metadata.getPartitionFunction(), segment.getSegmentName());
      assertEquals(metadata.getPartitions().size(), 1, segment.getSegmentName());
      assertEquals(metadata.getPartitionFunction(), new ModuloPartitionFunction(NUM_PARTITIONS, null));
    }
  }

  @Test
  public void testOneMergeTaskPerPartition() {
    for (int blockSize : new int[]{0, 3}) {
      Result on = assertParity(_partitionedSortedSegments, QUERY, blockSize);
      assertEngaged(on);
      assertEquals(explainLong(on, "numMergeGroups"), (long) NUM_PARTITIONS);
      assertTrue(explain(on).get("partitioned").getBool());
      assertTrue(explain(on).get("parallel").getBool());
      List<String> groups = explain(on).get("mergeGroups").getStringList().getValuesList();
      assertEquals(groups, List.of("partition 0: 2 segments", "partition 1: 2 segments", "partition 2: 2 segments",
          "partition 3: 2 segments"));
      assertEveryKeyOnce(on._rows);
      assertTrue(on._terminal instanceof MetadataResultsBlock);
      if (blockSize == 3) {
        assertTrue(on._numDataBlocks > NUM_PARTITIONS, "Expected several blocks, got " + on._numDataBlocks);
      }
    }
    // Final results through the same workers
    Result finalResults =
        assertParity(_partitionedSortedSegments, "SET serverReturnFinalResult=true; " + QUERY, 5);
    assertEngaged(finalResults);
    assertEveryKeyOnce(finalResults._rows);
  }

  /// Hash-then-sort segments run on the worker threads, so the thread-local group-key state of the hash aggregation is
  /// exercised concurrently; repeat to give a data race a chance to show.
  @Test
  public void testPartitionedHashSegmentsRunConcurrentlyWithoutSharingGroupKeyState() {
    for (int i = 0; i < 25; i++) {
      Result on = assertParity(_partitionedUnsortedSegments, QUERY, 4);
      assertEngaged(on, 0, _partitionedUnsortedSegments.size());
      assertEquals(explainLong(on, "numMergeGroups"), (long) NUM_PARTITIONS);
      assertEveryKeyOnce(on._rows);
    }
  }

  @Test
  public void testPartitionedMixOfSortedAndHashSegments() {
    List<IndexSegment> segments = new ArrayList<>(_partitionedSortedSegments);
    segments.addAll(_partitionedUnsortedSegments);
    Result on = assertParity(segments, QUERY, 6);
    assertEngaged(on, _partitionedSortedSegments.size(), _partitionedUnsortedSegments.size());
    assertEquals(explainLong(on, "numMergeGroups"), (long) NUM_PARTITIONS);
    assertEveryKeyOnce(on._rows);
  }

  @Test
  public void testConsumingSegmentJoinsItsPartition()
      throws Exception {
    // A consuming segment of partition 2 sits next to the immutable ones; its partition metadata comes from the rows
    MutableSegmentImpl consuming = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, KEY_COL,
        new ModuloPartitionFunction(NUM_PARTITIONS, null), 2, false);
    try {
      for (GenericRow row : partitionRows(2, NUM_PARTITIONS, 0, 100, 9)) {
        consuming.index(row, null);
      }
      List<IndexSegment> segments = new ArrayList<>(_partitionedSortedSegments);
      segments.add(consuming);
      Result on = assertParity(segments, QUERY, 5);
      assertEngaged(on, _partitionedSortedSegments.size(), 1);
      assertEquals(explainLong(on, "numMergeGroups"), (long) NUM_PARTITIONS);
      assertTrue(explain(on).get("mergeGroups").getStringList().getValuesList().contains("partition 2: 3 segments"));
      assertEveryKeyOnce(on._rows);
    } finally {
      consuming.destroy();
    }
  }

  /// Null keys across partitions. A null row holds the column default value, so the segment records the partition of
  /// that value next to its own stream partition: a segment with nulls outside the default value's partition reports
  /// several partitions and forces one merge group, and the null group can therefore come out of at most one merge
  /// group. Both shapes must emit the null group exactly once (last, when there is one merge group).
  @Test
  public void testNullKeysInSeveralPartitionsAreOneGroup()
      throws Exception {
    int nullPartition = new ModuloPartitionFunction(NUM_PARTITIONS, null)
        .getPartition(String.valueOf(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT));
    int otherPartition = (nullPartition + 1) % NUM_PARTITIONS;
    // Nulls only in the default value's partition: the partitions stay separate merge groups
    assertNullGroupOnce(List.of(nullPartition), NUM_PARTITIONS, 1);
    // Nulls in two partitions (one of them foreign to the default value): the spanning segment makes one merge group
    assertNullGroupOnce(List.of(nullPartition, otherPartition), 1, 2);
    // Nulls in two partitions, none of them the default value's
    assertNullGroupOnce(List.of(otherPartition, (otherPartition + 1) % NUM_PARTITIONS), 1, 2);
  }

  /// One consuming segment per partition in `nullPartitions` (null handling on, two null rows each), plus the two
  /// immutable segments of every other partition, which carry no nulls.
  private void assertNullGroupOnce(List<Integer> nullPartitions, int expectedMergeGroups, int numNullSegments)
      throws Exception {
    List<MutableSegmentImpl> consuming = new ArrayList<>();
    try {
      List<IndexSegment> segments = new ArrayList<>();
      for (int partition : nullPartitions) {
        MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, KEY_COL,
            new ModuloPartitionFunction(NUM_PARTITIONS, null), partition, false, true);
        consuming.add(segment);
        for (GenericRow row : partitionRows(partition, NUM_PARTITIONS, 0, 40, 20 + partition)) {
          segment.index(row, null);
        }
        segment.index(nullKeyRow(1 + partition, 700 + partition), null);
        segment.index(nullKeyRow(3 + partition, 800 + partition), null);
        segments.add(segment);
      }
      for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
        if (!nullPartitions.contains(partition)) {
          segments.add(_partitionedSortedSegments.get(partition * 2));
          segments.add(_partitionedSortedSegments.get(partition * 2 + 1));
        }
      }
      long expectedNullSum = 0;
      for (int partition : nullPartitions) {
        expectedNullSum += 700L + partition + 800 + partition;
      }
      String nullHandling = "SET enableNullHandling=true; ";
      for (String options : new String[]{"", "SET serverReturnFinalResult=true; "}) {
        for (int blockSize : new int[]{0, 3}) {
          String query = nullHandling + options + "SELECT key, COUNT(*), SUM(val), MIN(val), MAX(val) FROM testTable "
              + "GROUP BY key" + BIG_LIMIT;
          Result on = assertParity(segments, query, blockSize);
          assertTrue(on._queryContext.isSortedGroupByEnabled());
          assertEquals(on._combine.getClass(), StreamingSortedGroupByCombineOperator.class);
          assertEquals(explainLong(on, "numMergeGroups"), (long) expectedMergeGroups, "partitions " + nullPartitions);
          assertEquals(on._rows.stream().filter(row -> row[0] == null).count(), 1L,
              "exactly one null group in " + query);
          if (expectedMergeGroups == 1) {
            assertNull(on._rows.get(on._rows.size() - 1)[0], "the null group is emitted last");
          }
          for (Object[] row : on._rows) {
            if (row[0] == null) {
              assertEquals((long) (Long) row[1], 2L * numNullSegments);
              assertEquals((double) (Double) row[2], (double) expectedNullSum);
            }
          }
          List<Object> keys = new ArrayList<>();
          for (Object[] row : on._rows) {
            assertFalse(keys.contains(row[0]), "Key emitted twice: " + row[0]);
            keys.add(row[0]);
          }
        }
      }
    } finally {
      for (MutableSegmentImpl segment : consuming) {
        segment.destroy();
      }
    }
  }

  @Test
  public void testNoPartitionMetadataIsOneMergeGroup() {
    Result on = assertParity(_sortedSegments, QUERY, 0);
    assertEngaged(on);
    assertEquals(explainLong(on, "numMergeGroups"), 1L);
    assertFalse(explain(on).get("partitioned").getBool());
    assertFalse(explain(on).get("parallel").getBool());
    assertTrue(explain(on).get("notPartitionedBecause").getString().contains("no partition metadata"),
        explain(on).get("notPartitionedBecause").getString());
    // One partitioned and one unpartitioned segment is still one group
    List<IndexSegment> someUnpartitioned = new ArrayList<>(_partitionedSortedSegments);
    someUnpartitioned.add(_sortedSegments.get(0));
    Result partial = assertParity(someUnpartitioned, QUERY, 0);
    assertEngaged(partial);
    assertEquals(explainLong(partial, "numMergeGroups"), 1L);
    assertStrictlyAscendingKeys(partial._rows);
  }

  @Test
  public void testMixedPartitionFunctionsAreOneMergeGroup() {
    List<IndexSegment> segments = new ArrayList<>(_partitionedSortedSegments);
    segments.add(_modulo8Segment);
    Result on = assertParity(segments, QUERY, 0);
    assertEngaged(on);
    assertEquals(explainLong(on, "numMergeGroups"), 1L);
    assertFalse(explain(on).get("partitioned").getBool());
    assertTrue(explain(on).get("notPartitionedBecause").getString().contains("different partition functions"),
        explain(on).get("notPartitionedBecause").getString());
    assertStrictlyAscendingKeys(on._rows);
  }

  @Test
  public void testSegmentSpanningPartitionsIsOneMergeGroup() {
    assertTrue(_spanningSegment.getDataSource(KEY_COL, SCHEMA).getDataSourceMetadata().getPartitions().size() > 1);
    List<IndexSegment> segments = new ArrayList<>(_partitionedSortedSegments);
    segments.add(_spanningSegment);
    Result on = assertParity(segments, QUERY, 0);
    assertEngaged(on);
    assertEquals(explainLong(on, "numMergeGroups"), 1L);
    assertTrue(explain(on).get("notPartitionedBecause").getString().contains("spans"),
        explain(on).get("notPartitionedBecause").getString());
    assertStrictlyAscendingKeys(on._rows);
  }

  @Test
  public void testStopWhileWorkersAreBlockedOnTheQueueReleasesEverything() {
    // One-block queue and one-row blocks: every partition task blocks offering its second block
    Result result = plan(_partitionedSortedSegments,
        "SET sortedGroupByMode='ON'; SET maxStreamingPendingBlocks=1; " + QUERY, 1);
    StreamingSortedGroupByCombineOperator combine = (StreamingSortedGroupByCombineOperator) result._combine;
    combine.start();
    assertTrue(combine.nextBlock() instanceof GroupByResultsBlock);
    long startMs = System.currentTimeMillis();
    combine.stop();
    combine.stop();
    assertTrue(System.currentTimeMillis() - startMs < 10_000, "stop() must cancel blocked workers, not wait for them");
    assertTrue(combine.nextBlock() instanceof MetadataResultsBlock, "A stopped merge must not resume");
  }

  @Test
  public void testCancellationIsHonoredWithParallelMerges() {
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      Result result = plan(_partitionedSortedSegments, "SET sortedGroupByMode='ON'; " + QUERY, 1);
      StreamingSortedGroupByCombineOperator combine = (StreamingSortedGroupByCombineOperator) result._combine;
      combine.start();
      try {
        assertTrue(combine.nextBlock() instanceof GroupByResultsBlock);
        QueryThreadContext.get().getExecutionContext()
            .terminate(QueryErrorCode.QUERY_CANCELLATION, "cancelled by test");
        BaseResultsBlock next = combine.nextBlock();
        assertTrue(next instanceof ExceptionResultsBlock, "Expected an exception block, got " + next.getClass());
      } finally {
        combine.stop();
      }
    }
  }
}
