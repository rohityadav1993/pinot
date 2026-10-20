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
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.query.StreamingSortedGroupByOperator;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
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

  private static IndexSegment buildSegment(TableConfig tableConfig, String segmentName, List<GenericRow> rows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
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
      queryContext.setSortedGroupByBlockSize(blockSize);
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
      while (true) {
        BaseResultsBlock block = combine.nextBlock();
        if (block instanceof MetadataResultsBlock || block instanceof ExceptionResultsBlock) {
          result._terminal = block;
          break;
        }
        collect(result, (GroupByResultsBlock) block);
        assertTrue(result._numDataBlocks < 1_000_000, "Streaming combine did not terminate");
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

  private static void assertEngaged(Result result) {
    assertTrue(result._queryContext.isSortedGroupByEnabled());
    assertNull(result._queryContext.getSortedGroupByFallbackReason());
    assertEquals(result._combine.getClass(), StreamingSortedGroupByCombineOperator.class);
    for (Object child : result._combine.getChildOperators()) {
      assertTrue(unwrap(child) instanceof StreamingSortedGroupByOperator, "Child is " + child.getClass());
    }
    assertEquals(result._combine.toExplainString(), StreamingSortedGroupByCombineOperator.EXPLAIN_NAME);
  }

  private static void assertNotEngaged(Result result, String expectedReasonFragment) {
    assertFalse(result._queryContext.isSortedGroupByEnabled());
    assertNotNull(result._queryContext.getSortedGroupByFallbackReason());
    assertTrue(result._queryContext.getSortedGroupByFallbackReason().contains(expectedReasonFragment),
        "Reason was: " + result._queryContext.getSortedGroupByFallbackReason());
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
    assertEquals(on._rows.size(), 5);
    // The default LIMIT of 10 is a LIMIT too
    assertNotEngaged(run(_sortedSegments,
        "SET sortedGroupByMode='ON'; SELECT key, " + AGGS + " FROM testTable GROUP BY key", 0), "LIMIT 10");
  }

  @Test
  public void testOffByDefaultReportsExistingPath() {
    Result off = run(_sortedSegments, QUERY, 0);
    assertFalse(off._queryContext.isSortedGroupByEnabled());
    assertNull(off._queryContext.getSortedGroupByFallbackReason(), "Never requested, so nothing to explain");
    assertFalse(off._combine instanceof StreamingSortedGroupByCombineOperator);
  }

  @Test
  public void testAutoIsNotEngagedYet() {
    Result auto = run(_sortedSegments, "SET sortedGroupByMode='AUTO'; " + QUERY, 0);
    assertNotEngaged(auto, "AUTO");
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

  @Test
  public void testFallbackNonSortedKey() {
    Result on = assertParity(_sortedSegments, "SELECT other, COUNT(*) FROM testTable GROUP BY other" + BIG_LIMIT, 0);
    assertNotEngaged(on, "not sorted");
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
  public void testFallbackWhenOneSegmentIsNotSorted() {
    List<IndexSegment> segments = new ArrayList<>(_sortedSegments);
    segments.add(_unsortedSegment);
    Result on = assertParity(segments, QUERY, 0);
    assertNotEngaged(on, "not sorted");
  }

  @Test
  public void testFallbackWhenOneSegmentIsConsuming()
      throws Exception {
    MutableSegmentImpl consuming =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Set.of(), Set.of(), Set.of(), false, true);
    try {
      for (GenericRow row : sortedRows(0, KEYS_PER_SEGMENT, 3)) {
        consuming.index(row, null);
      }
      List<IndexSegment> segments = new ArrayList<>(_sortedSegments);
      segments.add(consuming);
      Result on = assertParity(segments, QUERY, 0);
      assertNotEngaged(on, "consuming segment");
    } finally {
      consuming.destroy();
    }
  }

  @Test
  public void testFallbackWhenNullHandlingIsOnAndColumnIsNotFlaggedNonNull() {
    // The fixture segments were built with null handling off, so their key column is not flagged non-null
    Result on = assertParity(_sortedSegments, "SET enableNullHandling=true; " + QUERY, 0);
    assertNotEngaged(on, "null handling");
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
}
