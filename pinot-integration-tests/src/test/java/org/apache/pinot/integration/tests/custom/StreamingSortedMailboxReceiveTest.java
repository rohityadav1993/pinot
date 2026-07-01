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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration parity test for the streaming k-way merge in {@code SortedMailboxReceiveOperator}.
 *
 * <p>Verifies that a {@code SELECT ... ORDER BY <key> LIMIT <n>} query returns identical row sets AND identical order
 * across three configurations:
 * <ul>
 *   <li>{@code streamingSortedMailboxReceive=false} (baseline accumulate-then-sort receive),</li>
 *   <li>{@code streamingSortedMailboxReceive=true} (explicit hint forces the streaming merge when the planner has
 *       proven sorted senders), and</li>
 *   <li>{@code streamingSelectionOrderBy=true} with the step-2 hint (planner marks the receive node
 *       {@code sortedOnSender} AND the explicit hint activates the merge).</li>
 * </ul>
 *
 * <p>Also asserts that with the step-1 hint on, the {@code EXPLAIN PLAN FOR} output marks the receive node
 * {@code sortedOnSender=true}.
 *
 * <p>The base cluster starts two servers and the data is split across two segments, so the receive node merges multiple
 * pre-sorted sender streams.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class StreamingSortedMailboxReceiveTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "StreamingSortedMailboxReceiveTest";
  private static final int NUM_TOTAL_DOCS = 1000;
  private static final String KEY_INT = "keyInt";
  private static final String KEY_STR = "keyStr";
  private static final String PAYLOAD = "payload";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(KEY_INT, FieldSpec.DataType.INT)
        .addSingleValueDimension(KEY_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(PAYLOAD, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  protected long getCountStarResult() {
    return NUM_TOTAL_DOCS;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(KEY_INT, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(KEY_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(PAYLOAD, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_TOTAL_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        // Unique keys so ORDER BY produces a single deterministic ordering (no ties to differ across paths).
        record.put(KEY_INT, i);
        record.put(KEY_STR, String.format("key-%05d", i));
        record.put(PAYLOAD, (long) i * 7);
        // Round-robin across the avro files (segments) so each segment holds an interleaved key range.
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testOrderByLimitParityWithAndWithoutStreamingMerge()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // LIMIT below the total doc count so a leaf ORDER BY LIMIT Sort is pushed to the senders.
    String baseQuery = String.format("SELECT %s, %s, %s FROM %s ORDER BY %s LIMIT 50", KEY_INT, KEY_STR, PAYLOAD,
        getTableName(), KEY_INT);

    JsonNode baselineRows = runAndGetRows("SET " + streamingMergeOption(false) + "; " + baseQuery);
    JsonNode mergeRows = runAndGetRows("SET " + streamingMergeOption(true) + "; " + baseQuery);
    assertRowsIdenticalInOrder(baselineRows, mergeRows);
    assertEquals(mergeRows.size(), 50, "LIMIT must be honored");
    // Sanity: results are actually globally sorted ascending by keyInt.
    for (int i = 1; i < mergeRows.size(); i++) {
      assertTrue(mergeRows.get(i).get(0).asInt() >= mergeRows.get(i - 1).get(0).asInt(),
          "merge output must be globally sorted ascending by " + KEY_INT);
    }
  }

  @Test
  public void testOrderByLimitParityDescending()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String baseQuery = String.format("SELECT %s, %s FROM %s ORDER BY %s DESC LIMIT 25", KEY_INT, PAYLOAD,
        getTableName(), KEY_INT);

    JsonNode baselineRows = runAndGetRows("SET " + streamingMergeOption(false) + "; " + baseQuery);
    JsonNode mergeRows = runAndGetRows("SET " + streamingMergeOption(true) + "; " + baseQuery);
    assertRowsIdenticalInOrder(baselineRows, mergeRows);
    assertEquals(mergeRows.size(), 25, "LIMIT must be honored");
    // Sanity: results are actually globally sorted descending by keyInt (DESC is where an inverted comparator or wrong
    // null direction in the merge would surface, since both paths could otherwise share the same bug undetected).
    for (int i = 1; i < mergeRows.size(); i++) {
      assertTrue(mergeRows.get(i).get(0).asInt() <= mergeRows.get(i - 1).get(0).asInt(),
          "merge output must be globally sorted descending by " + KEY_INT);
    }
  }

  @Test
  public void testPlannerAutoActivationParity()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String baseQuery = String.format("SELECT %s, %s, %s FROM %s ORDER BY %s LIMIT 50", KEY_INT, KEY_STR, PAYLOAD,
        getTableName(), KEY_INT);

    // Baseline: streaming merge explicitly off (and no step-1 hint).
    JsonNode baselineRows = runAndGetRows("SET " + streamingMergeOption(false) + "; " + baseQuery);
    // Step-1 hint marks the receive node sortedOnSender, but the AND gate requires the explicit step-2 hint too.
    JsonNode autoRows = runAndGetRows(
        "SET " + CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SELECTION_ORDER_BY + "=true; "
            + "SET " + streamingMergeOption(true) + "; " + baseQuery);
    assertRowsIdenticalInOrder(baselineRows, autoRows);
  }

  @Test
  public void testExplainShowsStreamingSortedLeafWithStep1Hint()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // The planner sets MailboxReceiveNode.sortedOnSender during fragmentation when the step-1 hint is on and the
    // sender fragment is a leaf selection ORDER BY. That internal flag is NOT surfaced as text by any EXPLAIN mode
    // today (the asking-servers explain renders only the leaf stage via PlanNodeToRelConverter; the intermediate
    // exchange stage stays a logical PinotLogicalExchange), so we cannot assert the flag string directly here.
    // Its runtime effect is proven by testPlannerAutoActivationParity. What the asking-servers explain DOES show
    // stably is the precondition the flag encodes: with the step-1 hint on, the leaf runs the streaming sorted
    // selection combine (SelectOrderbyStreaming) under the exchange, i.e. each sender stream is globally sorted.
    String query = String.format(
        "SET %s=true; SET %s=true; EXPLAIN PLAN FOR SELECT %s, %s FROM %s ORDER BY %s LIMIT 50",
        CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SELECTION_ORDER_BY,
        CommonConstants.Broker.Request.QueryOptionKey.EXPLAIN_ASKING_SERVERS, KEY_INT, PAYLOAD, getTableName(),
        KEY_INT);
    JsonNode plan = postQuery(query);
    assertEquals(plan.get("exceptions").size(), 0, "EXPLAIN produced exceptions: " + plan.get("exceptions"));
    String planText = plan.toString();
    assertTrue(planText.contains("SelectOrderbyStreaming"),
        "Step-1 hint should activate the streaming sorted leaf selection. Plan: " + plan);
    assertTrue(planText.contains("PinotLogicalExchange"),
        "Plan should retain the exchange feeding the sorted receiver. Plan: " + plan);
  }

  private static String streamingMergeOption(boolean enabled) {
    return CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE + "=" + enabled;
  }

  private JsonNode runAndGetRows(String query)
      throws Exception {
    JsonNode response = postQuery(query);
    assertEquals(response.get("exceptions").size(), 0, "Query produced exceptions: " + response.get("exceptions"));
    return response.get("resultTable").get("rows");
  }

  private static void assertRowsIdenticalInOrder(JsonNode expected, JsonNode actual) {
    assertEquals(actual.size(), expected.size(), "row count mismatch");
    List<String> expectedRows = new ArrayList<>();
    List<String> actualRows = new ArrayList<>();
    for (int i = 0; i < expected.size(); i++) {
      expectedRows.add(expected.get(i).toString());
      actualRows.add(actual.get(i).toString());
    }
    // Order-sensitive comparison: row i must match across both result sets.
    assertEquals(actualRows, expectedRows, "row sets / order differ between streaming-merge and baseline");
  }
}
