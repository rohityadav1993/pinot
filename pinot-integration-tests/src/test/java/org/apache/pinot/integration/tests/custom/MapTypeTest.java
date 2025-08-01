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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class MapTypeTest extends CustomDataQueryClusterIntegrationTest {

  // Default settings
  protected static final String DEFAULT_TABLE_NAME = "MapTypeTest";
  private static final int NUM_DOCS = 1000;
  private static final String STRING_KEY_MAP_FIELD_NAME = "stringKeyMap";
  private static final String INT_KEY_MAP_FIELD_NAME = "intKeyMap";
  private static final String STRING_KEY_MAP_STR_FIELD_NAME = "stringKeyMapStr";
  private static final String INT_KEY_MAP_STR_FIELD_NAME = "intKeyMapStr";
  private int _setSelectionDefaultDocCount = 10;

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(STRING_KEY_MAP_FIELD_NAME + SchemaUtils.MAP_KEY_COLUMN_SUFFIX,
            FieldSpec.DataType.STRING)
        .addMultiValueDimension(STRING_KEY_MAP_FIELD_NAME + SchemaUtils.MAP_VALUE_COLUMN_SUFFIX, FieldSpec.DataType.INT)
        .addMultiValueDimension(INT_KEY_MAP_FIELD_NAME + SchemaUtils.MAP_KEY_COLUMN_SUFFIX, FieldSpec.DataType.INT)
        .addMultiValueDimension(INT_KEY_MAP_FIELD_NAME + SchemaUtils.MAP_VALUE_COLUMN_SUFFIX, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_KEY_MAP_STR_FIELD_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(INT_KEY_MAP_STR_FIELD_NAME, FieldSpec.DataType.STRING).build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig(STRING_KEY_MAP_STR_FIELD_NAME, "toJsonMapStr(" + STRING_KEY_MAP_FIELD_NAME + ")"),
        new TransformConfig(INT_KEY_MAP_STR_FIELD_NAME, "toJsonMapStr(" + INT_KEY_MAP_FIELD_NAME + ")"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setIngestionConfig(ingestionConfig)
        .build();
  }

  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema stringKeyMapAvroSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
    org.apache.avro.Schema intKeyMapAvroSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    List<org.apache.avro.Schema.Field> fields =
        Arrays.asList(
            new org.apache.avro.Schema.Field(STRING_KEY_MAP_FIELD_NAME, stringKeyMapAvroSchema, null, null),
            new org.apache.avro.Schema.Field(INT_KEY_MAP_FIELD_NAME, intKeyMapAvroSchema, null, null));
    avroSchema.setFields(fields);

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_DOCS; i++) {
        Map<String, Integer> stringKeyMap = new HashMap<>();
        stringKeyMap.put("k1", i);
        stringKeyMap.put("k2", NUM_DOCS + i);
        Map<Integer, String> intKeyMap = new HashMap<>();
        intKeyMap.put(95, Integer.toString(i));
        intKeyMap.put(717, Integer.toString(NUM_DOCS + i));
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(STRING_KEY_MAP_FIELD_NAME, stringKeyMap);
        record.put(INT_KEY_MAP_FIELD_NAME, intKeyMap);
        fileWriter.append(record);
      }
    }

    return List.of(avroFile);
  }

  protected int getSelectionDefaultDocCount() {
    return _setSelectionDefaultDocCount;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testJsonPathQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Selection only
    String query = "SELECT stringKeyMapStr FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), String.format("{\"k1\":%d,\"k2\":%d}", i, NUM_DOCS + i));
    }
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k1', 'INT') FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
    }
    query = "SELECT jsonExtractScalar(intKeyMapStr, '$.95', 'INT') FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
    }

    // Selection order-by
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k2', 'INT') FROM " + getTableName()
        + " ORDER BY jsonExtractScalar(stringKeyMapStr, '$.k1', 'INT')";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), NUM_DOCS + i);
    }
    query = "SELECT jsonExtractScalar(intKeyMapStr, '$.717', 'INT') FROM " + getTableName()
        + " ORDER BY jsonExtractScalar(intKeyMapStr, '$.95', 'INT')";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), NUM_DOCS + i);
    }

    // Aggregation only
    query = "SELECT MAX(jsonExtractScalar(stringKeyMapStr, '$.k1', 'INT')) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode aggregationResult = pinotResponse.get("resultTable").get("rows").get(0).get(0);
    assertEquals(aggregationResult.intValue(), NUM_DOCS - 1);
    query = "SELECT MAX(jsonExtractScalar(intKeyMapStr, '$.95', 'INT')) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    aggregationResult = pinotResponse.get("resultTable").get("rows").get(0).get(0);
    assertEquals(aggregationResult.intValue(), NUM_DOCS - 1);

    // Aggregation group-by
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k1', 'INT') AS key, "
        + "MIN(jsonExtractScalar(stringKeyMapStr, '$.k2', 'INT')) AS value FROM " + getTableName()
        + " GROUP BY key ORDER BY value";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
      assertEquals(rows.get(i).get(1).intValue(), NUM_DOCS + i);
    }
    query = "SELECT jsonExtractScalar(intKeyMapStr, '$.95', 'INT') AS key, "
        + "MIN(jsonExtractScalar(intKeyMapStr, '$.717', 'INT')) AS value FROM " + getTableName()
        + " GROUP BY key ORDER BY value";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
      assertEquals(rows.get(i).get(1).intValue(), NUM_DOCS + i);
    }

    // Filter
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k2', 'INT') FROM " + getTableName()
        + " WHERE jsonExtractScalar(stringKeyMapStr, '$.k1', 'INT') = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).intValue(), NUM_DOCS + 25);
    query = "SELECT jsonExtractScalar(intKeyMapStr, '$.717', 'INT') FROM " + getTableName()
        + " WHERE jsonExtractScalar(intKeyMapStr, '$.95', 'INT') = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).intValue(), NUM_DOCS + 25);

    // Select non-existing key (illegal query)
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k3', 'INT') FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertNotEquals(pinotResponse.get("exceptions").size(), 0);
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.123', 'INT') FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertNotEquals(pinotResponse.get("exceptions").size(), 0);

    // Select non-existing key with default value
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k3', 'INT', '0') FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.123', 'INT', '0') FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Selection only
    String query = "SELECT mapValue(stringKeyMap__KEYS, 'k1', stringKeyMap__VALUES) FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
    }
    query = "SELECT mapValue(intKeyMap__KEYS, 95, intKeyMap__VALUES) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
    }

    // Selection order-by
    query = "SELECT mapValue(stringKeyMap__KEYS, 'k2', stringKeyMap__VALUES) FROM " + getTableName()
        + " ORDER BY mapValue(stringKeyMap__KEYS, 'k1', stringKeyMap__VALUES)";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), NUM_DOCS + i);
    }
    query = "SELECT mapValue(intKeyMap__KEYS, 717, intKeyMap__VALUES) FROM " + getTableName()
        + " ORDER BY mapValue(intKeyMap__KEYS, 95, intKeyMap__VALUES)";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), NUM_DOCS + i);
    }

    // Aggregation only
    query = "SELECT MAX(mapValue(stringKeyMap__KEYS, 'k1', stringKeyMap__VALUES)) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode aggregationResult = pinotResponse.get("resultTable").get("rows").get(0).get(0);
    assertEquals(aggregationResult.intValue(), NUM_DOCS - 1);
    query = "SELECT MAX(mapValue(intKeyMap__KEYS, 95, intKeyMap__VALUES)) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    aggregationResult = pinotResponse.get("resultTable").get("rows").get(0).get(0);
    assertEquals(aggregationResult.intValue(), NUM_DOCS - 1);

    // Aggregation group-by
    query = "SELECT mapValue(stringKeyMap__KEYS, 'k1', stringKeyMap__VALUES) AS key, "
        + "MIN(mapValue(stringKeyMap__KEYS, 'k2', stringKeyMap__VALUES)) AS value FROM " + getTableName()
        + " GROUP BY key ORDER BY value";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
      assertEquals(rows.get(i).get(1).intValue(), NUM_DOCS + i);
    }
    query = "SELECT mapValue(intKeyMap__KEYS, 95, intKeyMap__VALUES) AS key, "
        + "MIN(mapValue(intKeyMap__KEYS, 717, intKeyMap__VALUES)) AS value FROM " + getTableName()
        + " GROUP BY key ORDER BY value";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
      assertEquals(rows.get(i).get(1).intValue(), NUM_DOCS + i);
    }

    // Filter
    query = "SELECT mapValue(stringKeyMap__KEYS, 'k2', stringKeyMap__VALUES) FROM " + getTableName()
        + " WHERE mapValue(stringKeyMap__KEYS, 'k1', stringKeyMap__VALUES) = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).intValue(), NUM_DOCS + 25);
    query = "SELECT mapValue(intKeyMap__KEYS, 717, intKeyMap__VALUES) FROM " + getTableName()
        + " WHERE mapValue(intKeyMap__KEYS, 95, intKeyMap__VALUES) = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).intValue(), NUM_DOCS + 25);

    // Filter on non-existing key
    query = "SELECT mapValue(stringKeyMap__KEYS, 'k2', stringKeyMap__VALUES) FROM " + getTableName()
        + " WHERE mapValue(stringKeyMap__KEYS, 'k3', stringKeyMap__VALUES) = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
    query = "SELECT mapValue(intKeyMap__KEYS, 717, intKeyMap__VALUES) FROM " + getTableName()
        + " WHERE mapValue(intKeyMap__KEYS, 123, intKeyMap__VALUES) = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Select non-existing key (illegal query)
    query = "SELECT mapValue(stringKeyMap__KEYS, 'k3', stringKeyMap__VALUES) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertNotEquals(pinotResponse.get("exceptions").size(), 0);
    query = "SELECT mapValue(stringKeyMap__KEYS, 123, stringKeyMap__VALUES) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertNotEquals(pinotResponse.get("exceptions").size(), 0);
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testMultiValueQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query;
    JsonNode pinotResponse;
    JsonNode rows;

    // Filter on non-existing key
    query
        = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k2', 'INT') FROM " + getTableName()
        + " WHERE jsonExtractScalar(stringKeyMapStr, '$.k3', 'INT_ARRAY') = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
    query = "SELECT jsonExtractScalar(intKeyMapStr, '$.717', 'INT') FROM " + getTableName()
        + " WHERE jsonExtractScalar(intKeyMapStr, '$.123', 'INT_ARRAY') = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Select non-existing key with proper filter
    query = "SELECT jsonExtractScalar(intKeyMapStr, '$.123', 'INT') FROM " + getTableName()
        + " WHERE jsonExtractKey(intKeyMapStr, '$.*') = '$[''123'']'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
    query = "SELECT jsonExtractScalar(stringKeyMapStr, '$.k3', 'INT') FROM " + getTableName()
        + " WHERE jsonExtractKey(stringKeyMapStr, '$.*') = '$[''k3'']'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Select non-existing key with proper filter
    query = "SELECT mapValue(stringKeyMap__KEYS, 'k3', stringKeyMap__VALUES) FROM " + getTableName()
        + " WHERE stringKeyMap__KEYS = 'k3'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
    query = "SELECT mapValue(intKeyMap__KEYS, 123, intKeyMap__VALUES) FROM " + getTableName()
        + " WHERE stringKeyMap__KEYS = 123";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
  }

  @Override
  protected void setUseMultiStageQueryEngine(boolean useMultiStageQueryEngine) {
    super.setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    _setSelectionDefaultDocCount = useMultiStageQueryEngine ? 1000 : 10;
  }
}
