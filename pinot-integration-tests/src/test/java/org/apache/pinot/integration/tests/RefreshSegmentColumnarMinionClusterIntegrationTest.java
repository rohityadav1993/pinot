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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Comprehensive integration test for RefreshSegmentTaskExecutor columnar vs traditional refresh comparison.
 * This test validates data correctness and functionality across various scenarios by comparing the output
 * of columnar refresh against traditional row-major refresh to ensure identical results.
 *
 * Test scenarios include:
 * - Basic refresh with no changes (data integrity validation)
 * - New column addition with default value validation
 * - Compatible data type changes with conversion validation
 * - Index addition with functionality validation
 */
public class RefreshSegmentColumnarMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotTaskManager _taskManager;
  protected PinotHelixResourceManager _pinotHelixResourceManager;
  protected final File _segmentDataDir = new File(_tempDir, "segmentDataDir");
  protected final File _segmentTarDir = new File(_tempDir, "segmentTarDir");

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDataDir, _segmentTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();

    // Create schema and tableConfig
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    // Start without any task config - we'll add it per test
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);
    // Create segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDataDir,
        _segmentTarDir);
    uploadSegments(getTableName(), _segmentTarDir);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test(priority = 1)
  public void testBasicRefreshDataIntegrity() throws Exception {
    System.out.println("=== Testing Basic Refresh: Columnar vs Traditional Data Integrity ===");

    // Store original segment data for comparison
    Map<String, GenericRow[]> originalSegmentData = readAllSegmentData();

    // Execute traditional refresh
    updateTableTaskConfig(getTraditionalRefreshTaskConfig());
    executeRefreshTasks();
    Map<String, GenericRow[]> traditionalRefreshData = readAllSegmentData();

    // Reset segments by re-uploading original data
    resetSegments();
    Map<String, GenericRow[]> resetData = readAllSegmentData();
    validateSegmentDataIntegrity("Original vs Reset", originalSegmentData, resetData);

    // Execute columnar refresh
    updateTableTaskConfig(getColumnarRefreshTaskConfig());
    executeRefreshTasks();
    Map<String, GenericRow[]> columnarRefreshData = readAllSegmentData();

    // Validate that both approaches produce identical results
    validateSegmentDataIntegrity("Traditional vs Columnar Refresh", traditionalRefreshData,
        columnarRefreshData);

    // Validate query functionality works the same
    validateBasicQueryWorks();

    System.out.println("✓ Basic refresh produces identical results for both approaches");
  }

  @Test(priority = 2)
  public void testNewColumnAddition() throws Exception {
    System.out.println("=== Testing New Column Addition: Columnar vs Traditional ===");

    // Reset to clean state
    resetSegments();

    // Add new column to schema
    String newColumnName = "NewTestColumn";
    String defaultValue = "DEFAULT_VALUE";
    Schema schema = createSchema();
    schema.addField(new DimensionFieldSpec(newColumnName, FieldSpec.DataType.STRING, true, defaultValue));
    forceUpdateSchema(schema);

    // Execute traditional refresh with new column
    updateTableTaskConfig(getTraditionalRefreshTaskConfig());
    executeRefreshTasks();
    Map<String, GenericRow[]> traditionalRefreshData = readAllSegmentData();

    // Reset segments and execute columnar refresh (preserve schema changes)
    updateTableTaskConfig(getColumnarRefreshTaskConfig());
    resetSegments();
    forceUpdateSchema(schema);  // Re-apply the schema with new column
    executeRefreshTasks();
    Map<String, GenericRow[]> columnarRefreshData = readAllSegmentData();

    // Validate that both approaches produce identical results
    validateSegmentDataIntegrity("Traditional vs Columnar with New Column", traditionalRefreshData,
        columnarRefreshData);

    // Validate new column has correct default values
    validateNewColumnDefaultValues(columnarRefreshData, newColumnName, defaultValue);

    // Validate query functionality
    validateBasicQueryWorks();

    System.out.println("✓ New column addition produces identical results for both approaches");
  }

  @Test(priority = 3)
  public void testCompatibleDataTypeChanges() throws Exception {
    System.out.println("=== Testing Compatible Data Type Changes: Columnar vs Traditional ===");

    // Reset to clean state
    resetSegments();

    // Store original data for type conversion validation
    Map<String, GenericRow[]> originalData = readAllSegmentData();

    // Change data types for compatible conversions
    Schema schema = createSchema();
    // INT -> LONG conversion
    schema.getFieldSpecFor("ArrTime").setDataType(FieldSpec.DataType.LONG);
    // INT -> FLOAT conversion (for ActualElapsedTime)
    schema.getFieldSpecFor("ActualElapsedTime").setDataType(FieldSpec.DataType.FLOAT);
    forceUpdateSchema(schema);

    // Execute traditional refresh with data type changes
    updateTableTaskConfig(getTraditionalRefreshTaskConfig());
    executeRefreshTasks();
    Map<String, GenericRow[]> traditionalRefreshData = readAllSegmentData();

    // Reset segments and execute columnar refresh (preserve schema changes)
    updateTableTaskConfig(getColumnarRefreshTaskConfig());
    resetSegments();
    forceUpdateSchema(schema);  // Re-apply the schema with data type changes
    executeRefreshTasks();
    Map<String, GenericRow[]> columnarRefreshData = readAllSegmentData();

    // Validate that both approaches produce identical results
    validateSegmentDataIntegrity("Traditional vs Columnar with Data Type Changes", traditionalRefreshData,
        columnarRefreshData);

    // Validate data type conversions are correct
    validateDataTypeConversions(originalData, columnarRefreshData, "ArrTime",
        FieldSpec.DataType.INT, FieldSpec.DataType.LONG);
    validateDataTypeConversions(originalData, columnarRefreshData, "ActualElapsedTime",
        FieldSpec.DataType.INT, FieldSpec.DataType.FLOAT);

    // Validate query functionality with new data types
    validateBasicQueryWorks();

    System.out.println("✓ Data type changes produce identical results for both approaches");
  }

  @Test(priority = 4)
  public void testIndexAddition() throws Exception {
    System.out.println("=== Testing Index Addition: Columnar vs Traditional ===");

    // Reset to clean state
    resetSegments();

    // Store original data for comparison
    Map<String, GenericRow[]> originalData = readAllSegmentData();

    // Add inverted index to columns
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(offlineTableName);
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

    List<String> invertedIndexColumns = Arrays.asList("DivActualElapsedTime", "Origin", "Quarter");
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
    tableConfig.setIndexingConfig(indexingConfig);
    _pinotHelixResourceManager.updateTableConfig(tableConfig);

    // Execute traditional refresh with index addition
    updateTableTaskConfig(getTraditionalRefreshTaskConfig());
    executeRefreshTasks();
    Map<String, GenericRow[]> traditionalRefreshData = readAllSegmentData();

    // Reset segments and execute columnar refresh
    updateTableTaskConfig(getColumnarRefreshTaskConfig());
    resetSegments();
    executeRefreshTasks();
    Map<String, GenericRow[]> columnarRefreshData = readAllSegmentData();

    // Validate that both approaches produce identical results
    validateSegmentDataIntegrity("Traditional vs Columnar with Index Addition", traditionalRefreshData,
        columnarRefreshData);

    // Validate data integrity is maintained
    validateSegmentDataIntegrity("Original vs Columnar with Indexes", originalData, columnarRefreshData);

    // Validate query functionality and index effectiveness
    validateBasicQueryWorks();
    validateInvertedIndexFunctionality("DivActualElapsedTime");

    System.out.println("✓ Index addition produces identical results for both approaches");
  }

  // Helper methods

  private TableTaskConfig getTraditionalRefreshTaskConfig() {
    Map<String, String> taskConfigs = new HashMap<>();
    // Traditional row-major refresh (default behavior)
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, taskConfigs));
  }

  private TableTaskConfig getColumnarRefreshTaskConfig() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.RefreshSegmentTask.COLUMNAR_RELOAD_AND_SKIP_TRANSFORMATION, "true");
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, taskConfigs));
  }

  private void updateTableTaskConfig(TableTaskConfig taskConfig) throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(offlineTableName);
    tableConfig.setTaskConfig(taskConfig);
    _pinotHelixResourceManager.updateTableConfig(tableConfig);
  }

  private void executeRefreshTasks() throws Exception {
    // Schedule refresh task
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    assertNotNull(_taskManager.scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Collections.singleton(offlineTableName)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE));

    // Wait for task completion
    waitForTaskToFinish();
  }

  private void waitForTaskToFinish() throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        Set<String> runningTasks = _helixTaskResourceManager.getTasksInProgress(
            MinionConstants.RefreshSegmentTask.TASK_TYPE);
        return runningTasks.isEmpty();
      } catch (Exception e) {
        return false;
      }
    }, 15_000L, "Failed to finish RefreshSegmentTask");
  }

  private void resetSegments() throws Exception {
    // First drop all existing segments
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    List<String> segmentNames = _pinotHelixResourceManager.getSegmentsFor(offlineTableName, false);
    for (String segmentName : segmentNames) {
      _pinotHelixResourceManager.deleteSegment(offlineTableName, segmentName);
    }

    // Wait for segments to be deleted
    TestUtils.waitForCondition(aVoid -> {
      List<String> remainingSegments = _pinotHelixResourceManager.getSegmentsFor(offlineTableName, false);
      return remainingSegments.isEmpty();
    }, 30_000L, "Failed to delete existing segments");

    // Reset schema to original state
    forceUpdateSchema(createSchema());

    // Re-upload original segments
    uploadSegments(getTableName(), _segmentTarDir);

    // Wait for segments to be loaded
    TestUtils.waitForCondition(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM " + getTableName();
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("rows").get(0).get(0).asLong() > 0;
      } catch (Exception e) {
        return false;
      }
    }, 30_000L, "Failed to load segments after reset");
  }

  /**
   * Read all segment data using PinotSegmentRecordReader for comprehensive validation
   */
  private Map<String, GenericRow[]> readAllSegmentData() throws Exception {
    Map<String, GenericRow[]> segmentData = new HashMap<>();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Get all segment ZK metadata
    for (SegmentZKMetadata zkMetadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      String segmentName = zkMetadata.getSegmentName();

      // Find segment directory
      File segmentDir = findSegmentDirectory(segmentName);
      if (segmentDir == null) {
        continue;
      }

      // Load segment and read data
      ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
      try {
        PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
        recordReader.init(segment);

        // Read all records
        List<GenericRow> records = new ArrayList<>();
        while (recordReader.hasNext()) {
          records.add(recordReader.next());
        }

        segmentData.put(segmentName, records.toArray(new GenericRow[0]));
        recordReader.close();
      } finally {
        segment.destroy();
      }
    }

    return segmentData;
  }

  /**
   * Find segment directory in the server data directory
   */
  private File findSegmentDirectory(String segmentName) {
    // Check multiple possible locations for segment storage
    String tempDir = System.getProperty("java.io.tmpdir");
    File[] tempDirContents = new File(tempDir).listFiles();

    if (tempDirContents != null) {
      for (File timestampDir : tempDirContents) {
        if (timestampDir.isDirectory() && timestampDir.getName().matches("\\d+")) {
          File serverDir = new File(timestampDir, "PinotServer");
          if (serverDir.exists()) {
            File dataDir = new File(serverDir, "dataDir-0");
            if (dataDir.exists()) {
              // Check for segment directly in dataDir
              File segmentDir = new File(dataDir, segmentName);
              if (segmentDir.exists()) {
                return segmentDir;
              }

              // Check in table subdirectory
              String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
              File tableDir = new File(dataDir, offlineTableName);
              if (tableDir.exists()) {
                File segmentDirInTable = new File(tableDir, segmentName);
                if (segmentDirInTable.exists()) {
                  return segmentDirInTable;
                }
              }

              // Check all subdirectories in dataDir
              File[] subdirs = dataDir.listFiles();
              if (subdirs != null) {
                for (File subdir : subdirs) {
                  if (subdir.isDirectory()) {
                    File segmentDirInSub = new File(subdir, segmentName);
                    if (segmentDirInSub.exists()) {
                      return segmentDirInSub;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    return null;
  }

  /**
   * Validate data integrity between two segment data sets
   */
  private void validateSegmentDataIntegrity(String comparisonName, Map<String, GenericRow[]> expectedData,
                                           Map<String, GenericRow[]> actualData) {
    assertEquals(actualData.size(), expectedData.size(),
        comparisonName + ": Number of segments should be identical");

    for (String segmentName : expectedData.keySet()) {
      assertTrue(actualData.containsKey(segmentName),
          comparisonName + ": Should contain segment: " + segmentName);

      GenericRow[] expectedRows = expectedData.get(segmentName);
      GenericRow[] actualRows = actualData.get(segmentName);

      assertEquals(actualRows.length, expectedRows.length,
          comparisonName + ": Number of rows should be identical for segment: " + segmentName);

      // Compare each row for existing columns
      for (int i = 0; i < expectedRows.length; i++) {
        GenericRow expectedRow = expectedRows[i];
        GenericRow actualRow = actualRows[i];

        // Check all common columns
        for (String fieldName : expectedRow.getFieldToValueMap().keySet()) {
          if (actualRow.getFieldToValueMap().containsKey(fieldName)) {
            Object expectedValue = expectedRow.getValue(fieldName);
            Object actualValue = actualRow.getValue(fieldName);

            // Handle numeric type conversions gracefully
            if (!valuesAreEqual(expectedValue, actualValue)) {
              assertEquals(actualValue, expectedValue,
                  String.format("%s: Value mismatch for column '%s' in segment '%s' row %d",
                      comparisonName, fieldName, segmentName, i));
            }
          }
        }
      }
    }
  }

  /**
   * Validate new column has default values
   */
  private void validateNewColumnDefaultValues(Map<String, GenericRow[]> segmentData,
                                             String newColumnName, String expectedDefaultValue) {
    for (String segmentName : segmentData.keySet()) {
      GenericRow[] rows = segmentData.get(segmentName);

      for (int i = 0; i < rows.length; i++) {
        GenericRow row = rows[i];
        assertTrue(row.getFieldToValueMap().containsKey(newColumnName),
            String.format("New column '%s' should exist in segment '%s' row %d",
                newColumnName, segmentName, i));

        Object value = row.getValue(newColumnName);
        assertEquals(value, expectedDefaultValue,
            String.format("New column '%s' should have default value in segment '%s' row %d",
                newColumnName, segmentName, i));
      }
    }
  }

  /**
   * Validate data type conversions
   */
  private void validateDataTypeConversions(Map<String, GenericRow[]> originalData,
                                          Map<String, GenericRow[]> convertedData,
                                          String columnName,
                                          FieldSpec.DataType originalType,
                                          FieldSpec.DataType newType) {
    for (String segmentName : originalData.keySet()) {
      GenericRow[] originalRows = originalData.get(segmentName);
      GenericRow[] convertedRows = convertedData.get(segmentName);

      for (int i = 0; i < originalRows.length; i++) {
        Object originalValue = originalRows[i].getValue(columnName);
        Object convertedValue = convertedRows[i].getValue(columnName);

        // Validate type conversion correctness
        if (originalValue != null && convertedValue != null) {
          // Check that the converted value is the correct type
          validateValueType(convertedValue, newType, segmentName, columnName, i);

          // Check that the value is correctly converted
          validateValueConversion(originalValue, convertedValue, originalType, newType,
              segmentName, columnName, i);
        }
      }
    }
  }

  /**
   * Validate that a value is of the expected type
   */
  private void validateValueType(Object value, FieldSpec.DataType expectedType,
                                String segmentName, String columnName, int rowIndex) {
    boolean isCorrectType = false;
    switch (expectedType) {
      case INT:
        isCorrectType = value instanceof Integer;
        break;
      case LONG:
        isCorrectType = value instanceof Long;
        break;
      case FLOAT:
        isCorrectType = value instanceof Float;
        break;
      case DOUBLE:
        isCorrectType = value instanceof Double;
        break;
      case STRING:
        isCorrectType = value instanceof String;
        break;
      default:
        // For other data types, assume correct for now
        isCorrectType = true;
        break;
    }

    assertTrue(isCorrectType,
        String.format("Value type mismatch for column '%s' in segment '%s' row %d. Expected %s, got %s",
            columnName, segmentName, rowIndex, expectedType, value.getClass().getSimpleName()));
  }

  /**
   * Validate that value conversion is correct
   */
  private void validateValueConversion(Object originalValue, Object convertedValue,
                                      FieldSpec.DataType originalType, FieldSpec.DataType newType,
                                      String segmentName, String columnName, int rowIndex) {
    // For numeric conversions, check that the value is preserved
    if (originalType == FieldSpec.DataType.INT && newType == FieldSpec.DataType.LONG) {
      assertEquals(((Long) convertedValue).intValue(), ((Integer) originalValue).intValue(),
          String.format("INT->LONG conversion failed for column '%s' in segment '%s' row %d",
              columnName, segmentName, rowIndex));
    } else if (originalType == FieldSpec.DataType.INT && newType == FieldSpec.DataType.FLOAT) {
      assertEquals(((Float) convertedValue).intValue(), ((Integer) originalValue).intValue(),
          String.format("INT->FLOAT conversion failed for column '%s' in segment '%s' row %d",
              columnName, segmentName, rowIndex));
    }
  }

  /**
   * Check if two values are equal, handling numeric type conversions
   */
  private boolean valuesAreEqual(Object expected, Object actual) {
    if (expected == null && actual == null) {
      return true;
    }
    if (expected == null || actual == null) {
      return false;
    }

    // Direct equality check first
    if (expected.equals(actual)) {
      return true;
    }

    // Handle numeric conversions
    if (expected instanceof Number && actual instanceof Number) {
      Number expectedNum = (Number) expected;
      Number actualNum = (Number) actual;

      // For integer types, compare as long
      if (isIntegerType(expected) && isIntegerType(actual)) {
        return expectedNum.longValue() == actualNum.longValue();
      }

      // For floating point comparisons, use double precision
      if (isNumericType(expected) && isNumericType(actual)) {
        return Math.abs(expectedNum.doubleValue() - actualNum.doubleValue()) < 0.0001;
      }
    }

    return false;
  }

  private boolean isIntegerType(Object value) {
    return value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte;
  }

  private boolean isNumericType(Object value) {
    return value instanceof Number;
  }

  /**
   * Validate inverted index functionality by checking query performance
   */
  private void validateInvertedIndexFunctionality(String columnName) {
    waitForServerSegmentDownload(aVoid -> {
      try {
        // Query with filter on the indexed column - should have 0 entries scanned in filter
        String query = String.format("SELECT COUNT(*) FROM %s WHERE %s = 305", getTableName(), columnName);
        JsonNode response = postQuery(query);
        long entriesScanned = response.get("numEntriesScannedInFilter").asLong();
        return entriesScanned == 0; // Inverted index should result in 0 entries scanned
      } catch (Exception e) {
        return false;
      }
    });
  }

  public void forceUpdateSchema(Schema schema) throws IOException {
    // Use the parent class method which uses the controller client
    super.forceUpdateSchema(schema);
  }

  protected void waitForServerSegmentDownload(Function<Void, Boolean> conditionFunc) {
    TestUtils.waitForCondition(aVoid -> {
      boolean val = conditionFunc.apply(aVoid);
      return val;
    }, 15_000L, "Failed to validate server segment download");
  }

  private void validateBasicQueryWorks() {
    waitForServerSegmentDownload(aVoid -> {
      try {
        String query = "SELECT COUNT(*) FROM " + getTableName();
        JsonNode response = postQuery(query);
        return response.get("resultTable").get("rows").get(0).get(0).asLong() > 0;
      } catch (Exception e) {
        return false;
      }
    });
  }
}
