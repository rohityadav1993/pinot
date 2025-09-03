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
package org.apache.pinot.perf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReaderFactory;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;


import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * Comprehensive benchmark to compare row-wise vs column-wise segment refresh performance.
 *
 * This benchmark tests various scenarios:
 * - No schema/config change
 * - New column addition
 * - Data type change
 * - Index updates
 *
 * Test parameters: 50 columns (25 dimensions, 25 metrics), 1 million rows
 */
public class SegmentRefreshBenchmark {

  // Benchmark parameters
  private static final int NUM_ROWS = 1_000_000;
  private static final int NUM_COLUMNS = 50;
  private static final int NUM_DIMENSIONS = 25;
  private static final int NUM_METRICS = 25;

  // Test infrastructure
  private File _tempDir;
  private File _segmentDir;
  private File _workingDir;
  private String _tableName = "benchmarkTable";

  // Test schemas and configs
  private Schema _originalSchema;
  private TableConfig _originalTableConfig;

  public SegmentRefreshBenchmark() {
  }

  /**
   * Set up the benchmark environment.
   */
  public void setup() throws Exception {
    System.out.println("Setting up benchmark for " + NUM_COLUMNS + " columns, " + NUM_ROWS + " rows...");

    // Create temporary directories
    _tempDir = new File(System.getProperty("java.io.tmpdir"), "pinot-benchmark-" + System.currentTimeMillis());
    _segmentDir = new File(_tempDir, "segments");
    _workingDir = new File(_tempDir, "working");

    _tempDir.mkdirs();
    _segmentDir.mkdirs();
    _workingDir.mkdirs();

    // Create test schema and config
    createOriginalSchema();
    createOriginalTableConfig();

    // Generate test segment
    generateTestSegment();

    System.out.println("Setup complete. Segment created at: " + _segmentDir.getAbsolutePath());
  }

  /**
   * Clean up benchmark resources.
   */
  public void tearDown() throws Exception {
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  /**
   * Run comprehensive benchmark comparing row-major vs column-major refresh.
   */
  public BenchmarkResults runBenchmark() throws Exception {
    BenchmarkResults results = new BenchmarkResults();

    System.out.println("=== Starting Comprehensive Segment Refresh Benchmark ===");

    // Scenario 1: No change
    System.out.println("\n--- Scenario 1: No Schema/Config Change ---");
    BenchmarkResult noChangeResult = benchmarkScenario("no_change", _originalSchema, _originalTableConfig);
    results.addResult("no_change", noChangeResult);

    // Scenario 2: New column addition
    System.out.println("\n--- Scenario 2: New Column Addition ---");
    Schema schemaWithNewColumn = createSchemaWithNewColumn();
    BenchmarkResult newColumnResult = benchmarkScenario("new_column", schemaWithNewColumn,
        _originalTableConfig);
    results.addResult("new_column", newColumnResult);

    // Scenario 3: Data type change
    System.out.println("\n--- Scenario 3: Data Type Change ---");
    Schema schemaWithDataTypeChange = createSchemaWithDataTypeChange();
    BenchmarkResult dataTypeChangeResult = benchmarkScenario("data_type_change",
        schemaWithDataTypeChange, _originalTableConfig);
    results.addResult("data_type_change", dataTypeChangeResult);

    // Scenario 4: Index updates
    System.out.println("\n--- Scenario 4: Index Updates ---");
    TableConfig configWithNewIndexes = createTableConfigWithNewIndexes();
    BenchmarkResult indexUpdateResult = benchmarkScenario("index_update", _originalSchema, configWithNewIndexes);
    results.addResult("index_update", indexUpdateResult);

    System.out.println("\n=== Benchmark Complete ===");
    return results;
  }

  /**
   * Benchmark a specific scenario comparing row-major vs column-major refresh.
   */
  private BenchmarkResult benchmarkScenario(String scenarioName, Schema targetSchema, TableConfig targetConfig)
      throws Exception {
    // Row-major refresh
    long rowMajorTime = benchmarkRowMajorRefresh(scenarioName, targetSchema, targetConfig);

    // Column-major refresh
    long columnMajorTime = benchmarkColumnMajorRefresh(scenarioName, targetSchema, targetConfig);

    return new BenchmarkResult(scenarioName, rowMajorTime, columnMajorTime);
  }

  /**
   * Benchmark row-major segment refresh.
   */
  private long benchmarkRowMajorRefresh(String scenarioName, Schema targetSchema, TableConfig targetConfig)
      throws Exception {
    System.out.println("  Running row-major refresh...");

    File workingDirRowMajor = new File(_workingDir, scenarioName + "_row_major");
    workingDirRowMajor.mkdirs();

    File[] segmentDirs = _segmentDir.listFiles();
    if (segmentDirs == null || segmentDirs.length == 0) {
      throw new RuntimeException("No segments found for benchmarking");
    }

    File indexDir = segmentDirs[0];

    long startTime = System.currentTimeMillis();

    // Use traditional row-major segment building
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(targetConfig, targetSchema);
    config.setInputFilePath(indexDir.getAbsolutePath());
    config.setOutDir(workingDirRowMajor.getAbsolutePath());
    config.setSegmentName("benchmark_segment_row_major");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

    // Load original segment and use record reader (row-major)
    ImmutableSegment originalSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      recordReader.init(originalSegment);

      driver.init(config, recordReader);
      driver.build();

      recordReader.close();
    } finally {
      originalSegment.destroy();
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("    Row-major refresh completed in: " + duration + " ms");
    return duration;
  }

  /**
   * Benchmark column-major segment refresh.
   */
  private long benchmarkColumnMajorRefresh(String scenarioName, Schema targetSchema, TableConfig targetConfig)
      throws Exception {
    System.out.println("  Running column-major refresh...");

    File workingDirColumnMajor = new File(_workingDir, scenarioName + "_column_major");
    workingDirColumnMajor.mkdirs();

    File[] segmentDirs = _segmentDir.listFiles();
    if (segmentDirs == null || segmentDirs.length == 0) {
      throw new RuntimeException("No segments found for benchmarking");
    }

    File indexDir = segmentDirs[0];

    long startTime = System.currentTimeMillis();

    // Use column-major segment building
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(targetConfig, targetSchema);
    config.setInputFilePath(indexDir.getAbsolutePath());
    config.setOutDir(workingDirColumnMajor.getAbsolutePath());
    config.setSegmentName("benchmark_segment_column_major");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

    // Load original segment and use column reader factory (column-major)
    ImmutableSegment originalSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      ColumnReaderFactory columnReaderFactory = new PinotSegmentColumnReaderFactory(originalSegment);
      columnReaderFactory.init(targetSchema);

      driver.init(config, columnReaderFactory);
      driver.build();
    } finally {
      originalSegment.destroy();
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("    Column-major refresh completed in: " + duration + " ms");
    return duration;
  }

  /**
   * Create the original schema with 50 columns (25 dimensions, 25 metrics).
   */
  private void createOriginalSchema() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    builder.setSchemaName(_tableName);

    // Add 25 dimension columns with varied data types
    for (int i = 0; i < NUM_DIMENSIONS; i++) {
      if (i % 3 == 0) {
        builder.addSingleValueDimension("dim_string_" + i, FieldSpec.DataType.STRING);
      } else if (i % 3 == 1) {
        builder.addSingleValueDimension("dim_int_" + i, FieldSpec.DataType.INT);
      } else {
        builder.addSingleValueDimension("dim_long_" + i, FieldSpec.DataType.LONG);
      }
    }

    // Add 25 metric columns with varied data types
    for (int i = 0; i < NUM_METRICS; i++) {
      if (i % 3 == 0) {
        builder.addMetric("metric_double_" + i, FieldSpec.DataType.DOUBLE);
      } else if (i % 3 == 1) {
        builder.addMetric("metric_float_" + i, FieldSpec.DataType.FLOAT);
      } else {
        builder.addMetric("metric_long_" + i, FieldSpec.DataType.LONG);
      }
    }

    _originalSchema = builder.build();
  }

  /**
   * Create the original table configuration.
   */
  private void createOriginalTableConfig() {
    _originalTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(_tableName)
        .build();
  }

  /**
   * Create schema with a new column added.
   */
  private Schema createSchemaWithNewColumn() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    builder.setSchemaName(_tableName);

    // Copy all original fields
    for (FieldSpec fieldSpec : _originalSchema.getAllFieldSpecs()) {
      builder.addField(fieldSpec);
    }

    // Add new column
    builder.addSingleValueDimension("new_column", FieldSpec.DataType.STRING);

    return builder.build();
  }

  /**
   * Create schema with data type changes.
   */
  private Schema createSchemaWithDataTypeChange() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    builder.setSchemaName(_tableName);

    // Copy all original fields with some data type changes
    for (FieldSpec fieldSpec : _originalSchema.getAllFieldSpecs()) {
      if (fieldSpec.getName().equals("dim_int_1")) {
        // Change INT to LONG
        builder.addSingleValueDimension("dim_int_1", FieldSpec.DataType.LONG);
      } else if (fieldSpec.getName().equals("metric_float_1")) {
        // Change FLOAT to DOUBLE
        builder.addMetric("metric_float_1", FieldSpec.DataType.DOUBLE);
      } else {
        builder.addField(fieldSpec);
      }
    }

    return builder.build();
  }

  /**
   * Create table config with new indexes.
   */
  private TableConfig createTableConfigWithNewIndexes() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(_tableName)
        .setInvertedIndexColumns(Arrays.asList("dim_string_0", "dim_int_1", "dim_long_2"))
        .build();
  }

  /**
   * Generate test segment with synthetic data.
   */
  private void generateTestSegment() throws Exception {
    System.out.println("Generating test segment with " + NUM_ROWS + " rows...");

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_originalTableConfig, _originalSchema);
    config.setOutDir(_segmentDir.getAbsolutePath());
    config.setSegmentName("benchmark_segment_original");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

    SyntheticDataGenerator dataGenerator = new SyntheticDataGenerator(_originalSchema, NUM_ROWS);
    dataGenerator.init();

    driver.init(config, dataGenerator);
    driver.build();

    System.out.println("Test segment generation complete.");
  }

  /**
   * Main method to run the benchmark.
   */
  public static void main(String[] args) throws Exception {
    SegmentRefreshBenchmark benchmark = new SegmentRefreshBenchmark();

    try {
      benchmark.setup();
      BenchmarkResults results = benchmark.runBenchmark();

      // Print results
      results.printResults();

      // Save results to file
      results.saveResults(new File("benchmark_results_" + System.currentTimeMillis() + ".csv"));
    } finally {
      benchmark.tearDown();
    }
  }

  /**
   * Container for individual benchmark result.
   */
  public static class BenchmarkResult {
    private final String _scenario;
    private final long _rowMajorTime;
    private final long _columnMajorTime;

    public BenchmarkResult(String scenario, long rowMajorTime, long columnMajorTime) {
      _scenario = scenario;
      _rowMajorTime = rowMajorTime;
      _columnMajorTime = columnMajorTime;
    }

    public String getScenario() {
      return _scenario;
    }

    public long getRowMajorTime() {
      return _rowMajorTime;
    }

    public long getColumnMajorTime() {
      return _columnMajorTime;
    }

    public long getAbsoluteDifference() {
      return _rowMajorTime - _columnMajorTime;
    }

    public double getPercentageImprovement() {
      if (_rowMajorTime == 0) {
        return 0.0;
      }
      return ((double) (_rowMajorTime - _columnMajorTime) / _rowMajorTime) * 100.0;
    }
  }

  /**
   * Container for all benchmark results.
   */
  public static class BenchmarkResults {
    private final Map<String, BenchmarkResult> _results = new HashMap<>();

    public void addResult(String scenario, BenchmarkResult result) {
      _results.put(scenario, result);
    }

    public void printResults() {
      System.out.println("\n=== BENCHMARK RESULTS ===");
      System.out.printf("%-20s | %-12s | %-12s | %-12s | %-12s%n",
          "Scenario", "Row-Major(ms)", "Column-Major(ms)", "Difference(ms)", "Improvement(%)");
      System.out.println("------------------------------------------------------------------------------");

      for (BenchmarkResult result : _results.values()) {
        System.out.printf("%-20s | %-12d | %-12d | %-12d | %-12.2f%%%n",
            result.getScenario(),
            result.getRowMajorTime(),
            result.getColumnMajorTime(),
            result.getAbsoluteDifference(),
            result.getPercentageImprovement());
      }
    }

    public void saveResults(File outputFile) throws IOException {
      try (FileWriter writer = new FileWriter(outputFile)) {
        writer.write("Scenario,Row-Major(ms),Column-Major(ms),Difference(ms),Improvement(%)\n");

        for (BenchmarkResult result : _results.values()) {
          writer.write(String.format("%s,%d,%d,%d,%.2f\n",
              result.getScenario(),
              result.getRowMajorTime(),
              result.getColumnMajorTime(),
              result.getAbsoluteDifference(),
              result.getPercentageImprovement()));
        }
      }

      System.out.println("Results saved to: " + outputFile.getAbsolutePath());
    }
  }
}
