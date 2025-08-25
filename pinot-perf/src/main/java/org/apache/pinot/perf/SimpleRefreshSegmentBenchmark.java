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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.plugin.minion.tasks.refreshsegment.RefreshSegmentTaskExecutor;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * Simple benchmark to compare row-wise vs column-wise segment refresh performance.
 * Measures total execution time without JMH overhead.
 */
public class SimpleRefreshSegmentBenchmark {

  // Benchmark parameters
  private int _numRows;
  private int _numColumns;
  private String _schemaEvolutionType;

  // Test infrastructure
  private File _tempDir;
  private File _segmentDir;
  private File _workingDir;

  // Test schemas and configs
  private Schema _originalSchema;
  private Schema _updatedSchema;
  private TableConfig _originalTableConfig;
  private TableConfig _updatedTableConfig;

  public SimpleRefreshSegmentBenchmark(int numRows, int numColumns, String schemaEvolutionType) {
    _numRows = numRows;
    _numColumns = numColumns;
    _schemaEvolutionType = schemaEvolutionType;
  }

  /**
   * Set up the benchmark environment.
   */
  public void setup()
      throws Exception {
    System.out.println("Setting up benchmark for " + _numColumns + " columns, " + _numRows + " rows...");

    // Create temporary directories
    _tempDir = new File(System.getProperty("java.io.tmpdir"), "pinot-benchmark-" + System.currentTimeMillis());
    _segmentDir = new File(_tempDir, "segments");
    _workingDir = new File(_tempDir, "working");

    _tempDir.mkdirs();
    _segmentDir.mkdirs();
    _workingDir.mkdirs();

    // Create test schemas and configs
    createTestSchemas();
    createTestTableConfigs();

    // Generate test segment
    generateTestSegment();

    System.out.println("Setup complete. Segment created at: " + _segmentDir.getAbsolutePath());
  }

  /**
   * Clean up benchmark resources.
   */
  public void tearDown()
      throws Exception {
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  /**
   * Benchmark row-wise segment refresh.
   */
  public long benchmarkRowWiseRefresh()
      throws Exception {
    System.out.println("Running row-wise refresh benchmark...");

    File workingDirRowWise = new File(_workingDir, "row-wise");
    workingDirRowWise.mkdirs();

    long startTime = System.currentTimeMillis();

    RefreshSegmentTaskExecutorWrapper executor = new RefreshSegmentTaskExecutorWrapper();
    File[] segmentDirs = _segmentDir.listFiles();
    if (segmentDirs != null && segmentDirs.length > 0) {
      File indexDir = segmentDirs[0];
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      Map<String, String> taskConfigs = new HashMap<>();
      // Ensure columnar reload is disabled for row-wise
      taskConfigs.put(MinionConstants.RefreshSegmentTask.COLUMNAR_RELOAD_AND_SKIP_TRANSFORMATION, "false");

      executor.executeRowBasedRefresh(indexDir, workingDirRowWise, _updatedTableConfig,
          segmentMetadata, "testSegment", _updatedSchema, taskConfigs);
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Row-wise refresh completed in: " + duration + " ms");
    return duration;
  }

  /**
   * Benchmark column-wise segment refresh.
   */
  public long benchmarkColumnWiseRefresh()
      throws Exception {
    System.out.println("Running column-wise refresh benchmark...");

    File workingDirColumnWise = new File(_workingDir, "column-wise");
    workingDirColumnWise.mkdirs();

    long startTime = System.currentTimeMillis();

    RefreshSegmentTaskExecutorWrapper executor = new RefreshSegmentTaskExecutorWrapper();
    File[] segmentDirs = _segmentDir.listFiles();
    if (segmentDirs != null && segmentDirs.length > 0) {
      File indexDir = segmentDirs[0];
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      Map<String, String> taskConfigs = new HashMap<>();
      // Enable columnar reload
      taskConfigs.put(MinionConstants.RefreshSegmentTask.COLUMNAR_RELOAD_AND_SKIP_TRANSFORMATION, "true");

      executor.executeColumnBasedRefresh(indexDir, workingDirColumnWise, _updatedTableConfig,
          segmentMetadata, "testSegment", _updatedSchema, taskConfigs);
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Column-wise refresh completed in: " + duration + " ms");
    return duration;
  }

  /**
   * Create test schemas for original and updated scenarios.
   */
  private void createTestSchemas() {
    // Create original schema
    Schema.SchemaBuilder originalBuilder = new Schema.SchemaBuilder();
    originalBuilder.setSchemaName("testSchema");

    // Add base columns with 60% dimensions, 40% metrics (balanced for wide tables)
    int numDimensions = (int) (_numColumns * 0.6); // 60% dimensions
    int numMetrics = _numColumns - numDimensions; // 40% metrics

    // Add dimension columns (primitive types: STRING, INT, LONG)
    for (int i = 0; i < numDimensions; i++) {
      if (i % 3 == 0) {
        originalBuilder.addSingleValueDimension("dim_string_" + i, FieldSpec.DataType.STRING);
      } else if (i % 3 == 1) {
        originalBuilder.addSingleValueDimension("dim_int_" + i, FieldSpec.DataType.INT);
      } else {
        originalBuilder.addSingleValueDimension("dim_long_" + i, FieldSpec.DataType.LONG);
      }
    }

    // Add metric columns (primitive types: DOUBLE, FLOAT, LONG)
    for (int i = 0; i < numMetrics; i++) {
      if (i % 3 == 0) {
        originalBuilder.addMetric("metric_double_" + i, FieldSpec.DataType.DOUBLE);
      } else if (i % 3 == 1) {
        originalBuilder.addMetric("metric_float_" + i, FieldSpec.DataType.FLOAT);
      } else {
        originalBuilder.addMetric("metric_long_" + i, FieldSpec.DataType.LONG);
      }
    }

    // Add a timestamp dimension instead of time column to simplify the benchmark
    originalBuilder.addSingleValueDimension("timestamp", FieldSpec.DataType.LONG);

    _originalSchema = originalBuilder.build();

    // Create updated schema based on evolution type
    Schema.SchemaBuilder updatedBuilder = new Schema.SchemaBuilder();
    updatedBuilder.setSchemaName("testSchema");

    // Copy all existing fields, potentially with data type changes
    for (FieldSpec fieldSpec : _originalSchema.getAllFieldSpecs()) {
      if (_schemaEvolutionType.equals("CHANGE_DATATYPES") && fieldSpec.getName().contains("int")) {
        // Change some INT columns to LONG for compatibility testing
        if (fieldSpec.getName().endsWith("_1")) {
          updatedBuilder.addSingleValueDimension(fieldSpec.getName(), FieldSpec.DataType.LONG);
        } else {
          updatedBuilder.addField(fieldSpec);
        }
      } else {
        updatedBuilder.addField(fieldSpec);
      }
    }

    // Add new columns for schema evolution (maintain 60/40 dimension/metric ratio)
    if (_schemaEvolutionType.equals("ADD_COLUMNS")) {
      int newColumnsToAdd = Math.max(2, _numColumns / 10); // Add ~10% more columns (min 2)
      int newDimensions = (int) (newColumnsToAdd * 0.6);
      int newMetrics = newColumnsToAdd - newDimensions;

      // Add new dimension columns (primitive types only)
      for (int i = 0; i < newDimensions; i++) {
        if (i % 2 == 0) {
          updatedBuilder.addSingleValueDimension("new_dim_string_" + i, FieldSpec.DataType.STRING);
        } else {
          updatedBuilder.addSingleValueDimension("new_dim_int_" + i, FieldSpec.DataType.INT);
        }
      }

      // Add new metric columns (primitive types only)
      for (int i = 0; i < newMetrics; i++) {
        if (i % 2 == 0) {
          updatedBuilder.addMetric("new_metric_double_" + i, FieldSpec.DataType.DOUBLE);
        } else {
          updatedBuilder.addMetric("new_metric_float_" + i, FieldSpec.DataType.FLOAT);
        }
      }
    }

    _updatedSchema = updatedBuilder.build();
  }

  /**
   * Create test table configurations for original and updated scenarios.
   */
  private void createTestTableConfigs() {
    // Create original table config (no time column for simplified benchmark)
    _originalTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    // Create updated table config with potential index changes
    TableConfigBuilder updatedBuilder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable");

    // Add indexing configurations based on schema evolution
    IndexingConfig indexingConfig = new IndexingConfig();

    if (_schemaEvolutionType.equals("ADD_COLUMNS") || _schemaEvolutionType.equals("CHANGE_DATATYPES")) {
      // Add some inverted indexes for new/changed columns
      List<String> invertedIndexColumns = Arrays.asList("dim_string_0", "dim_int_1");
      indexingConfig.setInvertedIndexColumns(invertedIndexColumns);

      // Add range indexes
      List<String> rangeIndexColumns = Arrays.asList("dim_long_2", "metric_double_0");
      indexingConfig.setRangeIndexColumns(rangeIndexColumns);
    }

    _updatedTableConfig = updatedBuilder.build();
    _updatedTableConfig.setIndexingConfig(indexingConfig);
  }

  /**
   * Generate a test segment with synthetic data.
   */
  private void generateTestSegment()
      throws Exception {
    // Create segment generator config
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_originalTableConfig, _originalSchema);
    config.setOutDir(_tempDir.getAbsolutePath());
    config.setSegmentName("testSegment");

    // Create synthetic data generator
    SyntheticDataGenerator dataGenerator = new SyntheticDataGenerator(_originalSchema, _numRows);

    // Generate segment
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, dataGenerator);
    driver.build();

    // Move generated segment to segment directory
    File generatedSegment = new File(_tempDir, "testSegment");
    if (generatedSegment.exists()) {
      FileUtils.moveDirectory(generatedSegment, new File(_segmentDir, "testSegment"));
    }
  }

  /**
   * Wrapper class to access RefreshSegmentTaskExecutor methods.
   */
  public static class RefreshSegmentTaskExecutorWrapper extends RefreshSegmentTaskExecutor {

    public void executeRowBasedRefresh(File indexDir, File workingDir, TableConfig tableConfig,
        SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema,
        Map<String, String> taskConfigs)
        throws Exception {

      // Traditional row-based approach
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata,
          segmentName, schema);
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      recordReader.init(indexDir, null, null);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      recordReader.close();
    }

    public void executeColumnBasedRefresh(File indexDir, File workingDir, TableConfig tableConfig,
        SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema,
        Map<String, String> taskConfigs)
        throws Exception {

      // Column-based approach using rebuildSegment
      org.apache.pinot.segment.spi.ImmutableSegment sourceSegment =
          org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader.load(indexDir,
              org.apache.pinot.spi.utils.ReadMode.mmap);

      try {
        SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata,
            segmentName, schema);
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

        // Initialize with a PinotSegmentRecordReader for compatibility, but we'll use rebuildSegment
        try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
          recordReader.init(sourceSegment);
          driver.init(config, recordReader);

          // Use the optimized column-wise rebuilding method
          driver.rebuildSegment(sourceSegment);
        }
      } finally {
        sourceSegment.destroy();
      }
    }

    // Make the protected method accessible
    public static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
        SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema) {
      tableConfig.getIndexingConfig().setCreateInvertedIndexDuringSegmentGeneration(true);
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(workingDir.getPath());
      config.setSegmentName(segmentName);
      return config;
    }
  }

  /**
   * Create test schemas with advanced indexing (inverted, range, no-dictionary).
   */
  private void createTestSchemasWithIndexes() {
    // Create original schema with advanced indexing considerations
    Schema.SchemaBuilder originalBuilder = new Schema.SchemaBuilder();
    originalBuilder.setSchemaName("testSchemaWithIndexes");

    // Add base columns with 60% dimensions, 40% metrics (balanced for wide tables)
    int numDimensions = (int) (_numColumns * 0.6); // 60% dimensions
    int numMetrics = _numColumns - numDimensions; // 40% metrics

    // Add dimension columns (some will have inverted/range indexes, some no-dictionary)
    for (int i = 0; i < numDimensions; i++) {
      if (i % 4 == 0) {
        // String columns - some with inverted index
        originalBuilder.addSingleValueDimension("dim_string_" + i, FieldSpec.DataType.STRING);
      } else if (i % 4 == 1) {
        // INT columns - some with range index
        originalBuilder.addSingleValueDimension("dim_int_" + i, FieldSpec.DataType.INT);
      } else if (i % 4 == 2) {
        // LONG columns - some no-dictionary
        originalBuilder.addSingleValueDimension("dim_long_" + i, FieldSpec.DataType.LONG);
      } else {
        // Mixed string columns for no-dictionary testing
        originalBuilder.addSingleValueDimension("dim_string_nodict_" + i, FieldSpec.DataType.STRING);
      }
    }

    // Add metric columns (primitive types: DOUBLE, FLOAT, LONG)
    for (int i = 0; i < numMetrics; i++) {
      if (i % 3 == 0) {
        originalBuilder.addMetric("metric_double_" + i, FieldSpec.DataType.DOUBLE);
      } else if (i % 3 == 1) {
        originalBuilder.addMetric("metric_float_" + i, FieldSpec.DataType.FLOAT);
      } else {
        originalBuilder.addMetric("metric_long_" + i, FieldSpec.DataType.LONG);
      }
    }

    // Add a timestamp dimension
    originalBuilder.addSingleValueDimension("timestamp", FieldSpec.DataType.LONG);

    _originalSchema = originalBuilder.build();

    // Create updated schema - same as original for index testing
    _updatedSchema = _originalSchema;
  }

  /**
   * Create test table configurations with advanced indexing.
   */
  private void createTestTableConfigsWithIndexes() {
    // Create table config with advanced indexing
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTableWithIndexes");

    // Create indexing configuration with multiple index types
    IndexingConfig indexingConfig = new IndexingConfig();

    // Add inverted indexes on string columns
    List<String> invertedIndexColumns = Arrays.asList(
        "dim_string_0", "dim_string_4", "dim_string_8", "dim_string_12"
    );
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);

    // Add range indexes on numeric columns
    List<String> rangeIndexColumns = Arrays.asList(
        "dim_int_1", "dim_int_5", "dim_long_2", "metric_double_0", "metric_long_2"
    );
    indexingConfig.setRangeIndexColumns(rangeIndexColumns);

    // Add no-dictionary columns (high cardinality strings and some numerics)
    List<String> noDictionaryColumns = Arrays.asList(
        "dim_string_nodict_3", "dim_string_nodict_7", "dim_long_6", "dim_long_10"
    );
    indexingConfig.setNoDictionaryColumns(noDictionaryColumns);

    // Enable sorted column for better performance testing
    indexingConfig.setSortedColumn(Arrays.asList("dim_int_1"));

    _originalTableConfig = builder.build();
    _originalTableConfig.setIndexingConfig(indexingConfig);
    _updatedTableConfig = _originalTableConfig;
  }

  /**
   * Benchmark row-wise segment refresh with advanced indexing.
   */
  public long benchmarkRowWiseRefreshWithIndexes()
      throws Exception {
    System.out.println("Running row-wise refresh benchmark with advanced indexing...");

    File workingDirRowWise = new File(_workingDir, "row-wise-indexes");
    workingDirRowWise.mkdirs();

    long startTime = System.currentTimeMillis();

    RefreshSegmentTaskExecutorWrapper executor = new RefreshSegmentTaskExecutorWrapper();
    File[] segmentDirs = _segmentDir.listFiles();
    if (segmentDirs != null && segmentDirs.length > 0) {
      File indexDir = segmentDirs[0];
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      Map<String, String> taskConfigs = new HashMap<>();
      taskConfigs.put(MinionConstants.RefreshSegmentTask.COLUMNAR_RELOAD_AND_SKIP_TRANSFORMATION, "false");

      executor.executeRowBasedRefresh(indexDir, workingDirRowWise, _updatedTableConfig,
          segmentMetadata, "testSegmentWithIndexes", _updatedSchema, taskConfigs);
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Row-wise refresh with indexes completed in: " + duration + " ms");
    return duration;
  }

  /**
   * Benchmark column-wise segment refresh with advanced indexing.
   */
  public long benchmarkColumnWiseRefreshWithIndexes()
      throws Exception {
    System.out.println("Running column-wise refresh benchmark with advanced indexing...");

    File workingDirColumnWise = new File(_workingDir, "column-wise-indexes");
    workingDirColumnWise.mkdirs();

    long startTime = System.currentTimeMillis();

    RefreshSegmentTaskExecutorWrapper executor = new RefreshSegmentTaskExecutorWrapper();
    File[] segmentDirs = _segmentDir.listFiles();
    if (segmentDirs != null && segmentDirs.length > 0) {
      File indexDir = segmentDirs[0];
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      Map<String, String> taskConfigs = new HashMap<>();
      taskConfigs.put(MinionConstants.RefreshSegmentTask.COLUMNAR_RELOAD_AND_SKIP_TRANSFORMATION, "true");

      executor.executeColumnBasedRefresh(indexDir, workingDirColumnWise, _updatedTableConfig,
          segmentMetadata, "testSegmentWithIndexes", _updatedSchema, taskConfigs);
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Column-wise refresh with indexes completed in: " + duration + " ms");
    return duration;
  }

  /**
   * Run benchmark with advanced indexing.
   */
  public void runIndexBenchmark()
      throws Exception {
    System.out.println("=== Advanced Indexing Benchmark ===");
    System.out.println("Configuration:");
    System.out.println("  Rows: " + _numRows);
    System.out.println("  Columns: " + _numColumns);
    System.out.println("  Inverted Indexes: 4 string columns");
    System.out.println("  Range Indexes: 5 numeric columns");
    System.out.println("  No-Dictionary: 4 high-cardinality columns");
    System.out.println("  Sorted Column: dim_int_1");
    System.out.println();

    try {
      // Setup with advanced indexing
      setup();

      // Override schemas and configs for indexing
      createTestSchemasWithIndexes();
      createTestTableConfigsWithIndexes();

      // Regenerate segment with advanced indexing
      generateTestSegmentWithIndexes();

      // Run benchmarks
      long rowWiseTime = benchmarkRowWiseRefreshWithIndexes();
      long columnWiseTime = benchmarkColumnWiseRefreshWithIndexes();

      // Results
      System.out.println();
      System.out.println("=== ADVANCED INDEXING BENCHMARK RESULTS ===");
      System.out.println("Row-wise refresh time (with indexes):    " + rowWiseTime + " ms");
      System.out.println("Column-wise refresh time (with indexes): " + columnWiseTime + " ms");

      if (rowWiseTime > 0) {
        double improvement = ((double) (rowWiseTime - columnWiseTime) / rowWiseTime) * 100;
        System.out.println("Performance improvement:                 " + String.format("%.2f", improvement) + "%");
        System.out.println("Speedup factor:                          " + String.format("%.2fx",
            (double) rowWiseTime / columnWiseTime));
      }

      System.out.println();
      System.out.println("Index Configuration Impact:");
      System.out.println("  - Inverted indexes: Enable fast filtering on string dimensions");
      System.out.println("  - Range indexes: Optimize numeric range queries");
      System.out.println("  - No-dictionary: Handle high-cardinality data efficiently");
      System.out.println("  - Sorted column: Improve query performance and compression");
    } finally {
      // Cleanup
      tearDown();
    }
  }

  /**
   * Generate a test segment with advanced indexing configurations.
   */
  private void generateTestSegmentWithIndexes()
      throws Exception {
    // Create segment generator config with indexing
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_originalTableConfig, _originalSchema);
    config.setOutDir(_tempDir.getAbsolutePath());
    config.setSegmentName("testSegmentWithIndexes");

    // Create synthetic data generator optimized for indexing scenarios
    SyntheticDataGeneratorWithIndexes dataGenerator = new SyntheticDataGeneratorWithIndexes(_originalSchema, _numRows);

    // Generate segment
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, dataGenerator);
    driver.build();

    // Move generated segment to segment directory
    File generatedSegment = new File(_tempDir, "testSegmentWithIndexes");
    if (generatedSegment.exists()) {
      FileUtils.moveDirectory(generatedSegment, new File(_segmentDir, "testSegmentWithIndexes"));
    }
  }

  /**
   * Enhanced synthetic data generator for indexing scenarios.
   */
  private static class SyntheticDataGeneratorWithIndexes extends SyntheticDataGenerator {

    public SyntheticDataGeneratorWithIndexes(Schema schema, int numRows) {
      super(schema, numRows);
    }

    protected Object generateValue(FieldSpec fieldSpec, int rowIndex) {
      String fieldName = fieldSpec.getName();
      FieldSpec.DataType dataType = fieldSpec.getDataType();

      // Generate data optimized for different index types
      switch (dataType) {
        case STRING:
          if (fieldName.contains("nodict")) {
            // High cardinality for no-dictionary columns
            return generateHighCardinalityString(fieldName, rowIndex);
          } else {
            // Controlled cardinality for inverted index columns
            return generateInvertedIndexString(fieldName, rowIndex);
          }
        case INT:
          if (fieldName.equals("dim_int_1")) {
            // Sorted column - generate sorted values
            return rowIndex / 100; // Creates sorted groups
          } else {
            // Range index friendly - clustered values
            return generateRangeIndexInt(fieldName, rowIndex);
          }
        case LONG:
          if (fieldName.contains("nodict")) {
            // High cardinality for no-dictionary
            return generateHighCardinalityLong(fieldName, rowIndex);
          } else if (fieldName.contains("timestamp")) {
            return System.currentTimeMillis() - (rowIndex * 1000L);
          } else {
            return generateRangeIndexLong(fieldName, rowIndex);
          }
        case FLOAT:
          return generateRangeIndexFloat(fieldName, rowIndex);
        case DOUBLE:
          return generateRangeIndexDouble(fieldName, rowIndex);
        default:
          return super.generateValue(fieldName, dataType, rowIndex);
      }
    }

    private String generateHighCardinalityString(String fieldName, int rowIndex) {
      // Very high cardinality for no-dictionary testing
      return fieldName + "_unique_" + rowIndex + "_" + (rowIndex % 1000);
    }

    private String generateInvertedIndexString(String fieldName, int rowIndex) {
      // Controlled cardinality for inverted index efficiency
      String[] categories = {"category_A", "category_B", "category_C", "category_D", "category_E"};
      return categories[rowIndex % categories.length] + "_" + (rowIndex % 50);
    }

    private int generateRangeIndexInt(String fieldName, int rowIndex) {
      // Generate clustered ranges for efficient range queries
      return (rowIndex / 1000) * 1000 + (rowIndex % 100);
    }

    private long generateHighCardinalityLong(String fieldName, int rowIndex) {
      // High cardinality long values
      return ((long) rowIndex * 1000000L) + (rowIndex % 10000);
    }

    private long generateRangeIndexLong(String fieldName, int rowIndex) {
      // Range-friendly long values
      return ((long) (rowIndex / 5000)) * 100000L + (rowIndex % 1000);
    }

    private float generateRangeIndexFloat(String fieldName, int rowIndex) {
      // Range-friendly float values
      return (float) ((rowIndex / 1000) * 100.0 + (rowIndex % 100));
    }

    private double generateRangeIndexDouble(String fieldName, int rowIndex) {
      // Range-friendly double values
      return ((rowIndex / 2000) * 1000.0) + ((rowIndex % 500) * 0.1);
    }
  }

  /**
   * Main method to run the benchmark.
   */
  public static void main(String[] args)
      throws Exception {
    // Default parameters
    int numRows = 500000;
    int numColumns = 30;
    String schemaEvolutionType = "NONE";
    boolean runIndexBenchmark = false;

    // Parse command line arguments
    for (int i = 0; i < args.length; ) {
      switch (args[i]) {
        case "--rows":
          if (i + 1 < args.length) {
            numRows = Integer.parseInt(args[i + 1]);
            i += 2;
          } else {
            System.out.println("Missing value for --rows");
            return;
          }
          break;
        case "--columns":
          if (i + 1 < args.length) {
            numColumns = Integer.parseInt(args[i + 1]);
            i += 2;
          } else {
            System.out.println("Missing value for --columns");
            return;
          }
          break;
        case "--evolution":
          if (i + 1 < args.length) {
            schemaEvolutionType = args[i + 1];
            i += 2;
          } else {
            System.out.println("Missing value for --evolution");
            return;
          }
          break;
        case "--indexes":
          runIndexBenchmark = true;
          i += 1;
          break;
        case "--help":
          System.out.println(
              "Usage: SimpleRefreshSegmentBenchmark [--rows N] [--columns N] [--evolution TYPE] [--indexes]");
          System.out.println("  --rows N        Number of rows (default: 500000)");
          System.out.println("  --columns N     Number of columns (default: 30)");
          System.out.println(
              "  --evolution TYPE Schema evolution type: NONE, ADD_COLUMNS, CHANGE_DATATYPES (default: NONE)");
          System.out.println("  --indexes       Run advanced indexing benchmark (inverted, range, no-dictionary)");
          return;
        default:
          System.out.println("Unknown argument: " + args[i]);
          System.out.println("Use --help for usage information.");
          return;
      }
    }

    SimpleRefreshSegmentBenchmark benchmark =
        new SimpleRefreshSegmentBenchmark(numRows, numColumns, schemaEvolutionType);

    if (runIndexBenchmark) {
      // Run advanced indexing benchmark
      benchmark.runIndexBenchmark();
    } else {
      // Run standard benchmark
      System.out.println("=== Simple Refresh Segment Benchmark ===");
      System.out.println("Configuration:");
      System.out.println("  Rows: " + numRows);
      System.out.println("  Columns: " + numColumns);
      System.out.println("  Schema Evolution: " + schemaEvolutionType);
      System.out.println();

      try {
        // Setup
        benchmark.setup();

        // Run benchmarks
        long rowWiseTime = benchmark.benchmarkRowWiseRefresh();
        long columnWiseTime = benchmark.benchmarkColumnWiseRefresh();

        // Results
        System.out.println();
        System.out.println("=== BENCHMARK RESULTS ===");
        System.out.println("Row-wise refresh time:    " + rowWiseTime + " ms");
        System.out.println("Column-wise refresh time: " + columnWiseTime + " ms");

        if (rowWiseTime > 0) {
          double improvement = ((double) (rowWiseTime - columnWiseTime) / rowWiseTime) * 100;
          System.out.println("Performance improvement:  " + String.format("%.2f", improvement) + "%");
          System.out.println(
              "Speedup factor:           " + String.format("%.2fx", (double) rowWiseTime / columnWiseTime));
        }
      } finally {
        // Cleanup
        benchmark.tearDown();
      }
    }
  }
}
