# Segment Refresh Performance Benchmark

This benchmark compares the performance of **row-major** vs **column-major** segment refresh operations in Apache Pinot.

## Overview

The benchmark tests the performance improvements achieved by using columnar segment building (column-major) compared to the traditional row-based approach (row-major) for segment refresh operations.

### Test Parameters
- **Rows**: 1,000,000 records
- **Columns**: 50 total (25 dimensions, 25 metrics)
- **Column Types**: Mixed data types (STRING, INT, LONG, FLOAT, DOUBLE)
- **Scenarios**: 4 different test scenarios

## Test Scenarios

The benchmark evaluates performance across four key scenarios:

1. **No Change**: Baseline refresh with identical schema and configuration
2. **New Column Addition**: Adding a new column to the schema
3. **Data Type Change**: Converting existing column data types (e.g., INT → LONG, FLOAT → DOUBLE)
4. **Index Updates**: Adding inverted indexes to existing columns

## Architecture

### Components

- **SegmentRefreshBenchmark**: Main benchmark orchestrator
- **SyntheticDataGenerator**: Generates realistic test data with varied distributions
- **BenchmarkResult**: Container for individual scenario results
- **BenchmarkResults**: Aggregates and reports all results

### Data Generation

The synthetic data generator creates realistic data patterns:
- **String fields**: Fruits, cities, company names
- **Numeric fields**: IDs, counts, timestamps, prices, coordinates
- **Distributions**: Realistic ranges and correlations based on field names

## Usage

### Prerequisites

```bash
# Build the project and generate benchmark scripts
mvn clean package -DskipTests
```

### Running the Benchmark

#### Using Generated Script (Recommended)
```bash
cd target/pinot-perf-pkg/bin
./pinot-SegmentRefreshBenchmark.sh
```

#### Alternative: Direct Java Execution (Development)
```bash
# After building, run directly with java
cd target/classes
java -cp "../pinot-perf-pkg/lib/*:." org.apache.pinot.perf.SegmentRefreshBenchmark
```

### Expected Output

The benchmark will output:
1. **Console Output**: Real-time progress and results
2. **CSV File**: Detailed results (`benchmark_results_<timestamp>.csv`)

## Results Format

### Console Output
```
=== BENCHMARK RESULTS ===
Scenario             | Row-Major(ms) | Column-Major(ms) | Difference(ms) | Improvement(%)
------------------------------------------------------------------------------
no_change            | 5234          | 3567             | 1667           | 31.85%
new_column           | 5891          | 3234             | 2657           | 45.11%
data_type_change     | 6123          | 3456             | 2667           | 43.55%
index_update         | 7234          | 4123             | 3111           | 43.01%
```

### CSV Output
```csv
Scenario,Row-Major(ms),Column-Major(ms),Difference(ms),Improvement(%)
no_change,5234,3567,1667,31.85
new_column,5891,3234,2657,45.11
data_type_change,6123,3456,2667,43.55
index_update,7234,4123,3111,43.01
```

## Performance Expectations

Based on the columnar segment building approach, expected improvements:

- **No Change**: 20-40% improvement
- **New Column Addition**: 30-50% improvement (avoids row-by-row processing)
- **Data Type Changes**: 25-45% improvement (efficient column-wise conversion)
- **Index Updates**: 30-50% improvement (column-wise index building)

## Technical Details

### Row-Major Approach
- Loads segment using `PinotSegmentRecordReader`
- Processes data record-by-record
- Traditional segment building pipeline

### Column-Major Approach
- Uses `PinotSegmentColumnReaderFactory`
- Processes data column-by-column
- Leverages `SegmentColumnarIndexCreator.indexColumn()`
- Avoids unnecessary data transformations and sorting

### Memory Usage
- **JVM Settings**: `-Xmx8g -Xms4g -XX:+UseG1GC`
- **Temporary Space**: Uses system temp directory
- **Cleanup**: Automatic cleanup of temporary files

## Configuration

### Benchmark Parameters (Configurable)
```java
private static final int NUM_ROWS = 1_000_000;
private static final int NUM_COLUMNS = 50;
private static final int NUM_DIMENSIONS = 25;
private static final int NUM_METRICS = 25;
```

### JVM Tuning
For optimal performance, consider:
```bash
export JAVA_OPTS="-Xmx8g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

## Troubleshooting

### Common Issues

1. **Out of Memory**: Increase JVM heap size
   ```bash
   export JAVA_OPTS="-Xmx16g -Xms8g"
   ```

2. **Compilation Errors**: Ensure all dependencies are built
   ```bash
   cd ../
   mvn install -pl pinot-spi,pinot-segment-local,pinot-core -DskipTests
   cd pinot-perf
   ```

3. **Permission Issues**: Ensure write access to temp directory
   ```bash
   chmod 755 /tmp
   ```

### Debugging

Enable debug logging:
```bash
mvn exec:java -Dexec.mainClass="org.apache.pinot.perf.SegmentRefreshBenchmark" \
  -Dlog4j.configuration=file:log4j.properties -X
```

## Extending the Benchmark

### Adding New Scenarios
```java
// In runBenchmark() method
Schema customSchema = createCustomSchema();
TableConfig customConfig = createCustomTableConfig();
BenchmarkResult customResult = benchmarkScenario("custom", customSchema, customConfig);
results.addResult("custom", customResult);
```

### Modifying Data Generation
```java
// In SyntheticDataGenerator.generateValue()
case CUSTOM_TYPE:
    return generateCustomValue(fieldName, rowIndex);
```

### Custom Metrics
```java
// Add timing for specific operations
long operationStart = System.nanoTime();
// ... operation ...
long operationTime = System.nanoTime() - operationStart;
```

## Related Documentation

- [Columnar Segment Building Design](../docs/columnar-segment-building.md)
- [Segment Refresh Task Documentation](../docs/segment-refresh.md)
- [Performance Tuning Guide](../docs/performance-tuning.md)

## Contributing

When adding new benchmark scenarios:
1. Follow existing naming conventions
2. Add comprehensive documentation
3. Include expected performance ranges
4. Test with various data sizes
5. Update this README

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
