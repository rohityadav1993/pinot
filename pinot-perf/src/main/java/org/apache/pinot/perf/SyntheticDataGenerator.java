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
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Synthetic data generator for creating test segments with configurable characteristics.
  * This generator creates realistic data distributions for benchmarking segment refresh operations.
 *
 * Generates data for 50 columns (25 dimensions, 25 metrics) with 1 million rows.
 */
public class SyntheticDataGenerator implements RecordReader {
  private final Schema _schema;
  private final int _numRows;
  private final Random _random;
  private int _currentRowIndex;

  // Data generation parameters for realistic distributions
  private static final String[] STRING_VALUES = {
    "apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew",
    "kiwi", "lemon", "mango", "orange", "papaya", "quince", "raspberry", "strawberry",
    "watermelon", "blueberry", "coconut", "dragonfruit"
  };

  private static final String[] CITY_NAMES = {
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
    "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Boston"
  };

  private static final String[] COMPANY_NAMES = {
    "TechCorp", "DataSys", "CloudInc", "StreamCo", "AnalyticsPro", "QueryTech", "IndexCorp",
    "MetricSoft", "DimensionLab", "AggregateInc", "FilterSys", "JoinTech", "SortCorp",
    "GroupLab", "WindowInc", "PartitionCo", "ShardTech", "ReplicaSys", "ClusterCorp", "NodeInc"
  };

  public SyntheticDataGenerator(Schema schema, int numRows) {
    _schema = schema;
    _numRows = numRows;
    _random = new Random(42); // Fixed seed for reproducible results
    _currentRowIndex = 0;
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, RecordReaderConfig recordReaderConfig) throws IOException {
    _currentRowIndex = 0;
  }

  public void init() {
    _currentRowIndex = 0;
  }

  @Override
  public boolean hasNext() {
    return _currentRowIndex < _numRows;
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    if (!hasNext()) {
      return null;
    }

    reuse.clear();

    // Generate values for each field in the schema
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      FieldSpec.DataType dataType = fieldSpec.getDataType();

      Object value = generateValue(fieldName, dataType, _currentRowIndex);
      reuse.putValue(fieldName, value);
    }

    _currentRowIndex++;
    return reuse;
  }

  @Override
  public void rewind() throws IOException {
    _currentRowIndex = 0;
  }

  @Override
  public void close() throws IOException {
    // Nothing to close
  }

  /**
   * Generate realistic values based on field name and data type.
   */
  private Object generateValue(String fieldName, FieldSpec.DataType dataType, int rowIndex) {
    // Create some patterns and correlations for more realistic data
    int seed = fieldName.hashCode() + rowIndex;
    Random fieldRandom = new Random(seed);

    switch (dataType) {
      case STRING:
        if (fieldName.contains("city")) {
          return CITY_NAMES[fieldRandom.nextInt(CITY_NAMES.length)];
        } else if (fieldName.contains("company")) {
          return COMPANY_NAMES[fieldRandom.nextInt(COMPANY_NAMES.length)];
        } else {
          return STRING_VALUES[fieldRandom.nextInt(STRING_VALUES.length)];
        }

      case INT:
        if (fieldName.contains("id")) {
          // ID fields: 0 to 100,000
          return fieldRandom.nextInt(100_000);
        } else if (fieldName.contains("count")) {
          // Count fields: 0 to 1,000
          return fieldRandom.nextInt(1_000);
        } else {
          // General int fields: -10,000 to 10,000
          return fieldRandom.nextInt(20_000) - 10_000;
        }

      case LONG:
        if (fieldName.contains("timestamp")) {
          // Timestamp fields: recent timestamps
          return System.currentTimeMillis() - fieldRandom.nextInt(365 * 24 * 60 * 60 * 1000);
        } else if (fieldName.contains("id")) {
          // ID fields: 0 to 1,000,000
          return (long) fieldRandom.nextInt(1_000_000);
        } else {
          // General long fields: large range
          return fieldRandom.nextLong() % 1_000_000_000L;
        }

      case FLOAT:
        if (fieldName.contains("percentage") || fieldName.contains("ratio")) {
          // Percentage/ratio fields: 0.0 to 1.0
          return fieldRandom.nextFloat();
        } else if (fieldName.contains("price") || fieldName.contains("amount")) {
          // Price/amount fields: 0.0 to 10,000.0
          return fieldRandom.nextFloat() * 10_000;
        } else {
          // General float fields: -1000.0 to 1000.0
          return (fieldRandom.nextFloat() - 0.5f) * 2000;
        }

      case DOUBLE:
        if (fieldName.contains("latitude") || fieldName.contains("longitude")) {
          // Geo coordinates: realistic lat/lon ranges
          return fieldName.contains("latitude")
              ? (fieldRandom.nextDouble() - 0.5) * 180  // -90 to 90
              : (fieldRandom.nextDouble() - 0.5) * 360; // -180 to 180
        } else if (fieldName.contains("revenue") || fieldName.contains("sales")) {
          // Revenue/sales fields: 0.0 to 1,000,000.0
          return fieldRandom.nextDouble() * 1_000_000;
        } else {
          // General double fields: -10,000.0 to 10,000.0
          return (fieldRandom.nextDouble() - 0.5) * 20_000;
        }

      default:
        return null;
    }
  }

  /**
   * Get the schema being used for data generation.
   */
  public Schema getSchema() {
    return _schema;
  }

  /**
   * Get the total number of rows to generate.
   */
  public int getNumRows() {
    return _numRows;
  }

  /**
   * Get current progress (number of rows generated so far).
   */
  public int getCurrentRowIndex() {
    return _currentRowIndex;
  }

  /**
   * Check if data generation is complete.
   */
  public boolean isComplete() {
    return _currentRowIndex >= _numRows;
  }

  /**
   * Get progress percentage.
   */
  public double getProgress() {
    return ((double) _currentRowIndex / _numRows) * 100.0;
  }
}
