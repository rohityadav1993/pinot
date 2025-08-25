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
 */
public class SyntheticDataGenerator implements RecordReader {
  private final Schema _schema;
  private final int _numRows;
  private final Random _random;
  private int _currentRowIndex;

  // Data generation parameters
  private static final String[] STRING_VALUES = {
    "apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew",
    "kiwi", "lemon", "mango", "orange", "papaya", "quince", "raspberry", "strawberry"
  };

  private static final String[] CITY_NAMES = {
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus"
  };

  public SyntheticDataGenerator(Schema schema, int numRows) {
    _schema = schema;
    _numRows = numRows;
    _random = new Random(42); // Fixed seed for reproducible results
    _currentRowIndex = 0;
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, RecordReaderConfig recordReaderConfig) {
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
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
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
  public void rewind() {
    _currentRowIndex = 0;
  }

  @Override
  public void close() {
    // No resources to close
  }

  /**
   * Generate a synthetic value based on field name, data type, and row index.
   * Creates realistic data distributions with controlled cardinality.
   */
  protected Object generateValue(String fieldName, FieldSpec.DataType dataType, int rowIndex) {
    switch (dataType) {
      case STRING:
        return generateStringValue(fieldName, rowIndex);
      case INT:
        return generateIntValue(fieldName, rowIndex);
      case LONG:
        return generateLongValue(fieldName, rowIndex);
      case FLOAT:
        return generateFloatValue(fieldName, rowIndex);
      case DOUBLE:
        return generateDoubleValue(fieldName, rowIndex);
      case BOOLEAN:
        return _random.nextBoolean();
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  /**
   * Generate string values with realistic cardinality and distribution for wide tables.
   * Optimized for primitive types only with controlled cardinality.
   */
  private String generateStringValue(String fieldName, int rowIndex) {
    if (fieldName.startsWith("new_")) {
      // For new columns added during schema evolution
      return "new_value_" + (rowIndex % 100);
    } else if (fieldName.contains("string")) {
      // Use predefined values for better performance in wide tables
      return STRING_VALUES[rowIndex % STRING_VALUES.length];
    } else {
      // Default string generation with controlled cardinality for wide tables
      int cardinality = Math.min(1000, Math.max(100, _numRows / 100));
      return fieldName + "_" + (rowIndex % cardinality);
    }
  }

  /**
   * Generate integer values optimized for wide table benchmarking.
   * Simple distribution patterns for consistent performance measurement.
   */
  private int generateIntValue(String fieldName, int rowIndex) {
    if (fieldName.startsWith("new_")) {
      return rowIndex % 10000; // New columns with moderate cardinality
    } else if (fieldName.contains("int")) {
      return rowIndex % 50000; // Controlled cardinality for dimensions
    } else {
      return _random.nextInt(100000); // Default range
    }
  }

  /**
   * Generate long values optimized for wide table benchmarking.
   * Includes timestamp handling and metric values.
   */
  private long generateLongValue(String fieldName, int rowIndex) {
    if (fieldName.contains("timestamp") || fieldName.contains("time")) {
      // Simple timestamp generation for benchmarking
      return System.currentTimeMillis() - (rowIndex * 1000L);
    } else if (fieldName.contains("metric")) {
      return _random.nextLong() % 1000000L; // Metric values
    } else {
      // For data type evolution testing (INT -> LONG) or dimension values
      return (long) generateIntValue(fieldName, rowIndex);
    }
  }

  /**
   * Generate float values for metrics in wide table benchmarking.
   * Simplified for consistent performance measurement.
   */
  private float generateFloatValue(String fieldName, int rowIndex) {
    if (fieldName.startsWith("new_")) {
      return _random.nextFloat() * 1000.0f; // New metrics
    } else {
      return _random.nextFloat() * 10000.0f; // Standard metric range
    }
  }

  /**
   * Generate double values for high-precision metrics in wide table benchmarking.
   * Optimized for primitive types only.
   */
  private double generateDoubleValue(String fieldName, int rowIndex) {
    if (fieldName.startsWith("new_")) {
      return _random.nextDouble() * 1000.0; // New metrics
    } else {
      return _random.nextDouble() * 100000.0; // Standard high-precision metrics
    }
  }

  /**
   * Get the schema for this data generator.
   */
  public Schema getSchema() {
    return _schema;
  }

  /**
   * Get the total number of rows to be generated.
   */
  public int getNumRows() {
    return _numRows;
  }
}
