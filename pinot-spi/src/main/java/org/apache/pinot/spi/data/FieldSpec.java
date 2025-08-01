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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.OptBoolean;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 * The <code>FieldSpec</code> class contains all specs related to any field (column) in {@link Schema}.
 * <p>Specs stored are as followings:
 * <ul>
 *   <li>"name": name of the field.</li>
 *   <li>"dataType": type of the data stored (e.g. INTEGER, LONG, FLOAT, DOUBLE, STRING).</li>
 *   <li>"singleValueField": single-value or multi-value field.</li>
 *   <li>"notNull": whether the column accepts nulls or not. Defaults to false (accepts nulls).</li>
 *   <li>"maxLength": maximum length of the column. Defaults to 512.</li>
 *   <li>"maxLengthExceedStrategy": the strategy to handle the case when the column exceeds the max length.</li>
 *   <li>"allowTrailingZeros": whether to allow trailing zeros for a BIG_DECIMAL column.</li>
 *   <li>"defaultNullValue": when no value found for this field, use this value.</li>
 *   <li>"virtualColumnProvider": the virtual column provider to use for this field.</li>
 * </ul>
 */
@SuppressWarnings("unused")
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "fieldType",
    requireTypeIdForSubtypes = OptBoolean.FALSE
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DimensionFieldSpec.class, name = "DIMENSION"),
    @JsonSubTypes.Type(value = MetricFieldSpec.class, name = "METRIC"),
    @JsonSubTypes.Type(value = TimeFieldSpec.class, name = "TIME"),
    @JsonSubTypes.Type(value = DateTimeFieldSpec.class, name = "DATE_TIME"),
    @JsonSubTypes.Type(value = ComplexFieldSpec.class, name = "COMPLEX")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class FieldSpec implements Comparable<FieldSpec>, Serializable {
  public static final Integer DEFAULT_DIMENSION_NULL_VALUE_OF_INT = Integer.MIN_VALUE;
  public static final Long DEFAULT_DIMENSION_NULL_VALUE_OF_LONG = Long.MIN_VALUE;
  public static final Float DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT = Float.NEGATIVE_INFINITY;
  public static final Double DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE = Double.NEGATIVE_INFINITY;
  public static final Integer DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN = 0;
  public static final Long DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP = 0L;
  public static final String DEFAULT_DIMENSION_NULL_VALUE_OF_STRING = "null";
  public static final String DEFAULT_DIMENSION_NULL_VALUE_OF_JSON = "null";
  public static final byte[] DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES = new byte[0];
  public static final BigDecimal DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL = BigDecimal.ZERO;

  public static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = 0;
  public static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = 0L;
  public static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = 0.0F;
  public static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = 0.0D;
  public static final BigDecimal DEFAULT_METRIC_NULL_VALUE_OF_BIG_DECIMAL = BigDecimal.ZERO;
  public static final String DEFAULT_METRIC_NULL_VALUE_OF_STRING = "null";
  public static final byte[] DEFAULT_METRIC_NULL_VALUE_OF_BYTES = new byte[0];
  public static final FieldSpecMetadata FIELD_SPEC_METADATA;

  public static final Map DEFAULT_COMPLEX_NULL_VALUE_OF_MAP = Map.of();
  public static final List DEFAULT_COMPLEX_NULL_VALUE_OF_LIST = List.of();
  public static final int DEFAULT_MAX_LENGTH = 512;

  private static MaxLengthExceedStrategy _defaultJsonMaxLengthExceedStrategy = MaxLengthExceedStrategy.NO_ACTION;
  private static int _defaultJsonMaxLength = DEFAULT_MAX_LENGTH;

  public static MaxLengthExceedStrategy getDefaultJsonMaxLengthExceedStrategy() {
    return _defaultJsonMaxLengthExceedStrategy;
  }

  public static void setDefaultJsonMaxLengthExceedStrategy(MaxLengthExceedStrategy defaultJsonMaxLengthExceedStrategy) {
    _defaultJsonMaxLengthExceedStrategy = defaultJsonMaxLengthExceedStrategy;
  }

  public static int getDefaultJsonMaxLength() {
    return _defaultJsonMaxLength;
  }

  public static void setDefaultJsonMaxLength(int defaultJsonMaxLength) {
    _defaultJsonMaxLength = defaultJsonMaxLength;
  }

  static {
    // The metadata on the valid list of {@link DataType} for each {@link FieldType}
    // and the default null values for each combination
    FIELD_SPEC_METADATA = new FieldSpecMetadata();
    for (FieldType fieldType : FieldType.values()) {
      FieldTypeMetadata fieldTypeMetadata = new FieldTypeMetadata();
      for (DataType dataType : DataType.values()) {
        try {
          Schema.validate(fieldType, dataType);
          try {
            fieldTypeMetadata.put(dataType, new DataTypeMetadata(getDefaultNullValue(fieldType, dataType, null)));
          } catch (IllegalStateException ignored) {
            // default null value not defined for the (DataType, FieldType) combination
            // defaulting to null in such cases
            fieldTypeMetadata.put(dataType, new DataTypeMetadata(null));
          }
        } catch (IllegalStateException ignored) {
          // invalid DataType for the given FieldType
        }
      }
      FIELD_SPEC_METADATA.put(fieldType, fieldTypeMetadata);
    }
    for (DataType dataType : DataType.values()) {
      FIELD_SPEC_METADATA.put(dataType, new DataTypeProperties(dataType));
    }
  }

  public enum MaxLengthExceedStrategy {
    TRIM_LENGTH, ERROR, SUBSTITUTE_DEFAULT_VALUE, NO_ACTION
  }

  protected String _name;
  protected DataType _dataType;
  protected boolean _singleValueField = true;
  protected boolean _notNull;

  // Max length applies to STRING, JSON, BYTES columns, and is enforced in {@link SanitizationTransformer}.
  protected Integer _maxLength;
  protected MaxLengthExceedStrategy _maxLengthExceedStrategy;

  // Whether to allow trailing zeros for BIG_DECIMAL columns. Trailing zeros are stripped by default in
  // {@link SpecialValueTransformer}. If this flag is set to true, trailing zeros will be preserved, and it is users'
  // responsibility to ensure there are no big decimal values with same value but different trailing zeros. Read more
  // about why trailing zeros need to be stripped in {@link SpecialValueTransformer}.
  protected boolean _allowTrailingZeros;

  protected Object _defaultNullValue;
  private transient String _stringDefaultNullValue;

  // Transform function to generate this column, can be based on other columns
  @Deprecated // Set this in TableConfig -> IngestionConfig -> TransformConfigs
  protected String _transformFunction;

  protected String _virtualColumnProvider;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public FieldSpec() {
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField) {
    this(name, dataType, isSingleValueField, null, null);
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField, @Nullable Object defaultNullValue) {
    this(name, dataType, isSingleValueField, null, defaultNullValue);
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField, @Nullable Integer maxLength,
      @Nullable Object defaultNullValue) {
    this(name, dataType, isSingleValueField, maxLength, defaultNullValue, null);
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField, @Nullable Integer maxLength,
      @Nullable Object defaultNullValue, @Nullable MaxLengthExceedStrategy maxLengthExceedStrategy) {
    _name = name;
    _dataType = dataType;
    _singleValueField = isSingleValueField;
    _maxLength = maxLength;
    setDefaultNullValue(defaultNullValue);
    _maxLengthExceedStrategy = maxLengthExceedStrategy;
  }

  public abstract FieldType getFieldType();

  public String getName() {
    return _name;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setName(String name) {
    _name = name;
  }

  public DataType getDataType() {
    return _dataType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDataType(DataType dataType) {
    _dataType = dataType;
    _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
  }

  public boolean isSingleValueField() {
    return _singleValueField;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setSingleValueField(boolean isSingleValueField) {
    _singleValueField = isSingleValueField;
  }

  /**
   * Returns whether the column is nullable or not.
   */
  @JsonIgnore
  public boolean isNullable() {
    return !_notNull;
  }

  /**
   * @see #isNullable()
   */
  @JsonIgnore
  public void setNullable(Boolean nullable) {
    _notNull = !nullable;
  }

  public boolean isNotNull() {
    return _notNull;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setNotNull(boolean notNull) {
    _notNull = notNull;
  }

  /**
   * Returns the effective max length to be used.
   * This method should be used in business logic instead of {@code getMaxLength()},
   * as it falls back to data type-specific or global defaults when the field is unset.
   */
  @JsonIgnore
  public int getEffectiveMaxLength() {
    // If explicitly set, return that value
    if (_maxLength != null) {
      return _maxLength;
    }

    // For JSON fields, use configurable default
    if (_dataType == DataType.JSON) {
      return getDefaultJsonMaxLength();
    }

    // For all other fields, use the standard default
    return DEFAULT_MAX_LENGTH;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  // Use getEffectiveMaxLength() for default-aware access.
  @Nullable
  public Integer getMaxLength() {
    return _maxLength;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setMaxLength(@Nullable Integer maxLength) {
    _maxLength = maxLength;
  }

  /**
   * Returns the effective max length exceed strategy to be used.
   * This method should be used in business logic instead of {@code getMaxLengthExceedStrategy()},
   * as it falls back to data type-specific or global defaults when the field is unset.
   */
  @JsonIgnore
  public MaxLengthExceedStrategy getEffectiveMaxLengthExceedStrategy() {
    if (_maxLengthExceedStrategy != null) {
      return _maxLengthExceedStrategy;
    }

    // Apply data type-specific defaults
    switch (_dataType) {
      case STRING:
        return MaxLengthExceedStrategy.TRIM_LENGTH;
      case JSON:
        return getDefaultJsonMaxLengthExceedStrategy();
      default:
        return MaxLengthExceedStrategy.NO_ACTION;
    }
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  // Use getEffectiveMaxLengthExceedStrategy() for default-aware access.
  @Nullable
  public MaxLengthExceedStrategy getMaxLengthExceedStrategy() {
    return _maxLengthExceedStrategy;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setMaxLengthExceedStrategy(@Nullable MaxLengthExceedStrategy maxLengthExceedStrategy) {
    _maxLengthExceedStrategy = maxLengthExceedStrategy;
  }

  public boolean isAllowTrailingZeros() {
    return _allowTrailingZeros;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setAllowTrailingZeros(boolean allowTrailingZeros) {
    _allowTrailingZeros = allowTrailingZeros;
  }

  public Object getDefaultNullValue() {
    return _defaultNullValue;
  }

  public String getDefaultNullValueString() {
    return getStringValue(_defaultNullValue);
  }

  /**
   * Helper method to return the String value for the given object.
   * This is required as not all data types have a toString() (e.g. byte[]).
   *
   * @param value Value for which String value needs to be returned
   * @return String value for the object.
   */
  public static String getStringValue(Object value) {
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    if (value instanceof byte[]) {
      return BytesUtils.toHexString((byte[]) value);
    }
    return value.toString();
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDefaultNullValue(@Nullable Object defaultNullValue) {
    if (defaultNullValue != null) {
      _stringDefaultNullValue = getStringValue(defaultNullValue);
    }
    if (_dataType != null) {
      _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
    }
  }

  public static Object getDefaultNullValue(FieldType fieldType, DataType dataType,
      @Nullable String stringDefaultNullValue) {
    if (stringDefaultNullValue != null) {
      return dataType.convert(stringDefaultNullValue);
    } else {
      switch (fieldType) {
        case METRIC:
          switch (dataType) {
            case INT:
              return DEFAULT_METRIC_NULL_VALUE_OF_INT;
            case LONG:
              return DEFAULT_METRIC_NULL_VALUE_OF_LONG;
            case FLOAT:
              return DEFAULT_METRIC_NULL_VALUE_OF_FLOAT;
            case DOUBLE:
              return DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE;
            case BIG_DECIMAL:
              return DEFAULT_METRIC_NULL_VALUE_OF_BIG_DECIMAL;
            case STRING:
              return DEFAULT_METRIC_NULL_VALUE_OF_STRING;
            case BYTES:
              return DEFAULT_METRIC_NULL_VALUE_OF_BYTES;
            default:
              throw new IllegalStateException("Unsupported metric data type: " + dataType);
          }
        case DIMENSION:
        case TIME:
        case DATE_TIME:
          switch (dataType) {
            case INT:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
            case LONG:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_LONG;
            case FLOAT:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT;
            case DOUBLE:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE;
            case BOOLEAN:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN;
            case TIMESTAMP:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP;
            case STRING:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
            case JSON:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_JSON;
            case BYTES:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
            case BIG_DECIMAL:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL;
            default:
              throw new IllegalStateException("Unsupported dimension/time data type: " + dataType);
          }
        case COMPLEX:
          switch (dataType) {
            case MAP:
              return DEFAULT_COMPLEX_NULL_VALUE_OF_MAP;
            case LIST:
              return DEFAULT_COMPLEX_NULL_VALUE_OF_LIST;
            case STRUCT:
            default:
              throw new IllegalStateException("Unsupported complex data type: " + dataType);
          }
        default:
          throw new IllegalStateException("Unsupported field type: " + fieldType);
      }
    }
  }

  /**
   * Transform function if defined else null.
   * Deprecated. Use TableConfig -> IngestionConfig -> TransformConfigs
   */
  @Deprecated
  public String getTransformFunction() {
    return _transformFunction;
  }

  /**
   * Deprecated. Use TableConfig -> IngestionConfig -> TransformConfigs
   */
  // Required by JSON de-serializer. DO NOT REMOVE.
  @Deprecated
  public void setTransformFunction(@Nullable String transformFunction) {
    _transformFunction = transformFunction;
  }

  public String getVirtualColumnProvider() {
    return _virtualColumnProvider;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setVirtualColumnProvider(String virtualColumnProvider) {
    _virtualColumnProvider = virtualColumnProvider;
  }

  /**
   * Returns whether the column is virtual. Virtual columns are constructed while loading the segment, thus do not exist
   * in the record, nor should be persisted to the disk.
   * <p>Identify a column as virtual if the virtual column provider is configured.
   */
  @JsonIgnore
  public boolean isVirtualColumn() {
    return _virtualColumnProvider != null && !_virtualColumnProvider.isEmpty();
  }

  /**
   * Returns the {@link ObjectNode} representing the field spec.
   * <p>Only contains fields with non-default value.
   * <p>NOTE: here we use {@link ObjectNode} to preserve the insertion order.
   */
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = JsonUtils.newObjectNode();
    jsonObject.put("name", _name);
    jsonObject.put("dataType", _dataType.name());
    jsonObject.put("fieldType", getFieldType().toString());
    if (!_singleValueField) {
      jsonObject.put("singleValueField", false);
    }
    if (_notNull) {
      jsonObject.put("notNull", true);
    }
    if (_maxLength != null) {
      jsonObject.put("maxLength", _maxLength);
    }
    if (_maxLengthExceedStrategy != null) {
      jsonObject.put("maxLengthExceedStrategy", _maxLengthExceedStrategy.name());
    }
    if (_allowTrailingZeros) {
      jsonObject.put("allowTrailingZeros", true);
    }
    appendDefaultNullValue(jsonObject);
    appendTransformFunction(jsonObject);
    if (_virtualColumnProvider != null) {
      jsonObject.put("virtualColumnProvider", _virtualColumnProvider);
    }
    return jsonObject;
  }

  protected void appendDefaultNullValue(ObjectNode jsonNode) {
    assert _defaultNullValue != null;
    String key = "defaultNullValue";
    if (!_defaultNullValue.equals(getDefaultNullValue(getFieldType(), _dataType, null))) {
      switch (_dataType) {
        case INT:
          jsonNode.put(key, (Integer) _defaultNullValue);
          break;
        case LONG:
          jsonNode.put(key, (Long) _defaultNullValue);
          break;
        case FLOAT:
          jsonNode.put(key, (Float) _defaultNullValue);
          break;
        case DOUBLE:
          jsonNode.put(key, (Double) _defaultNullValue);
          break;
        case BIG_DECIMAL:
          jsonNode.put(key, (BigDecimal) _defaultNullValue);
          break;
        case BOOLEAN:
          jsonNode.put(key, (Integer) _defaultNullValue == 1);
          break;
        case TIMESTAMP:
          jsonNode.put(key, new Timestamp((Long) _defaultNullValue).toString());
          break;
        case STRING:
        case JSON:
          jsonNode.put(key, (String) _defaultNullValue);
          break;
        case BYTES:
          jsonNode.put(key, BytesUtils.toHexString((byte[]) _defaultNullValue));
          break;
        case MAP:
          jsonNode.put(key, JsonUtils.objectToJsonNode(_defaultNullValue));
          break;
        case LIST:
          jsonNode.put(key, JsonUtils.objectToJsonNode(_defaultNullValue));
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + this);
      }
    }
  }

  protected void appendTransformFunction(ObjectNode jsonNode) {
    if (_transformFunction != null) {
      jsonNode.put("transformFunction", _transformFunction);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldSpec that = (FieldSpec) o;
    return _name.equals(that._name)
        && _dataType == that._dataType
        && _singleValueField == that._singleValueField
        && _notNull == that._notNull
        && Objects.equals(_maxLength, that._maxLength)
        && Objects.equals(_maxLengthExceedStrategy, that._maxLengthExceedStrategy)
        && _allowTrailingZeros == that._allowTrailingZeros
        && getStringValue(_defaultNullValue).equals(getStringValue(that._defaultNullValue))
        && Objects.equals(_transformFunction, that._transformFunction)
        && Objects.equals(_virtualColumnProvider, that._virtualColumnProvider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _dataType, _singleValueField, _notNull, _maxLength, _maxLengthExceedStrategy,
        _allowTrailingZeros, getStringValue(_defaultNullValue), _transformFunction, _virtualColumnProvider);
  }

  /**
   * The <code>FieldType</code> enum is used to demonstrate the real world business logic for a column.
   * <p><code>DIMENSION</code>: columns used to filter records.
   * <p><code>METRIC</code>: columns used to apply aggregation on. <code>METRIC</code> field only contains numeric data.
   * <p><code>TIME</code>: time column (at most one per {@link Schema}). <code>TIME</code> field can be used to prune
   * <p><code>DATE_TIME</code>: time column (at most one per {@link Schema}). <code>TIME</code> field can be used to
   * prune
   * segments, otherwise treated the same as <code>DIMENSION</code> field.
   */
  public enum FieldType {
    DIMENSION, METRIC, TIME, DATE_TIME, COMPLEX
  }

  /**
   * The <code>DataType</code> enum is used to demonstrate the data type of a field.
   */
  @SuppressWarnings("rawtypes")
  public enum DataType {
    // LIST is for complex lists which is different from multi-value column of primitives
    // STRUCT, MAP and LIST are composable to form a COMPLEX field
    INT(Integer.BYTES, true, true),
    LONG(Long.BYTES, true, true),
    FLOAT(Float.BYTES, true, true),
    DOUBLE(Double.BYTES, true, true),
    BIG_DECIMAL(true, true),
    BOOLEAN(INT, false, true),
    TIMESTAMP(LONG, false, true),
    STRING(false, true),
    JSON(STRING, false, false),
    BYTES(false, false),
    STRUCT(false, false),
    MAP(false, false),
    LIST(false, false),
    UNKNOWN(false, true);

    private final DataType _storedType;
    private final int _size;
    private final boolean _sortable;
    private final boolean _numeric;

    DataType(boolean numeric, boolean sortable) {
      _storedType = this;
      _size = -1;
      _sortable = sortable;
      _numeric = numeric;
    }

    DataType(DataType storedType, boolean numeric, boolean sortable) {
      _storedType = storedType;
      _size = storedType._size;
      _sortable = sortable;
      _numeric = numeric;
    }

    DataType(int size, boolean numeric, boolean sortable) {
      _storedType = this;
      _size = size;
      _sortable = sortable;
      _numeric = numeric;
    }

    /**
     * Returns the data type stored in Pinot.
     * <p>Pinot internally stores data (physical) in INT, LONG, FLOAT, DOUBLE, STRING, BYTES type, other data types
     * (logical) will be stored as one of these types.
     * <p>Stored type should be used when reading the physical stored values from Dictionary, Forward Index etc.
     */
    public DataType getStoredType() {
      return _storedType;
    }

    /**
     * Returns {@code true} if the data type is of fixed width (INT, LONG, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP),
     * {@code false} otherwise.
     */
    public boolean isFixedWidth() {
      return _size >= 0;
    }

    /**
     * Returns the number of bytes needed to store the data type.
     */
    public int size() {
      if (_size >= 0) {
        return _size;
      }
      throw new IllegalStateException("Cannot get number of bytes for: " + this);
    }

    /**
     * Returns {@code true} if the data type is numeric (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL), {@code false}
     * otherwise.
     */
    public boolean isNumeric() {
      return _numeric;
    }

    /**
     * Returns {@code true} if the data type is unknown, {@code false} otherwise.
     */
    public boolean isUnknown() {
      return _storedType == UNKNOWN;
    }

    /**
     * Converts the given string value to the data type. Returns byte[] for BYTES.
     */
    public Object convert(String value) {
      try {
        switch (this) {
          case INT:
            return Integer.valueOf(value);
          case LONG:
            return Long.valueOf(value);
          case FLOAT:
            return Float.valueOf(value);
          case DOUBLE:
            return Double.valueOf(value);
          case BIG_DECIMAL:
            return new BigDecimal(value);
          case BOOLEAN:
            return BooleanUtils.toInt(value);
          case TIMESTAMP:
            return TimestampUtils.toMillisSinceEpoch(value);
          case STRING:
          case JSON:
            return value;
          case BYTES:
            return BytesUtils.toBytes(value);
          case MAP:
            return JsonUtils.stringToObject(value, Map.class);
          case LIST:
            return JsonUtils.stringToObject(value, List.class);
          default:
            throw new IllegalStateException();
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Cannot convert value: '" + value + "' to type: " + this);
      }
    }

    /**
     * Compares the given values of the data type.
     *
     * return 0 if the values are equal
     * return -1 if value1 is less than value2
     * return 1 if value1 is greater than value2
     */
    public int compare(Object value1, Object value2) {
      switch (this) {
        case INT:
          return Integer.compare((int) value1, (int) value2);
        case LONG:
          return Long.compare((long) value1, (long) value2);
        case FLOAT:
          return Float.compare((float) value1, (float) value2);
        case DOUBLE:
          return Double.compare((double) value1, (double) value2);
        case BIG_DECIMAL:
          return ((BigDecimal) value1).compareTo((BigDecimal) value2);
        case BOOLEAN:
          return Boolean.compare((boolean) value1, (boolean) value2);
        case TIMESTAMP:
          return Long.compare((long) value1, (long) value2);
        case STRING:
        case JSON:
          return ((String) value1).compareTo((String) value2);
        case BYTES:
          return ByteArray.compare((byte[]) value1, (byte[]) value2);
        case MAP:
        case LIST:
          throw new UnsupportedOperationException("Cannot compare complex data types: " + this);
        default:
          throw new IllegalStateException();
      }
    }

    /**
     * Converts the given value of the data type to string.The input value for BYTES type should be byte[].
     */
    public String toString(Object value) {
      if (this == BIG_DECIMAL) {
        return ((BigDecimal) value).toPlainString();
      }
      if (this == BYTES) {
        return BytesUtils.toHexString((byte[]) value);
      }
      if (this == MAP || this == LIST) {
        try {
          return JsonUtils.objectToString(value);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      return value.toString();
    }

    /**
     * Converts the given string value to the data type. Returns ByteArray for BYTES.
     */
    public Comparable convertInternal(String value) {
      try {
        switch (this) {
          case INT:
            return Integer.valueOf(value);
          case LONG:
            return Long.valueOf(value);
          case FLOAT:
            return Float.valueOf(value);
          case DOUBLE:
            return Double.valueOf(value);
          case BIG_DECIMAL:
            return new BigDecimal(value);
          case BOOLEAN:
            return BooleanUtils.toInt(value);
          case TIMESTAMP:
            return TimestampUtils.toMillisSinceEpoch(value);
          case STRING:
          case JSON:
            return value;
          case BYTES:
            return BytesUtils.toByteArray(value);
          case MAP:
          case LIST:
            throw new UnsupportedOperationException("Cannot convert complex data types: " + this);
          default:
            throw new IllegalStateException();
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Cannot convert value: '" + value + "' to type: " + this);
      }
    }

    /**
     * Checks whether the data type can be a sorted column.
     */
    public boolean canBeASortedColumn() {
      return _sortable;
    }
  }

  @Override
  public int compareTo(FieldSpec otherSpec) {
    // Sort fieldspecs based on their name
    return _name.compareTo(otherSpec._name);
  }

  /***
   * Return true if it is backward compatible with the old FieldSpec.
   * Backward compatibility requires
   * all other fields except DefaultNullValue and Max Length should be retained.
   *
   * @param oldFieldSpec
   * @return
   */
  public boolean isBackwardCompatibleWith(FieldSpec oldFieldSpec) {
    return EqualityUtils.isEqual(_name, oldFieldSpec._name)
        && EqualityUtils.isEqual(_dataType, oldFieldSpec._dataType)
        && EqualityUtils.isEqual(_singleValueField, oldFieldSpec._singleValueField);
  }

  public static class FieldSpecMetadata {
    @JsonProperty("fieldTypes")
    public Map<FieldType, FieldTypeMetadata> _fieldTypes = new HashMap<>();
    @JsonProperty("dataTypes")
    public Map<DataType, DataTypeProperties> _dataTypes = new HashMap<>();

    void put(FieldType type, FieldTypeMetadata metadata) {
      _fieldTypes.put(type, metadata);
    }

    void put(DataType type, DataTypeProperties metadata) {
      _dataTypes.put(type, metadata);
    }
  }

  public static class FieldTypeMetadata {
    @JsonProperty("allowedDataTypes")
    public Map<DataType, DataTypeMetadata> _allowedDataTypes = new HashMap<>();

    void put(DataType dataType, DataTypeMetadata metadata) {
      _allowedDataTypes.put(dataType, metadata);
    }
  }

  public static class DataTypeMetadata {
    @JsonProperty("nullDefault")
    public Object _nullDefault;

    public DataTypeMetadata(Object nullDefault) {
      _nullDefault = nullDefault;
    }
  }

  public static class DataTypeProperties {
    @JsonProperty("storedType")
    public final DataType _storedType;
    @JsonProperty("size")
    public final int _size;
    @JsonProperty("sortable")
    public final boolean _sortable;
    @JsonProperty("numeric")
    public final boolean _numeric;

    public DataTypeProperties(DataType dataType) {
      _storedType = dataType._storedType;
      _sortable = dataType._sortable;
      _numeric = dataType._numeric;
      _size = dataType._size;
    }
  }
}
