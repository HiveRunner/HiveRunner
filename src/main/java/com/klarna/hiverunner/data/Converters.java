package com.klarna.hiverunner.data;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.converters.BooleanConverter;
import org.apache.commons.beanutils.converters.ByteArrayConverter;
import org.apache.commons.beanutils.converters.ByteConverter;
import org.apache.commons.beanutils.converters.DoubleConverter;
import org.apache.commons.beanutils.converters.FloatConverter;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.LongConverter;
import org.apache.commons.beanutils.converters.ShortConverter;
import org.apache.commons.beanutils.converters.SqlDateConverter;
import org.apache.commons.beanutils.converters.SqlTimestampConverter;
import org.apache.commons.beanutils.converters.StringConverter;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import com.google.common.collect.ImmutableMap;

/**
 * A utility class for converting from {@link String Strings} into the target Hive table's column type.
 */
public final class Converters {

  static final Map<PrimitiveTypeInfo, Class<?>> TYPES = ImmutableMap
      .<PrimitiveTypeInfo, Class<?>> builder()
      .put(stringTypeInfo, String.class)
      .put(booleanTypeInfo, Boolean.class)
      .put(byteTypeInfo, Byte.class)
      .put(shortTypeInfo, Short.class)
      .put(intTypeInfo, Integer.class)
      .put(longTypeInfo, Long.class)
      .put(floatTypeInfo, Float.class)
      .put(doubleTypeInfo, Double.class)
      .put(dateTypeInfo, Date.class)
      .put(timestampTypeInfo, Timestamp.class)
      .put(binaryTypeInfo, Byte[].class)
      .build();

  private static final ConvertUtilsBean CONVERTER;

  static {
    CONVERTER = new ConvertUtilsBean();
    CONVERTER.register(new StringConverter(), String.class);
    CONVERTER.register(new BooleanConverter(), Boolean.class);
    CONVERTER.register(new ByteConverter(), Byte.class);
    CONVERTER.register(new ShortConverter(), Short.class);
    CONVERTER.register(new IntegerConverter(), Integer.class);
    CONVERTER.register(new LongConverter(), Long.class);
    CONVERTER.register(new FloatConverter(), Float.class);
    CONVERTER.register(new DoubleConverter(), Double.class);
    CONVERTER.register(new SqlDateConverter(), Date.class);
    CONVERTER.register(new SqlTimestampConverter(), Timestamp.class);
    CONVERTER.register(new ByteArrayConverter(), Byte[].class);
    CONVERTER.register(new HiveDecimalConverter(), HiveDecimal.class);
    CONVERTER.register(new HiveVarcharConverter(), HiveVarchar.class);
    CONVERTER.register(new HiveCharConverter(), HiveChar.class);
  }

  private Converters() {
  }

  static Class<?> type(PrimitiveTypeInfo typeInfo) {
    Class<?> type = TYPES.get(typeInfo);
    if (type == null) {
      if (typeInfo instanceof DecimalTypeInfo) {
        type = HiveDecimal.class;
      } else if (typeInfo instanceof VarcharTypeInfo) {
        type = HiveVarchar.class;
      } else if (typeInfo instanceof CharTypeInfo) {
        type = HiveChar.class;
      } else {
        type = String.class;
      }
    }
    return type;
  }

  /**
   * Attempts to convert the input value into the target type. If the input value is {@code null} then {@code null} is
   * returned. If the input value is a String then an attempt is made to convert it into the target type. If the input
   * value is not a {@link String} then it is assumed the user has explicitly chosen the required type and no attempt is
   * made to perform a conversion. This may result in Hive throwing an error if the incorrect type was chosen.
   *
   * @param value The input value.
   * @param typeInfo The target Table's column type.
   */
  public static Object convert(Object value, PrimitiveTypeInfo typeInfo) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return CONVERTER.convert((String) value, type(typeInfo));
    }
    return value;
  }

  private static class HiveDecimalConverter implements Converter {
    @Override
    public Object convert(@SuppressWarnings("rawtypes") Class type, Object value) {
      try {
        return HiveDecimal.create(new BigDecimal(value.toString()));
      } catch (NumberFormatException e) {
        throw new ConversionException(e);
      }
    }
  }

  private static class HiveVarcharConverter implements Converter {
    @Override
    public Object convert(@SuppressWarnings("rawtypes") Class type, Object value) {
      return new HiveVarchar(value.toString(), -1);
    }
  }

  private static class HiveCharConverter implements Converter {
    @Override
    public Object convert(@SuppressWarnings("rawtypes") Class type, Object value) {
      return new HiveChar(value.toString(), -1);
    }
  }

}
