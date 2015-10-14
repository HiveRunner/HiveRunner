package com.klarna.hiverunner.data;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.decimalTypeInfo;
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

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import com.google.common.collect.ImmutableMap;

final class Converters {

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
      .put(decimalTypeInfo, BigDecimal.class)
      .put(binaryTypeInfo, byte[].class)
      .build();

  private Converters() {
  }

  static Class<?> type(PrimitiveTypeInfo typeInfo) {
    Class<?> type = TYPES.get(typeInfo);
    if (type == null) {
      return String.class;
    }
    return type;
  }

  static Object convert(Object value, PrimitiveTypeInfo typeInfo) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return ConvertUtils.convert((String) value, type(typeInfo));
    }
    return value;
  }

}
