package com.klarna.hiverunner.data;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.charTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.decimalTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.unknownTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.varcharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.voidTypeInfo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.junit.Test;

public class ConvertersTest {

  @Test
  public void inputNull() {
    for (PrimitiveTypeInfo typeInfo : Converters.TYPES.keySet()) {
      assertNull(Converters.convert(null, typeInfo));
    }
  }

  @Test
  public void inputNotString() {
    for (PrimitiveTypeInfo typeInfo : Converters.TYPES.keySet()) {
      assertEquals(0, Converters.convert(0, typeInfo));
    }
  }

  @Test
  public void inputString() {
    assertEquals("foo", Converters.convert("foo", stringTypeInfo));
    assertEquals(true, Converters.convert("true", booleanTypeInfo));
    assertEquals((byte) 7, Converters.convert("7", byteTypeInfo));
    assertEquals((short) 7, Converters.convert("7", shortTypeInfo));
    assertEquals(7, Converters.convert("7", intTypeInfo));
    assertEquals(7L, Converters.convert("7", longTypeInfo));
    assertEquals(7.0F, Converters.convert("7", floatTypeInfo));
    assertEquals(7.0D, Converters.convert("7", doubleTypeInfo));
    assertEquals(Date.valueOf("2015-10-15"), Converters.convert("2015-10-15", dateTypeInfo));
    assertEquals(Timestamp.valueOf("2015-10-15 23:59:59.999"),
        Converters.convert("2015-10-15 23:59:59.999", timestampTypeInfo));
    assertEquals(new BigDecimal("1.234"), Converters.convert("1.234", decimalTypeInfo));
    assertArrayEquals(new byte[] { 0, 1, 2 }, (byte[]) Converters.convert("0,1,2", binaryTypeInfo));
    assertEquals("foo", Converters.convert("foo", charTypeInfo));
    assertEquals("foo", Converters.convert("foo", varcharTypeInfo));
    assertEquals("foo", Converters.convert("foo", unknownTypeInfo));
    assertEquals("foo", Converters.convert("foo", voidTypeInfo));
  }

}
