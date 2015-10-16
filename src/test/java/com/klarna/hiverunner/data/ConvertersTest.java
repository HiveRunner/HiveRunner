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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.commons.beanutils.ConversionException;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
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
  public void stringTypeInfo() {
    assertEquals("foo", Converters.convert("foo", stringTypeInfo));
  }

  @Test
  public void booleanTypeInfo() {
    assertConversionException("foo", booleanTypeInfo);
    assertEquals(true, Converters.convert("true", booleanTypeInfo));
    assertEquals(false, Converters.convert("false", booleanTypeInfo));
  }

  @Test
  public void byteTypeInfo() {
    assertConversionException("foo", byteTypeInfo);
    assertConversionException("-129", byteTypeInfo);
    assertEquals((byte) -128, Converters.convert("-128", byteTypeInfo));
    assertEquals((byte) 127, Converters.convert("127", byteTypeInfo));
    assertConversionException("128", byteTypeInfo);
  }

  @Test
  public void shortTypeInfo() {
    assertConversionException("foo", shortTypeInfo);
    assertConversionException("-32769", shortTypeInfo);
    assertEquals((short) -32768, Converters.convert("-32768", shortTypeInfo));
    assertEquals((short) 32767, Converters.convert("32767", shortTypeInfo));
    assertConversionException("32768", shortTypeInfo);
  }

  @Test
  public void intTypeInfo() {
    assertConversionException("foo", intTypeInfo);
    assertConversionException("-2147483649", intTypeInfo);
    assertEquals(-2147483648, Converters.convert("-2147483648", intTypeInfo));
    assertEquals(2147483647, Converters.convert("2147483647", intTypeInfo));
    assertConversionException("2147483648", intTypeInfo);
  }

  @Test
  public void longTypeInfo() {
    assertConversionException("foo", longTypeInfo);
    assertConversionException("-9223372036854775809", longTypeInfo);
    assertEquals(-9223372036854775808L, Converters.convert("-9223372036854775808", longTypeInfo));
    assertEquals(9223372036854775807L, Converters.convert("9223372036854775807", longTypeInfo));
    assertConversionException("9223372036854775808", longTypeInfo);
  }

  @Test
  public void floatTypeInfo() {
    assertConversionException("foo", floatTypeInfo);
    assertEquals(0F, Converters.convert("0", floatTypeInfo));
  }

  @Test
  public void doubleTypeInfo() {
    assertConversionException("foo", doubleTypeInfo);
    assertEquals(0D, Converters.convert("0", doubleTypeInfo));
  }

  @Test
  public void dateTypeInfo() {
    assertConversionException("foo", dateTypeInfo);
    assertEquals(Date.valueOf("2015-10-15"), Converters.convert("2015-10-15", dateTypeInfo));
  }

  @Test
  public void timestampTypeInfo() {
    assertConversionException("foo", timestampTypeInfo);
    assertEquals(Timestamp.valueOf("2015-10-15 23:59:59.999"),
        Converters.convert("2015-10-15 23:59:59.999", timestampTypeInfo));
  }

  @Test
  public void binaryTypeInfo() {
    assertConversionException("foo", binaryTypeInfo);
    assertArrayEquals(new byte[] { 0, 1, 2 }, (byte[]) Converters.convert("0,1,2", binaryTypeInfo));
  }

  @Test
  public void otherTypeInfo() {
    assertEquals(HiveDecimal.create("1.234"), Converters.convert("1.234", decimalTypeInfo));
    assertEquals(new HiveChar("foo", -1), Converters.convert("foo", charTypeInfo));
    assertTrue(new HiveVarchar("foo", -1).equals((HiveVarchar) Converters.convert("foo", varcharTypeInfo)));
    assertEquals("foo", Converters.convert("foo", unknownTypeInfo));
    assertEquals("foo", Converters.convert("foo", voidTypeInfo));
  }

  private void assertConversionException(Object value, PrimitiveTypeInfo typeInfo) {
    try {
      System.out.println(Converters.convert(value, typeInfo));
    } catch (ConversionException e) {
      return;
    }
    fail("Expected " + ConversionException.class.getSimpleName() + " for value " + value + " ("
        + value.getClass().getSimpleName() + ") to " + typeInfo.getTypeName());
  }
}
