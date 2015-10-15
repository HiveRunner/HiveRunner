package com.klarna.hiverunner.data;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.junit.Test;

public class UnpartitionedTableDataBuilderTest {

  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String COLUMN_1 = "column_1";

  private static final PrimitiveTypeInfo STRING = TypeInfoFactory.stringTypeInfo;

  private final TableDataInserter inserter = null;

  @Test(expected = IllegalStateException.class)
  public void testNoData() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new UnpartitionedTableDataBuilder(table, inserter).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownColumnNameWithColumnMask() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new UnpartitionedTableDataBuilder(table, inserter).withColumns("unknown_column");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownColumnNameOnSet() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new UnpartitionedTableDataBuilder(table, inserter).set("unknown_column", "value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddRowWithNoArguments() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new UnpartitionedTableDataBuilder(table, inserter).addRow();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddRowWithIncorrectNumberOfArguments() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new UnpartitionedTableDataBuilder(table, inserter).addRow("value1", "value2");
  }

  @Test(expected = IllegalStateException.class)
  public void testCopyRowWhenNoRowToCopy() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new UnpartitionedTableDataBuilder(table, inserter).copyRow();
  }

  @Test
  public void testCopyRow() {
    HCatTable table = table().cols(columns(COLUMN_1));

    List<HCatRecord> records = new UnpartitionedTableDataBuilder(table, inserter).addRow("value").copyRow().build();

    assertEquals(2, records.size());
    HCatRecord record = records.get(0);
    assertEquals(Arrays.asList((Object) "value"), record.getAll());
    record = records.get(1);
    assertEquals(Arrays.asList((Object) "value"), record.getAll());
  }

  @Test
  public void testUnpartitionedEmptyRow() {
    HCatTable table = table().cols(columns(COLUMN_1));

    List<HCatRecord> records = new UnpartitionedTableDataBuilder(table, inserter).newRow().build();

    assertEquals(1, records.size());
    HCatRecord record = records.get(0);
    assertEquals(Arrays.asList((Object) null), record.getAll());
  }

  @Test
  public void testUnpartitionedWithColumnMask() {
    HCatTable table = table().cols(columns(COLUMN_1));

    List<HCatRecord> records = new UnpartitionedTableDataBuilder(table, inserter)
        .withColumns(COLUMN_1)
        .addRow("value")
        .build();

    assertEquals(1, records.size());
    HCatRecord record = records.get(0);
    assertEquals(Arrays.asList((Object) "value"), record.getAll());
  }

  private static HCatTable table() {
    return new HCatTable(DATABASE_NAME, TABLE_NAME);
  }

  private static HCatFieldSchema column(String name) {
    try {
      return new HCatFieldSchema(name, STRING, null);
    } catch (HCatException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<HCatFieldSchema> columns(String... names) {
    List<HCatFieldSchema> columns = new ArrayList<>();
    for (String name : names) {
      columns.add(column(name));
    }
    return columns;
  }

}
