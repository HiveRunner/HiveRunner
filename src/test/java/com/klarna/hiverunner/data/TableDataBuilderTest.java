package com.klarna.hiverunner.data;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Multimap;

@RunWith(MockitoJUnitRunner.class)
public class TableDataBuilderTest {

  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String COLUMN_1 = "column_1";
  private static final String PARTITION_COLUMN_1 = "partition_column_1";

  private static final PrimitiveTypeInfo STRING = TypeInfoFactory.stringTypeInfo;

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownColumnNameWithColumnMask() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new TableDataBuilder(table).withColumns("unknown_column");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownColumnNameOnSet() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new TableDataBuilder(table).set("unknown_column", "value");
  }

  @Mock
  private TsvFileParser tsvFileParser;

  @Test
  public void testAddRowsFromWithMixedCaseColumnNames() {
    File file = new File("");
    HCatTable table = table().cols(columns("COLUMN_1", "coLUMN_2", "column_3"));
    TableDataBuilder tableDataBuilder = Mockito.spy(new TableDataBuilder(table));

    when(tsvFileParser.hasColumnNames()).thenReturn(true);
    when(tsvFileParser.getColumnNames(file)).thenReturn(Arrays.asList("COLUMN_1", "coLUMN_2", "column_3"));

    tableDataBuilder.addRowsFrom(file, tsvFileParser);
    verify(tableDataBuilder, times(1)).withColumns("column_1", "column_2", "column_3");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddRowWithNoArguments() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new TableDataBuilder(table).addRow();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddRowWithIncorrectNumberOfArguments() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new TableDataBuilder(table).addRow("value1", "value2");
  }

  @Test(expected = IllegalStateException.class)
  public void testCopyRowWhenNoRowToCopy() {
    HCatTable table = table().cols(columns(COLUMN_1));

    new TableDataBuilder(table).copyRow();
  }

  @Test
  public void testCopyRow() {
    HCatTable table = table().cols(columns(COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> data = new TableDataBuilder(table).addRow("value").copyRow().build();

    assertEquals(2, data.size());
    Iterator<HCatRecord> iterator = data.values().iterator();
    HCatRecord row = iterator.next();
    assertEquals(Arrays.asList((Object) "value"), row.getAll());
    row = iterator.next();
    assertEquals(Arrays.asList((Object) "value"), row.getAll());
  }

  @Test
  public void testUnpartitionedEmptyRow() {
    HCatTable table = table().cols(columns(COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> data = new TableDataBuilder(table).newRow().build();

    assertEquals(1, data.size());
    Iterator<HCatRecord> iterator = data.values().iterator();
    HCatRecord row = iterator.next();
    assertEquals(Arrays.asList((Object) null), row.getAll());
  }

  @Test
  public void testUnpartitionedWithColumnMask() {
    HCatTable table = table().cols(columns(COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> data = new TableDataBuilder(table)
        .withColumns(COLUMN_1)
        .addRow("value")
        .build();

    assertEquals(1, data.size());
    Iterator<HCatRecord> iterator = data.values().iterator();
    HCatRecord row = iterator.next();
    assertEquals(Arrays.asList((Object) "value"), row.getAll());
  }

  @Test(expected = IllegalStateException.class)
  public void testPartitionedNullPartitionColumnValue() {
    HCatTable table = table().cols(columns(COLUMN_1)).partCols(columns(PARTITION_COLUMN_1));

    new TableDataBuilder(table).newRow().build();
  }

  @Test
  public void testPartitionedSimple() {
    HCatTable table = table().cols(columns(COLUMN_1)).partCols(columns(PARTITION_COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> data = new TableDataBuilder(table)
        .addRow("value", "partition_value")
        .build();

    assertEquals(1, data.size());

    Map<String, String> partitionSpec = new HashMap<>();
    partitionSpec.put(PARTITION_COLUMN_1, "partition_value");

    Collection<HCatRecord> rows = data.get(partitionSpec);
    assertEquals(1, rows.size());
    HCatRecord row = rows.iterator().next();
    assertEquals(Arrays.asList((Object) "value", "partition_value"), row.getAll());
  }

  @Test
  public void testPartitionedMultiplePartitionsAndRows() {
    HCatTable table = table().cols(columns(COLUMN_1)).partCols(columns(PARTITION_COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> data = new TableDataBuilder(table)
        .addRow("value1", "partition_value1")
        .addRow("value2", "partition_value1")
        .addRow("value3", "partition_value2")
        .addRow("value4", "partition_value2")
        .build();

    assertEquals(4, data.size());

    Map<String, String> partitionSpec = new HashMap<>();
    partitionSpec.put(PARTITION_COLUMN_1, "partition_value1");

    Collection<HCatRecord> rows = data.get(partitionSpec);
    assertEquals(2, rows.size());
    Iterator<HCatRecord> iterator = rows.iterator();
    HCatRecord row = iterator.next();
    assertEquals(Arrays.asList((Object) "value1", "partition_value1"), row.getAll());
    row = iterator.next();
    assertEquals(Arrays.asList((Object) "value2", "partition_value1"), row.getAll());

    partitionSpec = new HashMap<>();
    partitionSpec.put(PARTITION_COLUMN_1, "partition_value2");

    rows = data.get(partitionSpec);
    assertEquals(2, rows.size());
    iterator = rows.iterator();
    row = iterator.next();
    assertEquals(Arrays.asList((Object) "value3", "partition_value2"), row.getAll());
    row = iterator.next();
    assertEquals(Arrays.asList((Object) "value4", "partition_value2"), row.getAll());
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
