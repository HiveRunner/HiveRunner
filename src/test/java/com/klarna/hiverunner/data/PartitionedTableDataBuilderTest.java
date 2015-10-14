package com.klarna.hiverunner.data;

import static org.junit.Assert.assertEquals;

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

import com.google.common.collect.Multimap;

public class PartitionedTableDataBuilderTest {

  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String COLUMN_1 = "column_1";
  private static final String PARTITION_COLUMN_1 = "partition_column_1";

  private static final PrimitiveTypeInfo STRING = TypeInfoFactory.stringTypeInfo;

  private final TableDataInserter inserter = null;

  @Test(expected = IllegalStateException.class)
  public void testPartitionedNullPartitionColumnValue() {
    HCatTable table = table().cols(columns(COLUMN_1)).partCols(columns(PARTITION_COLUMN_1));

    new PartitionedTableDataBuilder(table, inserter).newRow().build();
  }

  @Test
  public void testPartitionedSimple() {
    HCatTable table = table().cols(columns(COLUMN_1)).partCols(columns(PARTITION_COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> partitions = new PartitionedTableDataBuilder(table, inserter).addRow(
        "value", "partition_value").build();

    assertEquals(1, partitions.size());

    Map<String, String> partitionSpec = new HashMap<>();
    partitionSpec.put(PARTITION_COLUMN_1, "partition_value");

    Collection<HCatRecord> records = partitions.get(partitionSpec);
    assertEquals(1, records.size());
    HCatRecord record = records.iterator().next();
    assertEquals(Arrays.asList((Object) "value", "partition_value"), record.getAll());
  }

  @Test
  public void testPartitionedMultiplePartitionsAndRows() {
    HCatTable table = table().cols(columns(COLUMN_1)).partCols(columns(PARTITION_COLUMN_1));

    Multimap<Map<String, String>, HCatRecord> partitions = new PartitionedTableDataBuilder(table, inserter)
        .addRow("value1", "partition_value1")
        .addRow("value2", "partition_value1")
        .addRow("value3", "partition_value2")
        .addRow("value4", "partition_value2")
        .build();

    assertEquals(2, partitions.size());

    Map<String, String> partitionSpec = new HashMap<>();
    partitionSpec.put(PARTITION_COLUMN_1, "partition_value1");

    Collection<HCatRecord> records = partitions.get(partitionSpec);
    assertEquals(2, records.size());
    Iterator<HCatRecord> iterator = records.iterator();
    HCatRecord record = iterator.next();
    assertEquals(Arrays.asList((Object) "value1", "partition_value1"), record.getAll());
    record = iterator.next();
    assertEquals(Arrays.asList((Object) "value2", "partition_value1"), record.getAll());

    partitionSpec = new HashMap<>();
    partitionSpec.put(PARTITION_COLUMN_1, "partition_value2");

    records = partitions.get(partitionSpec);
    assertEquals(2, records.size());
    iterator = records.iterator();
    record = iterator.next();
    assertEquals(Arrays.asList((Object) "value3", "partition_value2"), record.getAll());
    record = iterator.next();
    assertEquals(Arrays.asList((Object) "value4", "partition_value2"), record.getAll());
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
