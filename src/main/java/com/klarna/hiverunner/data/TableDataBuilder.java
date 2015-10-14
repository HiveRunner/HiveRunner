package com.klarna.hiverunner.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.google.common.annotations.VisibleForTesting;

public class TableDataBuilder {

  static final String UNPARTITIONED = "UNPARTITIONED";

  private final HCatSchema schema;
  private final List<HCatFieldSchema> partitionColumns;
  private final TableDataInserterFactory tableDataInserterFactory;

  private final Map<String, List<HCatRecord>> partitions = new HashMap<>();

  private HCatRecord record;
  private List<String> names;

  public TableDataBuilder(HCatTable table, TableDataInserterFactory tableDataInserterFactory) {
    schema = createSchema(table);
    partitionColumns = table.getPartCols();
    this.tableDataInserterFactory = tableDataInserterFactory;
    withAllColumns();
  }

  private static HCatSchema createSchema(HCatTable table) {
    List<HCatFieldSchema> allColumns = new ArrayList<>();
    allColumns.addAll(table.getCols());
    allColumns.addAll(table.getPartCols());
    return new HCatSchema(allColumns);
  }

  public TableDataBuilder withColumns(String... names) {
    this.names = new ArrayList<>();
    for (String name : names) {
      if (!schema.getFieldNames().contains(name)) {
        throw new IllegalArgumentException("Column " + name + " does not exist");
      }
      this.names.add(name);
    }
    return this;
  }

  public TableDataBuilder withAllColumns() {
    names = schema.getFieldNames();
    return this;
  }

  public TableDataBuilder newRow() {
    flushRow();
    record = new DefaultHCatRecord(schema.size());
    return this;
  }

  public TableDataBuilder addRow(Object... values) {
    newRow();
    if (values.length != names.size()) {
      throw new IllegalArgumentException("Expected " + names.size() + " values, got " + values.length);
    }
    for (int i = 0; i < values.length; i++) {
      set(names.get(i), values[i]);
    }
    return this;
  }

  public TableDataBuilder copyRow(Object... values) {
    HCatRecord copy = new DefaultHCatRecord(new ArrayList<>(record.getAll()));
    flushRow();
    record = copy;
    return this;
  }

  public TableDataBuilder set(String name, Object value) {
    try {
      record.set(name, schema, value);
    } catch (HCatException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  TableData build() {
    flushRow();

    List<HCatRecord> records = partitions.get(UNPARTITIONED);
    if (records != null) {
      return new TableData(records);
    }
    Map<Map<String, String>, List<HCatRecord>> partitionRecords = new HashMap<>();
    for (Entry<String, List<HCatRecord>> partition : partitions.entrySet()) {
      partitionRecords.put(getPartitionSpec(partition.getKey()), partition.getValue());
    }
    return new TableData(partitionRecords);
  }

  public void commit() {
    TableData tableData = build();
    tableData.commit(tableDataInserterFactory.newInstance());
  }

  private void flushRow() {
    if (record != null) {
      String partitionSpec = getPartitionName();
      List<HCatRecord> partitionRecords = partitions.get(partitionSpec);
      if (partitionRecords == null) {
        partitionRecords = new ArrayList<>();
        partitions.put(partitionSpec, partitionRecords);
      }
      partitionRecords.add(record);
    }
  }

  private String getPartitionName() {
    if (partitionColumns.size() == 0) {
      return UNPARTITIONED;
    }

    Map<String, String> partitionSpec = new HashMap<>();
    for (HCatFieldSchema partitionColumn : partitionColumns) {
      String name = partitionColumn.getName();
      Object value;
      try {
        value = record.get(name, schema);
      } catch (HCatException e) {
        throw new RuntimeException(e);
      }
      if (value == null) {
        throw new IllegalStateException("Partition value for column '" + name + "' must not be null.");
      }
      partitionSpec.put(name, value.toString());
    }
    return getPartitionName(partitionSpec);
  }

  static String getPartitionName(Map<String, String> partitionSpec) {
    try {
      return Warehouse.makePartName(partitionSpec, false);
    } catch (MetaException e) {
      throw new IllegalStateException("Unable to create partition name", e);
    }
  }

  static Map<String, String> getPartitionSpec(String partitionName) {
    try {
      return Warehouse.makeSpecFromName(partitionName);
    } catch (MetaException e) {
      throw new IllegalStateException("Unable to create partition spec", e);
    }
  }

}
