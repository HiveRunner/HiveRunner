package com.klarna.hiverunner.data;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.google.common.collect.ImmutableList;

/**
 * A fluent builder class for creating a list of records to be inserted into a table.
 */
public abstract class TableDataBuilder<T> {

  private final HCatSchema schema;
  protected final TableDataInserter inserter;

  protected HCatRecord record;
  private List<String> names;

  public static TableDataBuilder<?> create(HCatTable table, HiveConf conf) {
    TableDataInserter inserter = new TableDataInserter(table.getDbName(), table.getTableName(), conf);
    if (table.getPartCols().size() > 0) {
      return new PartitionedTableDataBuilder(table, inserter);
    }
    return new UnpartitionedTableDataBuilder(table, inserter);
  }

  TableDataBuilder(HCatTable table, TableDataInserter inserter) {
    schema = new HCatSchema(ImmutableList
        .<HCatFieldSchema> builder()
        .addAll(table.getCols())
        .addAll(table.getPartCols())
        .build());
    this.inserter = inserter;
    withAllColumns();
  }

  public TableDataBuilder<T> withColumns(String... names) {
    this.names = new ArrayList<>();
    for (String name : names) {
      checkColumn(name);
      this.names.add(name);
    }
    return this;
  }

  public TableDataBuilder<T> withAllColumns() {
    names = schema.getFieldNames();
    return this;
  }

  public TableDataBuilder<T> newRow() {
    flushRow();
    record = new DefaultHCatRecord(schema.size());
    return this;
  }

  public TableDataBuilder<T> addRow(Object... values) {
    newRow();
    checkArgument(values.length == names.size(), "Expected %d values, got %d", names.size(), values.length);
    for (int i = 0; i < values.length; i++) {
      set(names.get(i), values[i]);
    }
    return this;
  }

  public TableDataBuilder<T> copyRow() {
    checkState(record != null, "No row to copy.");
    HCatRecord copy = new DefaultHCatRecord(new ArrayList<>(record.getAll()));
    flushRow();
    record = copy;
    return this;
  }

  public TableDataBuilder<T> set(String name, Object value) {
    checkColumn(name);
    try {
      record.set(name, schema, value);
    } catch (HCatException e) {
      throw new RuntimeException(e); // should never happen
    }
    return this;
  }

  protected Object get(String name) {
    checkColumn(name);
    try {
      return record.get(name, schema);
    } catch (HCatException e) {
      throw new RuntimeException(e); // should never happen
    }
  }

  protected abstract void flushRow();

  protected abstract T build();

  public abstract void commit();

  private void checkColumn(String name) {
    checkArgument(schema.getFieldNames().contains(name), "Column %s does not exist", name);
  }

}
