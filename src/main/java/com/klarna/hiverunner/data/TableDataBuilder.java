package com.klarna.hiverunner.data;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.ConversionException;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.Multimap;

class TableDataBuilder {

  private final Builder<Map<String, String>, HCatRecord> rowsBuilder = ImmutableMultimap.builder();
  private final HCatSchema schema;
  private final List<HCatFieldSchema> partitionColumns;

  private HCatRecord row;
  private List<String> names;

  TableDataBuilder(HCatTable table) {
    schema = new HCatSchema(ImmutableList
        .<HCatFieldSchema> builder()
        .addAll(table.getCols())
        .addAll(table.getPartCols())
        .build());
    partitionColumns = table.getPartCols();
    withAllColumns();
  }

  TableDataBuilder withColumns(String... names) {
    checkArgument(checkNotNull(names).length > 0, "Column names must be provided.");
    this.names = new ArrayList<>(names.length);
    for (String name : names) {
      checkColumn(name);
      this.names.add(name);
    }
    return this;
  }

  TableDataBuilder withAllColumns() {
    names = schema.getFieldNames();
    return this;
  }

  TableDataBuilder newRow() {
    flushRow();
    row = new DefaultHCatRecord(schema.size());
    return this;
  }

  TableDataBuilder addRow(Object... values) {
    return newRow().setRow(values);
  }

  TableDataBuilder setRow(Object... values) {
    checkArgument(values.length == names.size(), "Expected %d values, got %d", names.size(), values.length);
    for (int i = 0; i < values.length; i++) {
      set(names.get(i), values[i]);
    }
    return this;
  }

  TableDataBuilder addRowsFromTsv(File file) {
    return addRowsFrom(file, new TsvFileParser());
  }

  TableDataBuilder addRowsFromDelimited(File file, String delimiter, Object nullValue) {
    return addRowsFrom(file, new TsvFileParser().withDelimiter(delimiter).withNullValue(nullValue));
  }

  TableDataBuilder addRowsFrom(File file, FileParser fileParser) {
    if (fileParser.hasColumnNames()) {
      checkArgument(names.equals(schema.getFieldNames()),
          "Manual column spec and header column spec are mutually exclusive");
      String[] columns = FluentIterable
          .from(fileParser.getColumnNames(file))
          .transform(toLowerCase())
          .toArray(String.class);
      withColumns(columns);
    }
    return addRows(fileParser.parse(file, schema, names));
  }

  private Function<String, String> toLowerCase() {
    return new Function<String, String>() {
      @Override
      public String apply(String t) {
        return t.toLowerCase();
      }
    };
  }

  private TableDataBuilder addRows(List<Object[]> rows) {
    for (Object[] row : rows) {
      addRow(row);
    }
    return this;
  }

  TableDataBuilder copyRow() {
    checkState(row != null, "No previous row to copy.");
    HCatRecord copy = new DefaultHCatRecord(new ArrayList<>(row.getAll()));
    flushRow();
    row = copy;
    return this;
  }

  TableDataBuilder set(String name, Object value) {
    checkColumn(name);
    PrimitiveTypeInfo typeInfo;
    try {
      typeInfo = schema.get(name).getTypeInfo();
    } catch (HCatException e) {
      throw new IllegalArgumentException("Error getting type info for " + name, e);
    }
    Object converted;
    try {
      converted = Converters.convert(value, typeInfo);
    } catch (ConversionException e) {
      throw new IllegalArgumentException("Invalid value for " + name + ". Got '" + value + "' ("
          + value.getClass().getSimpleName() + "). Expected " + typeInfo.getTypeName() + ".", e);
    }
    try {
      row.set(name, schema, converted);
    } catch (HCatException e) {
      throw new RuntimeException("Error setting value for " + name, e);
    }
    return this;
  }

  private Object get(String name) {
    checkColumn(name);
    try {
      return row.get(name, schema);
    } catch (HCatException e) {
      throw new RuntimeException("Error getting value for " + name, e);
    }
  }

  private void flushRow() {
    if (row != null) {
      rowsBuilder.put(createPartitionSpec(), row);
    }
  }

  private Map<String, String> createPartitionSpec() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (HCatFieldSchema partitionColumn : partitionColumns) {
      String name = partitionColumn.getName();
      Object value = get(name);
      checkState(value != null, "Value for partition column %s must not be null.", name);
      builder.put(name, value.toString());
    }
    return builder.build();
  }

  Multimap<Map<String, String>, HCatRecord> build() {
    flushRow();
    return rowsBuilder.build();
  }

  private void checkColumn(String name) {
    checkArgument(schema.getFieldNames().contains(name.toLowerCase()), "Column %s does not exist", name);
  }

}
