package com.klarna.hiverunner.data;

import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.Multimap;

class PartitionedTableDataBuilder extends TableDataBuilder<Multimap<Map<String, String>, HCatRecord>> {

  private final List<HCatFieldSchema> partitionColumns;

  private final Builder<Map<String, String>, HCatRecord> partitionsBuilder = ImmutableMultimap.builder();

  PartitionedTableDataBuilder(HCatTable table, TableDataInserter inserter) {
    super(table, inserter);
    partitionColumns = table.getPartCols();
  }

  @Override
  protected void flushRow() {
    if (record != null) {
      partitionsBuilder.put(createPartitionSpec(), record);
    }
  }

  @Override
  protected Multimap<Map<String, String>, HCatRecord> build() {
    flushRow();
    Multimap<Map<String, String>, HCatRecord> partitions = partitionsBuilder.build();
    checkState(partitions.size() > 0, "No partitions.");
    return partitions;
  }

  @Override
  public void commit() {
    Multimap<Map<String, String>, HCatRecord> partitions = build();
    for (Map<String, String> partitionSpec : partitions.keySet()) {
      inserter.insert(partitionSpec, partitions.get(partitionSpec));
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

}
