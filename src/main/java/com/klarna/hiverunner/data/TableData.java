package com.klarna.hiverunner.data;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hive.hcatalog.data.HCatRecord;

public class TableData {

  private final List<HCatRecord> records;
  private final Map<Map<String, String>, List<HCatRecord>> partitions;

  private TableData(List<HCatRecord> records, Map<Map<String, String>, List<HCatRecord>> partitions) {
    this.records = records;
    this.partitions = partitions;
  }

  public TableData(List<HCatRecord> records) {
    this(records, null);
  }

  public TableData(Map<Map<String, String>, List<HCatRecord>> partitionRecords) {
    this(null, partitionRecords);
  }

  void commit(TableDataInserter inserter) {
    if (records != null) {
      inserter.insert(null, records);
    } else {
      for (Entry<Map<String, String>, List<HCatRecord>> partition : partitions.entrySet()) {
        inserter.insert(partition.getKey(), partition.getValue());
      }
    }
  }

  List<HCatRecord> getRecords() {
    return records;
  }

  Map<Map<String, String>, List<HCatRecord>> getPartitions() {
    return partitions;
  }

}
