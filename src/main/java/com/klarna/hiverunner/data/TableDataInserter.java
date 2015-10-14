package com.klarna.hiverunner.data;

import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

public class TableDataInserter {

  private final Map<String, String> config;
  private final String databaseName;
  private final String tableName;

  TableDataInserter(Map<String, String> config, String databaseName, String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.config = config;
  }

  public void insert(Map<String, String> partitionSpec, Iterable<HCatRecord> records) {
    WriteEntity entity = new WriteEntity.Builder()
        .withDatabase(databaseName)
        .withTable(tableName)
        .withPartition(partitionSpec)
        .build();
    try {
      WriterContext context = DataTransferFactory.getHCatWriter(entity, config).prepareWrite();
      HCatWriter writer = DataTransferFactory.getHCatWriter(context);

      try {
        writer.write(records.iterator());
      } catch (HCatException e) {
        writer.abort(context);
        throw e;
      }
      writer.commit(context);
    } catch (HCatException e) {
      throw new RuntimeException(e);
    }
  }

}
