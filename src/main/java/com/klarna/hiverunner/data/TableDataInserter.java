package com.klarna.hiverunner.data;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

class TableDataInserter {

  private final String databaseName;
  private final String tableName;
  private final Map<String, String> config;

  TableDataInserter(String databaseName, String tableName, HiveConf conf) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    config = Maps.fromProperties(conf.getAllProperties());
  }

  void insert(Multimap<Map<String, String>, HCatRecord> data) {
    Iterator<Map<String, String>> iterator = data.keySet().iterator();
    while (iterator.hasNext()) {
      Map<String, String> partitionSpec = iterator.next();
      insert(partitionSpec, data.get(partitionSpec));
    }
  }

  private void insert(Map<String, String> partitionSpec, Iterable<HCatRecord> rows) {
    WriteEntity entity = new WriteEntity.Builder()
        .withDatabase(databaseName)
        .withTable(tableName)
        .withPartition(partitionSpec)
        .build();

    try {
      HCatWriter master = DataTransferFactory.getHCatWriter(entity, config);
      WriterContext context = master.prepareWrite();
      HCatWriter writer = DataTransferFactory.getHCatWriter(context);
      writer.write(rows.iterator());
      master.commit(context);
    } catch (HCatException e) {
      throw new RuntimeException("An error occurred while inserting data to " + databaseName + "." + tableName, e);
    }
  }

}
