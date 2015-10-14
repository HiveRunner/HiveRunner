package com.klarna.hiverunner.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class TableDataInserterFactory {

  private final Map<String, String> config;
  private final String databaseName;
  private final String tableName;

  public TableDataInserterFactory(Configuration conf, String databaseName, String tableName) {
    config = createConfig(conf);
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  public TableDataInserter newInstance() {
    return new TableDataInserter(config, databaseName, tableName);
  }

  private static Map<String, String> createConfig(Configuration conf) {
    Map<String, String> config = new HashMap<>();
    for (Entry<String, String> entry : conf) {
      config.put(entry.getKey(), entry.getValue());
    }
    return config;
  }
}
