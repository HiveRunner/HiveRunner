package com.klarna.hiverunner.data;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class TableDataInserterTest {

  private static final String TEST_TABLE = "test_table";
  private static final String TEST_DB = "testdb";
  @HiveSQL(encoding = "UTF-8", files = {})
  private HiveShell hiveShell;
  private String dataLocation;

  @Before
  public void setUp() throws IOException {
    dataLocation = hiveShell.getBaseDir().newFolder("target", "hiverunner_data").getAbsolutePath();
    hiveShell.execute("create database testdb");
    hiveShell.execute("create table testdb.test_table (a STRING, b STRING) "
        + "PARTITIONED BY(local_date STRING) STORED AS ORC LOCATION '" + dataLocation + "'");
  }

  @Test
  public void insertsRowsIntoExistingTable() {
    Multimap<Map<String, String>, HCatRecord> data = ImmutableMultimap
        .<Map<String, String>, HCatRecord> builder()
        .put(of("local_date", "2015-10-14"), new DefaultHCatRecord(asList((Object) "aa", "bb")))
        .put(of("local_date", "2015-10-14"), new DefaultHCatRecord(asList((Object) "aa2", "bb2")))
        .put(of("local_date", "2015-10-14"), new DefaultHCatRecord(asList((Object) "cc", "dd")))
        .put(of("local_date", "2015-10-15"), new DefaultHCatRecord(asList((Object) "ee", "ff")))
        .build();

    TableDataInserter inserter = new TableDataInserter(TEST_DB, TEST_TABLE, hiveShell.getHiveConf());
    inserter.insert(data);

    List<String> result = hiveShell.executeQuery("select * from testdb.test_table");
    Collections.sort(result);

    assertEquals(4, result.size());
    assertEquals("aa", result.get(0).split("\t")[0]);
    assertEquals("bb", result.get(0).split("\t")[1]);
    assertEquals("2015-10-14", result.get(0).split("\t")[2]);

    assertEquals("aa2", result.get(1).split("\t")[0]);
    assertEquals("bb2", result.get(1).split("\t")[1]);
    assertEquals("2015-10-14", result.get(1).split("\t")[2]);

    assertEquals("cc", result.get(2).split("\t")[0]);
    assertEquals("dd", result.get(2).split("\t")[1]);
    assertEquals("2015-10-14", result.get(2).split("\t")[2]);

    assertEquals("ee", result.get(3).split("\t")[0]);
    assertEquals("ff", result.get(3).split("\t")[1]);
    assertEquals("2015-10-15", result.get(3).split("\t")[2]);
  }

}
