package com.klarna.hiverunner.data;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
    List<HCatRecord> records1 = new ArrayList<>();
    records1.add(new DefaultHCatRecord(new ArrayList<Object>(Lists.newArrayList("aa", "bb"))));
    records1.add(new DefaultHCatRecord(new ArrayList<Object>(Lists.newArrayList("aa2", "bb2"))));

    List<HCatRecord> records2 = new ArrayList<>();
    records2.add(new DefaultHCatRecord(new ArrayList<Object>(Lists.newArrayList("cc", "dd"))));

    List<HCatRecord> records3 = new ArrayList<>();
    records3.add(new DefaultHCatRecord(new ArrayList<Object>(Lists.newArrayList("ee", "ff"))));

    TableDataInserter inserter = new TableDataInserter(TEST_DB, TEST_TABLE, hiveShell.getHiveConf());
    inserter.insert(ImmutableMap.of("local_date", "2015-10-14"), records1);
    inserter.insert(ImmutableMap.of("local_date", "2015-10-14"), records2);
    inserter.insert(ImmutableMap.of("local_date", "2015-10-15"), records3);

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
