package com.klarna.hiverunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class InsertIntoTableTest {

  @HiveSQL(files = {})
  private HiveShell hiveShell;

  @Test
  public void insertDataIntoTable() {
    hiveShell.execute("create database test_db");
    hiveShell.execute("create table test_db.test_table (col1 string) partitioned by (col2 string) stored as orc");

    hiveShell
        .insertInto("test_db", "test_table")
        .addRow("v1", "p1")
        .addRow("v2", "p1")
        .addRow("v3", "p2")
        .addRow("v4", "p2")
        .commit();

    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");
    assertEquals(4, result.size());
    assertArrayEquals(new Object[] { "v1", "p1" }, result.get(0));
    assertArrayEquals(new Object[] { "v2", "p1" }, result.get(1));
    assertArrayEquals(new Object[] { "v3", "p2" }, result.get(2));
    assertArrayEquals(new Object[] { "v4", "p2" }, result.get(3));
  }

}
