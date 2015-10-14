package com.klarna.hiverunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class InsertIntoTableTest {

  @HiveSQL(files = {})
  private HiveShell hiveShell;

  @Before
  public void before() {
    hiveShell.execute("create database test_db");
  }

  @Test
  public void insertDataIntoPartitionedTable() {
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("c0 string")
        .append(")")
        .append("partitioned by (c1 string)")
        .append("stored as orc")
        .toString());

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

  @Test
  public void insertDataIntoTable() {
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("c0 string")
        .append("c1 boolean")
        .append(")")
        .append("stored as orc")
        .toString());
    // TODO DM test all types

    hiveShell.insertInto("test_db", "test_table").newRow().set("c0", "foo").set("c1", "true").commit();

    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");

    assertEquals(1, result.size());

    Object[] row = result.get(0);
    assertEquals("foo", row[0]);
    assertEquals(true, row[1]);
  }

}
