package com.klarna.hiverunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.data.TsvFileParser;

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
        .append("c0 string,")
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

  @Test
  public void insertsDataFromTsvFileIntoOrcTable() throws IOException {
    File dataFile = new File("src/test/resources/data/data_4_cols.tsv");
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("a string,")
        .append("b string,")
        .append("c string,")
        .append("d string")
        .append(")")
        .append("stored as orc")
        .toString());
    hiveShell.insertInto("test_db", "test_table").withAllColumns().addRows(dataFile).commit();
    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "c2", "d2" }, result.get(1));

  }

  @Test
  public void insertsDataFromTsvFileWithCustomDelimiterAndNullValue() throws IOException {
    File dataFile = new File("src/test/resources/data/dataWithCustomNullValue.csv");
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("a string,")
        .append("b string,")
        .append("c string,")
        .append("d string,")
        .append("e string")
        .append(")")
        .append("stored as orc")
        .toString());
    hiveShell.insertInto("test_db", "test_table").withAllColumns().addRows(dataFile, ",", "NULL").commit();
    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }

  @Test
  public void insertsDataFromFileWithCustomStrategy() throws IOException {
    File dataFile = new File("src/test/resources/data/dataWithCustomNullValue.csv");
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("a string,")
        .append("b string,")
        .append("c string,")
        .append("d string,")
        .append("e string")
        .append(")")
        .append("stored as orc")
        .toString());
    hiveShell
        .insertInto("test_db", "test_table")
        .withAllColumns()
        .addRows(dataFile, new TsvFileParser().withDlimiter(",").withNullValue("NULL"))
        .commit();
    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }

}
