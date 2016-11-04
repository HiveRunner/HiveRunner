package com.klarna.hiverunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.data.TsvFileParser;

@RunWith(StandaloneHiveRunner.class)
public class InsertIntoTableIntegrationTest {

  @HiveSQL(files = {})
  private HiveShell hiveShell;

  @Before
  public void before() {
    hiveShell.execute("create database test_db");
  }
  
  @Test
  public void insertDataIntoOrcPartitionedTable() {
    testInsertDataIntoPartitionedTable("orc");
  }
  
  @Test
  public void insertDataIntoTextPartitionedTable() {
    testInsertDataIntoPartitionedTable("textfile");
  }
  
  @Test
  public void insertDataIntoSequenceFilePartitionedTable() {
    testInsertDataIntoPartitionedTable("sequencefile");
  }

  private void testInsertDataIntoPartitionedTable(String storedAs) {
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("c0 string")
        .append(")")
        .append("partitioned by (c1 string)")
        .append("stored as " + storedAs)
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
  public void insertDataIntoTablePrimitiveParsedStrings() {
    hiveShell.execute(new StringBuilder()
        .append("create table test_db.test_table (")
        .append("c0 string,")
        .append("c1 boolean,")
        .append("c2 tinyint,")
        .append("c3 smallint,")
        .append("c4 int,")
        .append("c5 bigint,")
        .append("c6 float,")
        .append("c7 double,")
        .append("c8 date,")
        .append("c9 timestamp,")
        .append("c10 binary,")
        .append("c11 decimal(3,2),")
        .append("c12 varchar(1),")
        .append("c13 char(1)")
        .append(")")
        .append("stored as orc")
        .toString());

    hiveShell
        .insertInto("test_db", "test_table")
        .newRow()
        .set("c0", "foo")
        .set("c1", "true")
        .set("c2", "0")
        .set("c3", "1")
        .set("c4", "2")
        .set("c5", "3")
        .set("c6", "1.1")
        .set("c7", "2.2")
        .set("c8", "2015-10-15")
        .set("c9", "2015-10-15 23:59:59.999")
        .set("c10", "0,1,2")
        .set("c11", "1.234")
        .set("c12", "ab")
        .set("c13", "cd")
        .commit();

    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");

    assertEquals(1, result.size());

    Object[] row = result.get(0);
    assertEquals("foo", row[0]);
    assertEquals(true, row[1]);
    assertEquals((byte) 0, row[2]);
    assertEquals((short) 1, row[3]);
    assertEquals(2, row[4]);
    assertEquals(3L, row[5]);
    assertEquals(1.1D, (double) row[6], 0.0001D);
    assertEquals(2.2D, (double) row[7], 0.0001D);
    assertEquals("2015-10-15", row[8]);
    assertEquals("2015-10-15 23:59:59.999", row[9]);
    assertArrayEquals(new byte[] { 0, 1, 2 }, (byte[]) row[10]);
    assertEquals("1.23", row[11]);
    assertEquals("a", row[12]);
    assertEquals("c", row[13]);
  }

  @Test
  public void insertsDataFromTsvFileIntoOrcTable() throws IOException {
    File dataFile = new File("src/test/resources/data/data.tsv");
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
    hiveShell.insertInto("test_db", "test_table").withAllColumns().addRowsFromTsv(dataFile).commit();
    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", "e1" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "c2", "d2", "e2" }, result.get(1));

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
    hiveShell.insertInto("test_db", "test_table").withAllColumns().addRowsFromDelimited(dataFile, ",", "NULL").commit();
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
        .addRowsFrom(dataFile, new TsvFileParser().withDelimiter(",").withNullValue("NULL"))
        .commit();
    List<Object[]> result = hiveShell.executeStatement("select * from test_db.test_table");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }

}
