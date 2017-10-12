package com.klarna.hiverunner.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TsvFileParserTest {

  @Test
  public void parsesTsv() {
    File dataFile = new File("src/test/resources/data/data.tsv");
    TsvFileParser tsvFileParser = new TsvFileParser();
    List<Object[]> result = tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d", "e"));
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", "e1" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "c2", "d2", "e2" }, result.get(1));
  }

  @Test(expected = IllegalStateException.class)
  public void parsesTsvNotEnoughFieldsInFile() {
    File dataFile = new File("src/test/resources/data/data.tsv");
    TsvFileParser tsvFileParser = new TsvFileParser();
    tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d", "e", "f"));
  }

  @Test
  public void parsesTsvSubSelectFields() {
    File dataFile = new File("src/test/resources/data/data.tsv");
    TsvFileParser tsvFileParser = new TsvFileParser();
    List<Object[]> result = tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d"));
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "c2", "d2" }, result.get(1));
  }

  @Test
  public void parsesCsvWithEmptyFields() {
    File dataFile = new File("src/test/resources/data/data.csv");
    TsvFileParser tsvFileParser = new TsvFileParser().withDelimiter(",");
    List<Object[]> result = tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d", "e"));
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }

  @Test
  public void csvWithCustomNullValue() {
    File dataFile = new File("src/test/resources/data/dataWithCustomNullValue.csv");
    TsvFileParser tsvFileParser = new TsvFileParser().withDelimiter(",").withNullValue("NULL");
    List<Object[]> result = tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d", "e"));
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }

  @Test
  public void tsvWithHeader() {
    File dataFile = new File("src/test/resources/data/dataWithHeader.tsv");
    TsvFileParser tsvFileParser = new TsvFileParser().withHeader();

    assertTrue(tsvFileParser.hasColumnNames());
    assertEquals(tsvFileParser.getColumnNames(dataFile), Arrays.asList("a", "b", "c", "d", "e"));

    List<Object[]> result = tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d", "e"));
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", "e1" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "c2", "d2", "e2" }, result.get(1));
  }

  @Test
  public void csvWithHeader() {
    File dataFile = new File("src/test/resources/data/dataWithHeader.csv");
    TsvFileParser tsvFileParser = new TsvFileParser().withDelimiter(",").withHeader();

    assertTrue(tsvFileParser.hasColumnNames());
    assertEquals(tsvFileParser.getColumnNames(dataFile), Arrays.asList("a", "b", "c", "d", "e"));

    List<Object[]> result = tsvFileParser.parse(dataFile, null, Arrays.asList("a", "b", "c", "d", "e"));
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }
}
