package com.klarna.hiverunner.data;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TsvFileParserTest {

  @Test
  public void parsesTsv() {
    File dataFile = new File("src/test/resources/data/data.tsv");
    TsvFileParser tsvFileParser = new TsvFileParser();
    List<Object[]> result = tsvFileParser.parse(dataFile, "a", "b", "c", "d", "e");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "c2", "d2", "e2" }, result.get(1));
  }

  @Test
  public void parsesCsv() {
    File dataFile = new File("src/test/resources/data/data.csv");
    TsvFileParser tsvFileParser = new TsvFileParser().withDlimiter(",");
    List<Object[]> result = tsvFileParser.parse(dataFile, "a", "b", "c", "d", "e");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", "" }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", "", "d2", "e2" }, result.get(1));
  }

  @Test
  public void csvWithCustomNullValue() {
    File dataFile = new File("src/test/resources/data/dataWithCustomNullValue.csv");
    TsvFileParser tsvFileParser = new TsvFileParser().withDlimiter(",").withNullValue("NULL");
    List<Object[]> result = tsvFileParser.parse(dataFile, "a", "b", "c", "d", "e");
    assertEquals(2, result.size());
    assertArrayEquals(new String[] { "a1", "b1", "c1", "d1", null }, result.get(0));
    assertArrayEquals(new String[] { "a2", "b2", null, "d2", "e2" }, result.get(1));
  }

  @Test
  public void withCustomCharset() {
    File dataFile = new File("src/test/resources/data/dataUtf16.tsv");
    TsvFileParser tsvFileParser = new TsvFileParser().withCharset(Charset.forName("UTF16"));
    List<Object[]> result = tsvFileParser.parse(dataFile);
    assertEquals(1, result.size());
    assertArrayEquals(new String[] { "궢" }, result.get(0));
  }
}
