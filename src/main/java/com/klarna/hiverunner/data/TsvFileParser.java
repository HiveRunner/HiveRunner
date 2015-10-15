package com.klarna.hiverunner.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;

import com.google.common.base.Splitter;

public class TsvFileParser implements FileParser {

  private static final String DEFAULT_DELIMITER = "\t";
  private Charset charset;
  private String delimiter;
  private Object nullValue = null;

  public TsvFileParser() {
    this.charset = Charset.forName("UTF8");
    this.delimiter = DEFAULT_DELIMITER;
    this.nullValue = null;
  }

  public TsvFileParser withDlimiter(String delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  public TsvFileParser withCharset(Charset charset) {
    this.charset = charset;
    return this;
  }

  public TsvFileParser withNullValue(Object nullValue) {
    this.nullValue = nullValue;
    return this;
  }

  @Override
  public List<Object[]> parse(File file, Object... names) {
    try {
      List<Object[]> records = new ArrayList<>();
      List<String> allLines = Files.readAllLines(file.toPath(), charset);
      for (String line : allLines) {
        records.add(parseRow(line).toArray());
      }
      return records;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<Object> parseRow(String line) {
    Iterator<String> iterator = Splitter.on(delimiter).split(line).iterator();
    List<Object> columnList = new ArrayList<>();
    while (iterator.hasNext()) {
      String column = iterator.next();
      if (ObjectUtils.equals(nullValue, column)) {
        columnList.add(null);
      } else {
        columnList.add(column);
      }
    }
    return columnList;
  }
}
