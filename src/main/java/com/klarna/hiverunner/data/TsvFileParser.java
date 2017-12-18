package com.klarna.hiverunner.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.google.common.base.Splitter;

/**
 * A {@link FileParser} for parsing data out of a TSV file.
 */
public class TsvFileParser implements FileParser {

  private static final String DEFAULT_DELIMITER = "\t";
  private static final String DEFAULT_NULL_VALUE = "";

  private Splitter splitter;
  private Object nullValue;
  private Charset charset;
  private boolean hasHeader;

  public TsvFileParser() {
    withDelimiter(DEFAULT_DELIMITER);
    withNullValue(DEFAULT_NULL_VALUE);
    withCharset(StandardCharsets.UTF_8);
    withoutHeader();
  }

  /**
   * Use the provided delimiter. The default is a tab.
   */
  public TsvFileParser withDelimiter(String delimiter) {
    splitter = Splitter.on(delimiter);
    return this;
  }

  /**
   * Use the provided null value. When a column's value equals the null value it will be replaced with null. The default
   * is an empty string.
   */
  public TsvFileParser withNullValue(Object nullValue) {
    this.nullValue = nullValue;
    return this;
  }

  /**
   * Use the provided {@link Charset}. The default is UTF-8.
   */
  public TsvFileParser withCharset(Charset charset) {
    this.charset = charset;
    return this;
  }

  /**
   * Enable if TSV file has header row. Default is false.
   */
  public TsvFileParser withHeader() {
    this.hasHeader = true;
    return this;
  }

  /**
   * Enable if TSV file has header row. Default is false.
   */
  public TsvFileParser withoutHeader() {
    this.hasHeader = false;
    return this;
  }


  @Override
  public List<Object[]> parse(File file, HCatSchema schema, List<String> names) {
    try {
      List<String> lines = Files.readAllLines(file.toPath(), charset);

      if (this.hasHeader) {
        lines = lines.subList(1, lines.size());
      }

      List<Object[]> records = new ArrayList<>(lines.size());
      for (String line : lines) {
        records.add(parseRow(line, names.size()));
      }
      return records;
    } catch (IOException e) {
      throw new RuntimeException("Error while reading file", e);
    }
  }

  @Override
  public boolean hasColumnNames() {
    return this.hasHeader;
  }

  @Override
  public List<String> getColumnNames(File file) {
    try {
      String firstLine = Files.newBufferedReader(file.toPath(), charset).readLine();
      List<String> columns = new ArrayList<>();
      Iterator<String> iterator = splitter.split(firstLine).iterator();

      while (iterator.hasNext()) {
        String column = iterator.next();
        columns.add(column);
      }
      return columns;
    } catch(IOException e) {
      throw new RuntimeException("Error while reading file", e);
    }
  }

  private Object[] parseRow(String line, int size) {
    List<Object> row = new ArrayList<>(size);
    Iterator<String> iterator = splitter.split(line).iterator();

    for (int i = 0; i < size; i++) {
      if (iterator.hasNext()) {
        String column = iterator.next();
        if (ObjectUtils.equals(nullValue, column)) {
          row.add(null);
        } else {
          row.add(column);
        }
      } else {
        throw new IllegalStateException("Not enough columns. Require " + size + " columns, got " + i);
      }
    }

    return row.toArray(new Object[size]);
  }
}
