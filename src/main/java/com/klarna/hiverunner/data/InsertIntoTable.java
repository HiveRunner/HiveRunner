package com.klarna.hiverunner.data;

import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;

import com.klarna.hiverunner.HiveShell;

/**
 * A class for fluently creating a list of rows and inserting them into a table.
 */
public final class InsertIntoTable {

  private final TableDataBuilder builder;
  private final TableDataInserter inserter;

  /**
   * Factory method for creating an {@link InsertIntoTable}.
   * <p/>
   * This method is intended to be called via {@link HiveShell#insertInto(String, String)}.
   *
   * @param databaseName The database name.
   * @param tableName The table name.
   * @param conf The {@link HiveConf}.
   * @return InsertIntoTable
   */
  public static InsertIntoTable newInstance(String databaseName, String tableName, HiveConf conf) {
    TableDataBuilder builder = new TableDataBuilder(getHCatTable(databaseName, tableName, conf));
    TableDataInserter inserter = new TableDataInserter(databaseName, tableName, conf);
    return new InsertIntoTable(builder, inserter);
  }

  private static HCatTable getHCatTable(String databaseName, String tableName, HiveConf conf) {
    HCatClient client = null;
    try {
      client = HCatClient.create(conf);
      return client.getTable(databaseName, tableName);
    } catch (HCatException e) {
      throw new RuntimeException("Unable to get table from the metastore.", e);
    } finally {
      if (client != null) {
        try {
          client.close();
        } catch (HCatException e) {
          throw new RuntimeException("Unable close client.", e);
        }
      }
    }
  }

  InsertIntoTable(TableDataBuilder builder, TableDataInserter inserter) {
    this.builder = builder;
    this.inserter = inserter;
  }

  /**
   * Defines a subset of columns (a column name mask) so that only pertinent columns can be set.
   * <p/>
   * e.g.
   *
   * <pre>
   * {@code
   * tableDataBuilder
   *     .withColumns("col1", "col3")
   *     .addRow("value1", "value3")
   * }
   * </pre>
   *
   * @param names The column names.
   * @return {@code this}
   * @throws IllegalArgumentException if a column name does not exist in the table.
   */
  public InsertIntoTable withColumns(String... names) {
    builder.withColumns(names);
    return this;
  }

  /**
   * Resets the column name mask to all the columns in the table.
   *
   * @return {@code this}
   */
  public InsertIntoTable withAllColumns() {
    builder.withAllColumns();
    return this;
  }

  /**
   * Flushes the current row and creates a new row with {@code null} values for all columns.
   *
   * @return {@code this}
   */
  public InsertIntoTable newRow() {
    builder.newRow();
    return this;
  }

  /**
   * Flushes the current row and creates a new row with the values specified.
   *
   * @param values The values to set.
   * @return {@code this}
   */
  public InsertIntoTable addRow(Object... values) {
    builder.addRow(values);
    return this;
  }

  /**
   * Sets the current row with the values specified.
   *
   * @param values The values to set.
   * @return {@code this}
   */
  public InsertIntoTable setRow(Object... values) {
    builder.setRow(values);
    return this;
  }

  /**
   * Adds all rows from the TSV file specified. The default delimiter is tab and the default null value is an empty
   * string.
   *
   * @param file The file to read the data from.
   * @return {@code this}
   */
  public InsertIntoTable addRowsFromTsv(File file) {
    builder.addRowsFromTsv(file);
    return this;
  }

  /**
   * Adds all rows from the TSV file specified, using the provided delimiter and null value.
   *
   * @param file The file to read the data from.
   * @param delimiter A column delimiter.
   * @param nullValue Value to be treated as null in the source data.
   * @return {@code this}
   */
  public InsertIntoTable addRowsFromDelimited(File file, String delimiter, Object nullValue) {
    builder.addRowsFromDelimited(file, delimiter, nullValue);
    return this;
  }

  /**
   * Adds all rows from the file specified, using the provided parser.
   *
   * @param file File to read the data from.
   * @param fileParser Parser to be used to parse the file.
   * @return {@code this}
   */
  public InsertIntoTable addRowsFrom(File file, FileParser fileParser) {
    builder.addRowsFrom(file, fileParser);
    return this;
  }

  /**
   * Flushes the current row and creates a new row with the same values.
   *
   * @return {@code this}
   */
  public InsertIntoTable copyRow() {
    builder.copyRow();
    return this;
  }

  /**
   * Set the given column name to the given value.
   *
   * @param name The column name to set.
   * @param value the value to set.
   * @return {@code this}
   * @throws IllegalArgumentException if a column name does not exist in the table.
   */
  public InsertIntoTable set(String name, Object value) {
    builder.set(name, value);
    return this;
  }

  /**
   * Inserts the data into the table. This does not replace any existing data, but appends new part files to the
   * table/partition location(s).
   */
  public void commit() {
    inserter.insert(builder.build());
  }

}
