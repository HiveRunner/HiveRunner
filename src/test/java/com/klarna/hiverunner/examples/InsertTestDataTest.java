/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner.examples;

import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.data.TsvFileParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/*
    This example is intended to be a small show case for some of the ways of setting up your test data in HiveRunner.
    It will only print out some result and thus is not a strict unit test suite.

    The examples will go through cases with adding test data from "code" or from file, and how you only need to supply
    a selected subset of the columns or how to use more advanced features like files with custom separator characters
    or custom NULL keywords in the test data files.
 */
@ExtendWith(HiveRunnerExtension.class)
public class InsertTestDataTest {

    @HiveSQL(files = {})
    private HiveShell shell;

    @BeforeEach
    public void setupDatabase() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table (")
                .append("col_a STRING, col_b INT, col_c BOOLEAN")
                .append(")")
                .toString());
    }

    @Test
    public void insertRowsFromCode() {
        shell.insertInto("source_db", "test_table")
                .withAllColumns()
                .addRow("Value1", 1, true)
                .addRow("Value2", 99, false)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"), "from code");
    }

    @Test
    public void insertRowsFromCodeWithSelectedColumns() {
        shell.insertInto("source_db", "test_table")
                .withColumns("col_a", "col_c")
                .addRow("Value1", true)
                .addRow("Value2", false)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"), "from code selected columns");
    }

    @Test
    public void insertRowsFromTsvFile() {
        File dataFile = new File("src/test/resources/InsertTestDataTest/data1.tsv");
        shell.insertInto("source_db", "test_table")
                .withAllColumns()
                .addRowsFromTsv(dataFile)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"), "TSV file");
    }

    @Test
    public void insertRowsFromTsvFileWithHeader() {
        File dataFile = new File("src/test/resources/InsertTestDataTest/dataWithHeader1.tsv");
        TsvFileParser parser = new TsvFileParser().withHeader();
        shell.insertInto("source_db", "test_table")
                .addRowsFrom(dataFile, parser)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"), "TSV file header");
    }

    @Test
    public void insertRowsFromTsvFileWithSubsetHeader() {
        File dataFile = new File("src/test/resources/InsertTestDataTest/dataWithHeader2.tsv");
        TsvFileParser parser = new TsvFileParser().withHeader();
        shell.insertInto("source_db", "test_table")
                .addRowsFrom(dataFile, parser)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"), "TSV file subset header");
    }

    @Test
    public void insertRowsIntoPartitionedTableStoredAsSequencefileWithCustomDelimiterAndNullValue() {
        File dataFile = new File("src/test/resources/InsertTestDataTest/data2.tsv");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table2 (")
                .append("col_a STRING, col_b INT")
                .append(")")
                .append("partitioned by (col_c string)")
                .append("stored as SEQUENCEFILE")
                .toString());

        shell.insertInto("source_db", "test_table2")
                .withAllColumns()
                .addRowsFrom(dataFile, new TsvFileParser().withDelimiter(":").withNullValue("__NULL__"))
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table2"), "long method name");
    }

    private void printResult(List<Object[]> result, String methodName) {
        System.out.println(String.format("Result from %s:", methodName));
        for (Object[] row : result) {
            System.out.println(Arrays.asList(row));
        }
    }
}
