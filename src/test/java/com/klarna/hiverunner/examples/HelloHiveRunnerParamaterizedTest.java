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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;

/**
 * A basic Hive Runner example showing how to use JUnit5's ParameterizedTest.
 */
@ExtendWith(HiveRunnerExtension.class)
public class HelloHiveRunnerParamaterizedTest {

    @HiveSQL(files = {})
    private HiveShell shell;

    @BeforeEach
    public void setupSourceDatabase() {
        shell.executeStatement("CREATE DATABASE source_db");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SEQUENCEFILE", "ORC", "PARQUET"})
    public void testFileFormats(String fileFormat) {
        shell.executeStatement(new StringBuilder()
                .append("CREATE TABLE source_db.test_table (")
                .append("year STRING, value INT")
                .append(") stored as ")
                .append(fileFormat)
                .toString());

        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        List<Object[]> result = shell.executeStatement("select year, max(value) from source_db.test_table group by year");

        assertEquals(2, result.size());
        assertArrayEquals(new Object[]{"2014", 4}, result.get(0));
        assertArrayEquals(new Object[]{"2015", 5}, result.get(1));
    }
}
