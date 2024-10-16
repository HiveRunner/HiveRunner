/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A basic Hive Runner example showing how to use JUnit5's ParameterizedTest.
 */
@ExtendWith(HiveRunnerExtension.class)
public class HelloHiveRunnerParameterizedTest {

    @HiveSQL(files = {})
    private HiveShell shell;

    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {{
        setHiveExecutionEngine("tez");
    }};

    @ParameterizedTest
    @ValueSource(strings = {"SEQUENCEFILE", "ORC", "PARQUET"})
    public void testFileFormats(String fileFormat) {
        String dbName = "source_db_" + fileFormat.toLowerCase();
        String tableName = "test_table_" + fileFormat.toLowerCase();

        shell.executeStatement("CREATE DATABASE " + dbName);

        shell.executeStatement("CREATE TABLE " + dbName + "." + tableName + " (" +
                "year STRING, value INT" +
                ") stored as " + fileFormat);

        shell.insertInto(dbName, tableName)
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        List<Object[]> result = shell.executeStatement("select year, max(value) from " + dbName + "." + tableName + " group by year");

        assertEquals(2, result.size());
        assertArrayEquals(new Object[]{"2014", 4}, result.get(0));
        assertArrayEquals(new Object[]{"2015", 5}, result.get(1));

        shell.executeStatement("DROP DATABASE " + dbName + " CASCADE");
    }
}