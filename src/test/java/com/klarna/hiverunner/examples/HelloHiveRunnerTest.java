/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A basic Hive Runner example showing how to setup the test source database and target database, execute the query
 * and then validate the result.
 *
 * In this example we want to test some very simple code, calculate_max.sql, that calculate a max value by year.
 *
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner and have a reference to HiveShell.
 */
@ExtendWith(HiveRunnerExtension.class)
public class HelloHiveRunnerTest {

    @HiveSQL(files = {})
    private HiveShell shell;

    @BeforeEach
    public void setupSourceDatabase() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.test_table (")
            .append("year STRING, value INT")
            .append(")")
            .toString());

        shell.execute(Paths.get("src/test/resources/HelloHiveRunnerTest/create_max.sql"));
    }

    @Test
    public void testMaxValueByYear() {
        /*
         * Insert some source data
         */
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        /*
         * Execute the query
         */
        shell.execute(Paths.get("src/test/resources/HelloHiveRunnerTest/calculate_max.sql"));

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("select * from my_schema.result");

        assertEquals(2, result.size());
        assertArrayEquals(new Object[]{"2014",4}, result.get(0));
        assertArrayEquals(new Object[]{"2015",5}, result.get(1));
    }
}
