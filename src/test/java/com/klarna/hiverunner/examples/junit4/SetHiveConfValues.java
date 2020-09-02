/**
 * Copyright (C) 2013-2020 Klarna AB
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
package com.klarna.hiverunner.examples.junit4;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/*
    This example is intended to show how to set HiveConf (or HiveVar) values in HIveRunner.
    HiveConf can be very useful. For instance you might have a global cutoff value that could be set outside your code
    and used in many places in your queries. Common example would be a threshold value or a cutoff timestamp.
    To use the HiveConf values in HiveRunner, you must first make sure to switch off the autoStart flag. Then you can
    set the HiveConf values and, before executing any queries, manually start the HIveRunner shell. Make sure this is
    done first in your test setup. like shown in the example below.
 */
@RunWith(StandaloneHiveRunner.class)
public class SetHiveConfValues {

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell shell;

    @Before
    public void setupDatabases() {
        shell.setHiveConfValue("cutoff", "50");
        shell.start();

        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a (")
            .append("message STRING, value INT")
            .append(")")
            .toString());

        shell.insertInto("source_db", "table_a")
            .withAllColumns()
            .addRow("An ignored message", 1)
            .addRow("Hello", 51)
            .addRow("World", 99)
            .commit();
    }

    @Test
    public void useHiveConfValues() {
        List<Object[]> result = shell.executeStatement("select message from source_db.table_a where value > ${hiveconf:cutoff}");

        for (Object[] row : result) {
            System.out.print(row[0] + " ");
        }
    }
}
