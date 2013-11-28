/*
 * Copyright 2013 Klarna AB
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

package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Verifies that the database is reset to 'default' in tear down phase.
 * It seems like Hive (at least 0.11) has some static variable to store
 * represent the current set database.
 * <p/>
 * This is solved by doing a 'use default' in {@link com.klarna.hiverunner.builder.HiveShellTearable#tearDown()}
 */
@RunWith(StandaloneHiveRunner.class)
public class SchemaResetBetweenTestMethodsTest {


    @HiveSQL(files = {})
    private HiveShell hiveShell;

    @Test
    public void createDatabaseBar() {
        // Create a table. It is assumed that the current database is 'default'
        hiveShell.execute("create table baz (i int)");
        // If the current database was not 'default', this row would throw an exception
        hiveShell.execute("select * from default.baz");

        // Create any database and set it to current so that the other
        // test case may verify that it was indeed reset to 'default' at teardown.
        hiveShell.execute("create database bar");
        hiveShell.execute("USE bar");

        List<String> expectedDatabases = Arrays.asList("bar", "default");
        List<String> actualDatabases = hiveShell.executeQuery("show databases");

        Collections.sort(expectedDatabases);
        Collections.sort(actualDatabases);

        Assert.assertEquals(expectedDatabases, actualDatabases);

    }

    @Test
    public void createDatabaseFoo() {
        // See comments in test case above
        hiveShell.execute("create table baz (i int)");
        hiveShell.execute("select * from default.baz");
        hiveShell.execute("create database foo");
        hiveShell.execute("USE foo");

        List<String> expectedDatabases = Arrays.asList("foo", "default");
        List<String> actualDatabases = hiveShell.executeQuery("show databases");

        Collections.sort(expectedDatabases);
        Collections.sort(actualDatabases);

        Assert.assertEquals(expectedDatabases, actualDatabases);
    }


}
