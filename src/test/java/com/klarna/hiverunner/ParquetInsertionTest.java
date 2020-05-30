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
package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(StandaloneHiveRunner.class)
public class ParquetInsertionTest {
    /***
     * Methods that set up data in HiveRunner use HCatalog, which initially did not support writing to Parquet files.
     *
     * A version of HCatalog with this functionality working was introduced in Hive 3.
     * It was also subsequently back-ported to Hive 2.3.7, which is used in HiveRunner >= 5.2.0.
     *
     * This test validates that Parquet insertion is now possible. It has been verified to fail on HiveRunner <= 5.1.x.
     */

    @HiveSQL(files = {})
    private HiveShell hiveShell;

    private static final String tableName = "parquet_test_table";

    @HiveSetupScript
    private static final String createTableScript = "CREATE TABLE " + tableName + " (col1 string) STORED AS PARQUET;";

    @Test
    public void testCanInsertToParquetTable() {
        String textValue = "Some text value";
        hiveShell.insertInto("default", tableName).addRow(textValue).commit();
        Assert.assertEquals(hiveShell.executeQuery("SELECT col1 FROM " + tableName), Arrays.asList(textValue));
    }

}
