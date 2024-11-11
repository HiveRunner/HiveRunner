/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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
package com.klarna.hiverunner.data;

import static java.util.Arrays.asList;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static com.google.common.collect.ImmutableMap.of;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class TableDataInserterTest {

    private static final String TEST_TABLE = "test_table";
    private static final String TEST_DB = "testdb";
    @HiveSQL(encoding = "UTF-8", files = {})
    private HiveShell hiveShell;
    private String dataLocation;

    @BeforeEach
    public void setUp() throws IOException {
        dataLocation = Files.createTempDirectory(hiveShell.getBaseDir(), "hiverunner_data").toString();
        hiveShell.execute("create database testdb");
        hiveShell.execute("create table testdb.test_table (a STRING, b STRING) "
                + "PARTITIONED BY(local_date STRING) STORED AS ORC LOCATION '" + dataLocation + "'");
    }

    @Test
    public void insertsRowsIntoExistingTable() {
        Multimap<Map<String, String>, HCatRecord> data = ImmutableMultimap
                .<Map<String, String>, HCatRecord>builder()
                .put(of("local_date", "2015-10-14"), new DefaultHCatRecord(asList((Object) "aa", "bb")))
                .put(of("local_date", "2015-10-14"), new DefaultHCatRecord(asList((Object) "aa2", "bb2")))
                .put(of("local_date", "2015-10-14"), new DefaultHCatRecord(asList((Object) "cc", "dd")))
                .put(of("local_date", "2015-10-15"), new DefaultHCatRecord(asList((Object) "ee", "ff")))
                .build();

        TableDataInserter inserter = new TableDataInserter(TEST_DB, TEST_TABLE, hiveShell.getHiveConf());
        inserter.insert(data);

        List<String> result = hiveShell.executeQuery("select * from testdb.test_table");
        Collections.sort(result);

        assertEquals(4, result.size());
        assertEquals("aa", result.get(0).split("\t")[0]);
        assertEquals("bb", result.get(0).split("\t")[1]);
        assertEquals("2015-10-14", result.get(0).split("\t")[2]);

        assertEquals("aa2", result.get(1).split("\t")[0]);
        assertEquals("bb2", result.get(1).split("\t")[1]);
        assertEquals("2015-10-14", result.get(1).split("\t")[2]);

        assertEquals("cc", result.get(2).split("\t")[0]);
        assertEquals("dd", result.get(2).split("\t")[1]);
        assertEquals("2015-10-14", result.get(2).split("\t")[2]);

        assertEquals("ee", result.get(3).split("\t")[0]);
        assertEquals("ff", result.get(3).split("\t")[1]);
        assertEquals("2015-10-15", result.get(3).split("\t")[2]);
    }
}
