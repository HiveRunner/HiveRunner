/**
 * Copyright (C) 2013-2018 Klarna AB
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
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class MSCKRepairNpeTest {

    @HiveSQL(files = {})
    public HiveShell hiveShell;

    @Test
    public void testMsckRepair() {
        hiveShell.execute("set hive.mv.files.thread=0");

        hiveShell.execute("CREATE EXTERNAL TABLE foo (id int)" +
                "  PARTITIONED BY(month int)" +
                "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                "  STORED AS TEXTFILE" +
                "  LOCATION '${hiveconf:hadoop.tmp.dir}/foo';");


        // This will throw a NPE in Hive 2.1.0/2.2.0 (See https://issues.apache.org/jira/browse/HIVE-14798 and https://issues.apache.org/jira/browse/HIVE-14924) 
        hiveShell.execute("MSCK REPAIR TABLE foo");
    }
}
