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

import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(StandaloneHiveRunner.class)
public class IntegerPartitionFormatTest {


    @HiveSQL(files = {})
    public HiveShell hiveShell;

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/foo/month=07/foo.data")
    public String data = "06\n6";

    @HiveSetupScript
    public String setup =
            "CREATE EXTERNAL TABLE foo (id int)" +
                    "  PARTITIONED BY(month int)" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '${hiveconf:hadoop.tmp.dir}/foo';";

    @Before
    public void repair() {
        // MSCK REPAIR TABLE adds metadata about partitions to the Hive metastore for
        // partitions for which such metadata doesn't already exist
        hiveShell.execute("MSCK REPAIR TABLE foo");
    }

    @Test
    public void testInteger() {
        Assert.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 6"));
    }

    @Test
    public void testPrefixedInteger() {
        Assert.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 06"));
    }


    @Test
    public void testPrefixedPartitionInteger() {
        Assert.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 6 and month = 07"));
    }


    @Test
    public void testNonPrefixedPartitionInteger() {
        Assert.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 6 and month = 7"));
    }
}
