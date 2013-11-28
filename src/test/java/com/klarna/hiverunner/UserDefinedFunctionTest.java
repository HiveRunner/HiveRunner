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
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class UserDefinedFunctionTest {


    private final String hdfsSource = "${hiveconf:hadoop.tmp.dir}/udf";

    @HiveSetupScript
    String setup =
            "  CREATE TABLE udf_test (" +
                    " id int," +
                    " value string" +
                    "  )" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '" + hdfsSource + "' ";


    @HiveSQL(files = {}, autoStart = false)
    public HiveShell hiveShell;

    @Test
    public void udfMax() {
        hiveShell.addResource(hdfsSource + "/data.csv",
                "123\tv1\n" +
                        "124\tv2\n" +
                        "125\tv3");
        hiveShell.start();
        Assert.assertEquals(Arrays.asList("125"), hiveShell.executeQuery("SELECT max(id) FROM udf_test"));
    }

    @Test
    public void udfMin() {
        hiveShell.addResource(hdfsSource + "/data.csv",
                "123\tv1\n" +
                        "124\tv2\n" +
                        "125\tv3");
        hiveShell.start();
        Assert.assertEquals(Arrays.asList("123"), hiveShell.executeQuery("SELECT min(id) FROM udf_test"));
    }

    @Test
    public void regexp_extract() {
        hiveShell.addResource(hdfsSource + "/data.csv", "1\t123ABC");
        hiveShell.start();
        List<String> expected = Arrays.asList("123");
        List<String> actual = hiveShell.executeQuery("SELECT regexp_extract(value, '([0-9]*)[A-Z]*', 1) FROM udf_test");
        Assert.assertEquals(expected, actual);
    }


}

