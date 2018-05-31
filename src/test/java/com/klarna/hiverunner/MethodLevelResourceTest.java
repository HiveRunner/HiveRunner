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

import com.google.common.io.Resources;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;

@RunWith(StandaloneHiveRunner.class)
public class MethodLevelResourceTest {

    @HiveSetupScript
    String createTable = "CREATE EXTERNAL TABLE foo (i INT, j INT, k INT)" +
            "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
            "  STORED AS TEXTFILE" +
            "  LOCATION '${hiveconf:hadoop.tmp.dir}'";

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell hiveShell;

    @Test()
    public void resourceLoadingAsStringTest() {

        hiveShell.addResource("${hiveconf:hadoop.tmp.dir}/data.csv", "1,2,3");
        hiveShell.start();

        Assert.assertEquals(Arrays.asList("1\t2\t3"), hiveShell.executeQuery("SELECT * FROM foo"));
    }

    @Test()
    public void resourceLoadingAsFileTest() throws URISyntaxException {

        hiveShell.addResource("${hiveconf:hadoop.tmp.dir}/data.csv",
                new File(Resources.getResource("methodLevelResourceTest/MethodLevelResourceTest.txt").toURI()));

        hiveShell.start();
        Assert.assertEquals(Arrays.asList("1\t2\t3"), hiveShell.executeQuery("SELECT * FROM foo"));
    }


}
