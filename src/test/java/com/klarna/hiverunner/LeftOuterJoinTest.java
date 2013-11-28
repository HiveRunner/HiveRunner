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
public class LeftOuterJoinTest {

    private final String hdfsSourceFoo = "${hiveconf:hadoop.tmp.dir}/foo";
    private final String hdfsSourceBar = "${hiveconf:hadoop.tmp.dir}/bar";

    @HiveSetupScript
    String setup =
            "  CREATE TABLE foo (" +
                    " id string," +
                    " value string" +
                    "  )" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '" + hdfsSourceFoo + "' ; "
                    +
                    "  CREATE TABLE bar (" +
                    " id string," +
                    " value string" +
                    "  )" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '" + hdfsSourceBar + "' ;" +
                    "";


    @HiveSQL(files = {}, autoStart = false)
    private HiveShell hiveShell;


    @Test
    public void leftOuterJoin() {
        hiveShell.addResource(hdfsSourceFoo + "/data.csv",
                "id1\tfoo_value1\nid3\tfoo_value3");
        hiveShell.addResource(hdfsSourceBar + "/data.csv",
                "id1\tbar_value1\n" +
                        "id2\tbar_value2");
        hiveShell.start();

        String query = "SELECT foo.id, bar.value FROM foo left outer join bar on (foo.id = bar.id)";

        List<String> expected = Arrays.asList("id1\tbar_value1", "id3\tNULL");
        List<String> actual = hiveShell.executeQuery(query);

        Assert.assertEquals(expected, actual);
    }


}
