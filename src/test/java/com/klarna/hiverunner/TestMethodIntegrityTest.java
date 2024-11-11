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
package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@ExtendWith(HiveRunnerExtension.class)
public class TestMethodIntegrityTest {

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell shell;

    @Test
    public void collisionCourseTestMethodOne() {
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data1.csv", "1\n2\n3");
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data2.csv", "4\n5");
        shell.addSetupScript("create database foo;");
        shell.addSetupScript("" +
                " CREATE table foo.bar(id int)" +
                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                " STORED AS TEXTFILE" +
                " LOCATION '${hiveconf:hadoop.tmp.dir}/foo/bar';");
        shell.start();
        List<String> actual = shell.executeQuery("select * from foo.bar");
        List<String> expected = Arrays.asList("1", "2", "3", "4", "5");
        Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

    }

    @Test
    public void collisionCourseTestMethodTwo() {
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data1.csv", "9\n2\n8");
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data3.csv", "6\n7");
        shell.addSetupScript("create database foo;");
        shell.addSetupScript("" +
                " CREATE table foo.bar(id int)" +
                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                " STORED AS TEXTFILE" +
                " LOCATION '${hiveconf:hadoop.tmp.dir}/foo/bar';");
        shell.start();
        List<String> actual = shell.executeQuery("select * from foo.bar");
        List<String> expected = Arrays.asList("2", "6", "7", "8", "9");
        Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

    }

}
