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

package com.klarna.hiverunner.examples;

import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Hive Runner Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class HelloAnnotatedHiveRunner {


    /**
     * Explicit test class configuration of the HiveRunner runtime.
     * See {@link HiveRunnerConfig} for further details.
     */
    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
        setHiveExecutionEngine("mr");
    }};

    /**
     * Cater for all the parameters in the script that we want to test.
     * Note that the "hadoop.tmp.dir" is one of the dirs defined by the test harness
     */
    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "MY.HDFS.DIR", "${hadoop.tmp.dir}",
            "my.schema", "bar",
    });

    /**
     * In this example, the scripts under test expects a schema to be already present in hive so
     * we do that with a setup script.
     * <p/>
     * There may be multiple setup scripts but the order of execution is undefined.
     */
    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:my.schema}";

    /**
     * Create some data in the target directory. Note that the 'targetFile' references the
     * same dir as the create table statement in the script under test.
     * <p/>
     * This example is for defining the data in line as a string.
     */
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_string.csv")
    private String dataFromString = "2,World\n3,!";

    /**
     * Create some data in the target directory. Note that the 'targetFile' references the
     * same dir as the create table statement in the script under test.
     * <p/>
     * This example is for defining the data in in a resource file.
     */
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_file.csv")
    private File dataFromFile =
            new File(ClassLoader.getSystemResource("helloHiveRunner/hello_hive_runner.csv").getPath());


    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {
            "helloHiveRunner/create_table.sql",
            "helloHiveRunner/create_ctas.sql"
    }, encoding = "UTF-8")
    private HiveShell hiveShell;


    @Test
    public void testTablesCreated() {
        HashSet<String> expected = Sets.newHashSet("foo", "foo_prim");
        HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("show tables"));

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSelectFromFooWithCustomDelimiter() {
        HashSet<String> expected = Sets.newHashSet("3,!", "2,World", "1,Hello", "N/A,bar");
        HashSet<String> actual = Sets.newHashSet(hiveShell.executeQuery("select * from foo", ",", "N/A"));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSelectFromFooWithTypeCheck() {

        List<Object[]> actual = hiveShell.executeStatement("select * from foo order by i");

        Assert.assertArrayEquals(new Object[]{null, "bar"}, actual.get(0));
        Assert.assertArrayEquals(new Object[]{1, "Hello"}, actual.get(1));
        Assert.assertArrayEquals(new Object[]{2, "World"}, actual.get(2));
        Assert.assertArrayEquals(new Object[]{3, "!"}, actual.get(3));
    }


    @Test
    public void testSelectFromCtas() {
        HashSet<String> expected = Sets.newHashSet("Hello", "World", "!");
        HashSet<String> actual = Sets.newHashSet(hiveShell
                .executeQuery("select a.s from (select s, i from foo_prim order by i) a where a.i is not null"));
        Assert.assertEquals(expected, actual);
    }

}
