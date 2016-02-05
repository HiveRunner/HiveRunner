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

import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;

@RunWith(StandaloneHiveRunner.class)
public class HiveRunnerAnnotationsTest {

    @HiveSetupScript
    private File setupFile = new File(ClassLoader.getSystemResource("hiveRunnerAnnotationsTest/setupFile.csv").getPath());

    @HiveSetupScript
    private Path setupPath = Paths.get(ClassLoader.getSystemResource("hiveRunnerAnnotationsTest/setupPath.csv").getPath());


    @HiveSetupScript
    private String setup = "create table bar (i int);";

    @HiveProperties
    private Map<String, String> props = MapUtils.putAll(new HashMap(), new Object[]{
            "key1", "value1",
            "key2", "value2"
    });

    @HiveSQL(files = {"hiveRunnerAnnotationsTest/hql1.sql"}, autoStart = false)
    private HiveShell hiveShell;

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/foo/fromString.csv")
    public String dataFromString = "1,B\n2,D\nE,F";

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/foo/fromFile.csv")
    public File dataFromFile = new File(ClassLoader.getSystemResource("hiveRunnerAnnotationsTest/testData.csv").getPath());

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/foo/fromPath.csv")
    public Path dataFromPath = Paths.get(ClassLoader.getSystemResource("hiveRunnerAnnotationsTest/testData2.csv").getPath());

    @Before
    public void setup() {
        hiveShell.start();
    }

    @Test
    public void testHiveSQLLoaded() {
        List<String> actual = hiveShell.executeQuery("show tables");
        String[] actualArray = actual.toArray(new String[0]);
        assertThat(actualArray, hasItemInArray("bar"));
    }

    @Test
    public void testSetupScript() {
        List<String> actual = hiveShell.executeQuery("show tables");
        String[] actualArray = actual.toArray(new String[0]);
        assertThat(actualArray, hasItemInArray("foo"));
    }

    @Test
    public void testSetupScriptFromFile() {
        List<String> actual = hiveShell.executeQuery("show tables");
        String[] actualArray = actual.toArray(new String[0]);
        assertThat(actualArray, hasItemInArray("fox"));
    }

    @Test
    public void testSetupScriptFromPath() {
        List<String> actual = hiveShell.executeQuery("show tables");
        String[] actualArray = actual.toArray(new String[0]);
        assertThat(actualArray, hasItemInArray("love"));
    }


    @Test
    public void testPropertiesLoaded() {
        Assert.assertEquals("value1", hiveShell.getHiveConf().get("key1"));
        Assert.assertEquals("value2", hiveShell.getHiveConf().get("key2"));
    }

    @Test
    public void testLoadStringResources() {
        String[] actual = hiveShell.executeQuery("select * from foo").toArray(new String[0]);

        assertThat(actual, hasItemInArray("1\tB"));
        assertThat(actual, hasItemInArray("2\tD"));
        assertThat(actual, hasItemInArray("NULL\tF"));
    }

    @Test
    public void testLoadFileResources() {
        String[] actual = hiveShell.executeQuery("select * from foo").toArray(new String[0]);
        assertThat(actual, hasItemInArray("5\tF"));
        assertThat(actual, hasItemInArray("7\tW"));
    }

    @Test
    public void testLoadPathResources() {
        String[] actual = hiveShell.executeQuery("select * from foo").toArray(new String[0]);
        assertThat(actual, hasItemInArray("8\tT"));
        assertThat(actual, hasItemInArray("10\tQ"));
    }


}
