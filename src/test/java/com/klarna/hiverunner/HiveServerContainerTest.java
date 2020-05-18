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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import org.apache.hive.service.cli.HiveSQLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.klarna.hiverunner.config.HiveRunnerConfig;

public class HiveServerContainerTest {

    private Path basedir;
    private HiveServerContainer container;

    @Before
    public void setup() throws IOException {
        basedir = Files.createTempDirectory("HiveServerContainerTest");
        StandaloneHiveServerContext context = new StandaloneHiveServerContext(basedir, new HiveRunnerConfig());
        container = new HiveServerContainer(context);
        container.init(new HashMap<>(), new HashMap<>());
    }

    @After
    public void tearDown() {
        container.tearDown();
    }

    @Test
    public void testGetBasedir() {

        Assert.assertEquals(basedir.getRoot(), container.getBaseDir().getRoot());
    }

    @Test
    public void testExecuteStatementMR() {
        List<Object[]> actual = container.executeStatement("show databases");
        Assert.assertEquals(1, actual.size());
        Assert.assertArrayEquals(new Object[] { "default" }, actual.get(0));
    }

    @Test
    public void testExecuteStatementTez() {
        List<Object[]> actual = container.executeStatement("show databases");
        Assert.assertEquals(1, actual.size());
        Assert.assertArrayEquals(new Object[] { "default" }, actual.get(0));
    }

    @Test
    public void testTearDownShouldNotThrowException() {
        container.tearDown();
        container.tearDown();
        container.tearDown();
    }

    @Test(expected = HiveSQLException.class)
    public void testInvalidQuery() throws Throwable {
        try {
            container.executeStatement("use foo");
        } catch (IllegalArgumentException e) {
            throw e.getCause();
        }
    }
}