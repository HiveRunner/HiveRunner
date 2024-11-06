/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.klarna.hiverunner.config.HiveRunnerConfig;

public class HiveServerContainerTest {

    private Path basedir;
    private HiveServerContainer container;

    @BeforeEach
    public void setup() throws IOException {
        basedir = Files.createTempDirectory("HiveServerContainerTest");
        StandaloneHiveServerContext context = new StandaloneHiveServerContext(basedir, new HiveRunnerConfig());
        context.getHiveConf().setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
        container = new HiveServerContainer(context);
        container.init(new HashMap<>(), new HashMap<>());
    }

    @AfterEach
    public void tearDown() {
        container.tearDown();
    }

    @Test
    public void testGetBasedir() {
        Assertions.assertEquals(basedir.getRoot(), container.getBaseDir().getRoot());
    }

    @Test
    public void testExecuteStatementMR() {
        List<Object[]> actual = container.executeStatement("show databases");
        Assertions.assertEquals(1, actual.size());
        Assertions.assertArrayEquals(new Object[] { "default" }, actual.get(0));
    }

    @Test
    public void testExecuteStatementTez() {
        List<Object[]> actual = container.executeStatement("show databases");
        Assertions.assertEquals(1, actual.size());
        Assertions.assertArrayEquals(new Object[] { "default" }, actual.get(0));
    }

    @Test
    public void testExecuteStatementOutputStreamReset() {
        PrintStream initialPrintStream = System.out;
        container.executeStatement("show databases");
        Assertions.assertEquals(initialPrintStream, System.out);
    }

    @Test
    public void testExecuteStatementOutputStreamResetIfException() {
        PrintStream initialPrintStream = System.out;
        try {
            container.executeStatement("use non-existent");
            Assertions.fail("Exception should be thrown");
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(initialPrintStream, System.out);
        }
    }

    @Test
    public void testTearDownShouldNotThrowException() {
        container.tearDown();
        container.tearDown();
        container.tearDown();
    }

    @Test
    public void testInvalidQuery() throws Throwable {
        try {
            container.executeStatement("use foo");
        } catch (IllegalArgumentException e) {
            Assertions.assertThrows(HiveSQLException.class, () -> {throw e.getCause();});
        }
    }
}
