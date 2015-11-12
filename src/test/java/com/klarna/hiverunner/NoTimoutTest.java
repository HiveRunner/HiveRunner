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

import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class NoTimoutTest {

    @HiveRunnerSetup
    public final static HiveRunnerConfig CONFIG = new HiveRunnerConfig() {{
        setTimeoutEnabled(false);
        setTimeoutSeconds(5);
        setTimeoutRetries(2);
    }};

    @HiveSQL(files = {})
    private HiveShell hiveShell;

    @Before
    public void prepare() {
        String disableTimeout = System.getProperty("disableTimeout");
        if (disableTimeout != null && Boolean.parseBoolean(disableTimeout)) {
            System.out.println("Terminating test with success because timeout is disabled.");
        } else {
            hiveShell.execute("create database baz");
            hiveShell.execute("use baz");
            hiveShell.execute("create temporary function sleep_one_second_udf as 'com.klarna.hiverunner.SlowlyFailingUdf'");
            hiveShell.execute("create table foo (bar string)");
            hiveShell.execute("insert into table foo values ('a')");
        }
    }

    /**
     * Regression test for deadlock in ThrowOnTimeout.java that occured when running with long running test case and disabled timeout.
     *
     * If the deadlock is introduced, this test will never terminate.
     */
    @Test(expected = IllegalArgumentException.class)
    public void test() {
        hiveShell.executeQuery("select sleep_one_second_udf(bar) from foo");
    }
}
