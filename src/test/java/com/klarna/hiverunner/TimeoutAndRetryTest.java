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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test cases for verifying the Timeout functionality of HiveRunner.
 *
 * Due to timing issues these test cases may fail on a low resource test environment. In that case try raising the
 * Timeout by setting the 'TimeoutAndRetryTest.timeout.seconds' property in pom.xml or by passing it via command line
 * like -DTimeoutAndRetryTest.timeout.seconds=60
 */
@RunWith(StandaloneHiveRunner.class)
public class TimeoutAndRetryTest {

    @HiveRunnerSetup
    public final static HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
        setTimeoutEnabled(true);
        String timoutSeconds = System.getProperty("TimeoutAndRetryTest.timeout.seconds");
        setTimeoutSeconds(timoutSeconds == null ? 30 : Integer.parseInt(timoutSeconds));
        setTimeoutRetries(2);
    }};


    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {})
    private HiveShell hiveShell;

    @Before
    public void prepare() {
        String disableTimeout = System.getProperty("disableTimeout");
        if (disableTimeout != null && Boolean.parseBoolean(disableTimeout)) {
            System.out.println("Terminating test with success because timeout is disabled.");
        } else {
            System.out.println(hiveShell.getBaseDir().getRoot());
            System.out.println(hiveShell.executeQuery("show databases"));
            hiveShell.execute("create database baz");
            System.out.println(hiveShell.executeQuery("describe database baz"));
            hiveShell.execute("use baz");

            hiveShell.execute("create temporary function nonstop as 'com.klarna.hiverunner.NeverEndingUdf'");

            hiveShell.execute("create table foo (bar string)");

            hiveShell.execute("insert into table foo values ('a'), ('b'), ('c')");
        }
    }

    /**
     * This test should fail after a number of retries. It's not possible to expect the TimeoutException thrown by
     * the ThrowOnTimeout statement so this test is ignored.
     */
    @Ignore
    @Test
    public void neverEnd() {
        hiveShell.executeQuery("select nonstop(bar) from foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void expectTest() {
        throw new IllegalArgumentException("This should be expected");
    }

    @Test(expected = TimeoutException.class)
    public void expectTimoutTest() {
        throw new TimeoutException("This should be expected");
    }

    private static int throwOnSecondRunTimouts = 0;

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void throwOnSecondRun() {
        if (throwOnSecondRunTimouts == 0) {
            throwOnSecondRunTimouts++;
            try {
                hiveShell.executeQuery("select nonstop(bar) from foo");
            } catch (Throwable e) {
                System.out.println("Ignoring exception: "+  e.getMessage());
                e.printStackTrace();
            }
        } else {

            System.out.println("SECOND RUN!!!!");

            throw new ArrayIndexOutOfBoundsException();
        }

    }


    private static int throwOnSecondRunTimouts2 = 0;

    @Test(expected = TimeoutException.class)
    public void throwOnSecondRun2() {
        if (throwOnSecondRunTimouts2 == 0) {
            throwOnSecondRunTimouts2++;
            try {
                hiveShell.executeQuery("select nonstop(bar) from foo");
            } catch (Throwable e) {
                System.out.println("Ignoring exception: "+  e.getMessage());
                e.printStackTrace();
            }
        } else {

            System.out.println("SECOND RUN!!!!");

            throw new TimeoutException();
        }

    }

    private static int endOnSecondRunTimeouts = 0;

    @Test
    public void endOnSecondRun() {
        if (endOnSecondRunTimeouts == 0) {
            endOnSecondRunTimeouts++;
            try {
                hiveShell.executeQuery("select nonstop(bar) from foo");
            } catch (Throwable e) {
                System.out.println("Ignoring exception: "+  e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("SECOND RUN!!!!");
        }
    }




}
