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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Hive Runner Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class NeverEndingTest {

    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {})
    private HiveShell hiveShell;

    @Ignore
    @Test(expected = TimeoutException.class)
    public void neverEnd() throws InterruptedException {
        load();
    }

    private static int timeouts = 0;

//    @Ignore
    @Test
    public void endOnSecondRun() throws InterruptedException {
        if (timeouts == 0) {
            timeouts++;
            load();
        }
        System.out.println("SUCCESS");
    }

    private void load() {
        System.out.println(hiveShell.getBaseDir().getRoot());
        System.out.println(hiveShell.executeQuery("show databases"));
        hiveShell.execute("create database baz");
        System.out.println(hiveShell.executeQuery("describe database baz"));
        hiveShell.execute("use baz");

        hiveShell.execute("create temporary function nonstop as 'com.klarna.hiverunner.NeverEndingUdf'");

        hiveShell.execute("create table foo (bar string)");

        hiveShell.execute("insert into table foo values ('a'), ('b'), ('c')");

        hiveShell.executeQuery("select nonstop(bar) from foo");
    }

}
