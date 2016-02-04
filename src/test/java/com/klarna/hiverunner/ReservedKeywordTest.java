/*
 * Copyright 2016 Klarna AB
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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RunWith(StandaloneHiveRunner.class)
public class ReservedKeywordTest {

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell hiveShell;


    /**
     * As of Hive 1.2 there are a number of new reserved keywords, e.g. date, timestamp and update.
     * This test verifies that we still can have backwards compatibility by setting the HiveConf
     * 'hive.support.sql11.reserved.keywords' to false.
     */
    @Test
    public void reservedKeywordsShouldBeAllowedWhenHiveConfIsSet() throws IOException {

        hiveShell.setHiveConfValue("hive.support.sql11.reserved.keywords", "false");
        hiveShell.addSetupScript("CREATE table FOO (date String, timestamp string, update string)");

        hiveShell.start();

    }

    /**
     * As of Hive 1.2 there are a number of new reserved keywords, e.g. date, timestamp and update.
     * This test verifies that we still can use the identifier by adding a backtick quote.
     */
    @Test
    public void reservedKeywordsShouldBeAllowedWhenIdentifierHasBacktickQuote() throws IOException {

        hiveShell.addSetupScript("CREATE table FOO (`date` String, `timestamp` string, `update` string)");

        hiveShell.start();

    }
}
