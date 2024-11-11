/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;

@ExtendWith(HiveRunnerExtension.class)
public class MacroTest {


    @HiveSQL(files = {})
    public HiveShell hiveShell;

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/foo/foo.data")
    public String data = "easteregg";

    @HiveSetupScript
    public String setup =
            "CREATE TABLE corpus (stanza string)" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '${hiveconf:hadoop.tmp.dir}/foo';";

    @HiveSetupScript
    public String macro =
            "CREATE TEMPORARY MACRO foobarize (literal string) " +
                    "concat('foo', concat(literal, 'bar'));";

    @Test
    public void testMacro() {
        Assertions.assertEquals(Arrays.asList("fooeastereggbar"), hiveShell.executeQuery("select foobarize(stanza) from corpus"));
    }

}
