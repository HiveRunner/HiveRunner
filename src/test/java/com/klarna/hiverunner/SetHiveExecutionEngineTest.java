/**
 * Copyright (C) 2013-2018 Klarna AB
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
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HiveRunnerExtension.class)
public class SetHiveExecutionEngineTest {

    @HiveRunnerSetup
    public HiveRunnerConfig config = new HiveRunnerConfig(){{
        setHiveExecutionEngine("tez");
    }};

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell hiveShell;

    @Test
    public void test() {
        hiveShell.start();
        Assertions.assertEquals("tez", hiveShell.getHiveConf().getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
    }

}
