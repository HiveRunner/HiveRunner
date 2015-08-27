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

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;

/**
 * Adds specific tweaks to be able to run hive runner tests using map-reduce as the execution engine.
 */
class MapReduceStandaloneHiveServerContext extends StandaloneHiveServerContextBase {

    MapReduceStandaloneHiveServerContext(TemporaryFolder basedir) {
        super(basedir);
    }

    @Override
    protected void configureExecutionEngine(HiveConf conf) {
        super.configureExecutionEngine(conf);
        conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
    }
}
