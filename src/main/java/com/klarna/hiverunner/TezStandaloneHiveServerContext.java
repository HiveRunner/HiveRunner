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
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.rules.TemporaryFolder;

/**
 * Adds specific tweaks to be able to run hive runner tests using tez as the execution engine.
 */
class TezStandaloneHiveServerContext extends StandaloneHiveServerContextBase {

    TezStandaloneHiveServerContext(TemporaryFolder basedir) {
        super(basedir);
    }

    @Override
    protected void configureExecutionEngine(HiveConf conf) {
        /*
        Enable tez execution engine
         */
        conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");

        /*
        Tez local mode settings
         */
        conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
        conf.set("fs.defaultFS", "file:///");
        conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

        /*
            Tez will upload a hive-exec.jar to this location.
            It looks like it will do this only once per test suite so it makes sense to keep this in a central location
            rather than in the tmp dir of each test.
         */
        conf.setVar(HiveConf.ConfVars.HIVE_JAR_DIRECTORY, "target/dependency");
        conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, "target/dependency");

        /*
        Set to be able to run tests offline
         */
        conf.set(TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK, "true");

        /*
        General attempts to strip of unnecessary functionality to speed up test execution and increase stability
         */
        conf.set(TezConfiguration.TEZ_AM_USE_CONCURRENT_DISPATCHER, "false");
        conf.set(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, "false");
        conf.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
        conf.set(TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX, "1");
        conf.set(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, "false");
        conf.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
        conf.set(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, "false");

    }
}
