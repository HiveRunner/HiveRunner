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

import com.klarna.reflection.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.Hadoop20SShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.rules.TemporaryFolder;

/**
 * Adds specific tweaks to be able to run hive runner tests using map-reduce as the execution engine.
 */
class MapReduceStandaloneHiveServerContext extends StandaloneHiveServerContextBase {

    MapReduceStandaloneHiveServerContext(TemporaryFolder basedir) {
        super(basedir);

        configureQueryPlanner();
        configureJobTrackerMode();
    }

    private void configureQueryPlanner() {
        // Set to true to resolve a NPE when trying to resolve the path to reduce.xml for UDF count
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
    }

    // This is so peculiar that I'd rather keep it a private method than having a protected empty method in the base class.
    private void configureJobTrackerMode() {
        /*
        * Overload shims to make sure that org.apache.hadoop.hive.ql.exec.MapRedTask#runningViaChild
         * validates to false.
         *
         * Search for usage of org.apache.hadoop.hive.shims.HadoopShims#isLocalMode to find other affects of this.
        */
        ReflectionUtils.setStaticField(ShimLoader.class, "hadoopShims", new Hadoop20SShims() {
            @Override
            public boolean isLocalMode(Configuration conf) {
                return false;
            }
        });
    }
}
