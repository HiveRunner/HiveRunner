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
 * Context to run hadoop bin in a parallel JVM. Used for debugging of the HiveRunner
 * <p/>
 * NOTE: Don't forget to set the hadoop bin path property
 */
class LocalModeContext extends StandaloneHiveServerContext {
    LocalModeContext(TemporaryFolder basedir) {
        super(basedir);
    }

    @Override
    protected void configureJobTrackerMode(HiveConf conf) {
        // Overriding standalone context to reset to default
    }

    @Override
    protected void configureAssertionStatus(HiveConf conf) {
        // Overriding standalone context to reset to default
    }
}
