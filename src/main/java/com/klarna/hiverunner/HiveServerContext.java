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
 * Configuration for the HiveServer.
 */
public interface HiveServerContext {

    String getMetaStoreUrl();

    HiveConf getHiveConf();

    /**
     * Get file folder that acts as the base dir for the test data. This is the sand box for the
     * file system that the HiveRunner uses as replacement for HDFS.
     * <p/>
     * Each test method will have a new base dir spawned by the HiveRunner engine.
     */
    TemporaryFolder getBaseDir();
}
