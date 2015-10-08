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

import java.util.Map;

/**
 * Configuration for the HiveServer.
 *
 * Implementations of this interface should set the context of the HiveServer that is spawned by HiveRunner. {@link
 * com.klarna.hiverunner.StandaloneHiveRunner} uses the {@link StandaloneHiveServerContext} to create a context with
 * zero external dependencies.
 *
 * By implementing other contexts you may e.g. point hiveserver to a different metastore, pre installed external hadoop
 * instance etc.
 */
public interface HiveServerContext {

    /**
     * Create all test resources and set all hive configurations.
     *
     * Note that before this method is called, not all injected dependencies might have been initialized.
     * After this method is called, all configurations and resources should have been set.
     *
     * Called by {@link HiveServerContainer#init(Map)}
     */
    void init();

    /**
     * Get the hiveconf. This will not be available until init() has been called.
     */
    HiveConf getHiveConf();

    /**
     * Get file folder that acts as the base dir for the test data. This is the sand box for the
     * file system that the HiveRunner uses as replacement for HDFS.
     * <p/>
     * Each test method will have a new base dir spawned by the HiveRunner engine.
     */
    TemporaryFolder getBaseDir();
}
