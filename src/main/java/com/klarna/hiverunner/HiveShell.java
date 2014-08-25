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
import org.apache.hadoop.hive.service.HiveServer;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;


/**
 * Test handle to the hive server.
 *
 * Please refer to test class {@link com.klarna.hiverunner.HelloHiveRunner} for usage examples.
 */
public interface HiveShell {

    /**
     * Executes a single query.
     * <p/>
     * May only be called post #start()
     */
    List<String> executeQuery(String script);

    /**
     * Executes a hive script. The script may contain multiple statements delimited by ';'
     * <p/>
     * May only be called post #start()
     */
    void execute(String script);

    /**
     * Direct access to hive client.
     * <p/>
     * May only be called post #start()
     */
    HiveServer.HiveServerHandler getClient();

    /**
     * Start the shell. May only be called once. The test engine will by default call this method,
     * Set {@link com.klarna.hiverunner.annotations.HiveSQL#autoStart()} to false to explicitly control
     * when to start from the test case.
     * <p/>
     * This might be useful for test methods that needs additional setup not catered for with the provided annotations.
     */
    void start();

    /**
     * Set a HiveConf property.
     * <p/>
     * May only be called pre #start()
     */
    void setProperty(String key, String value);

    /**
     * Get the current HiveConf from hive
     */
    HiveConf getHiveConf();

    /**
     * Copy test data into hdfs
     * May only be called pre #start()
     * <p/>
     * {@see com.klarna.hiverunner.MethodLevelResourceTest#resourceLoadingAsFileTest()}
     * and {@see com.klarna.hiverunner.MethodLevelResourceTest#resourceLoadingAsStringTest()}
     */
    void addResource(String targetFile, File sourceFile);

    /**
     * Copy test data into hdfs
     * May only be called pre #start()
     * <p/>
     * {@see com.klarna.hiverunner.MethodLevelResourceTest#resourceLoadingAsFileTest()}
     * and {@see com.klarna.hiverunner.MethodLevelResourceTest#resourceLoadingAsStringTest()}
     */
    void addResource(String targetFile, Path sourceFile);


    /**
     * Copy test data into hdfs
     * May only be called pre #start()
     * <p/>
     * {@see com.klarna.hiverunner.MethodLevelResourceTest#resourceLoadingAsFileTest()}
     * and {@see com.klarna.hiverunner.MethodLevelResourceTest#resourceLoadingAsStringTest()}
     */
    void addResource(String targetFile, String data);

    /**
     * Add a hive script that will be executed when the hive shell is started
     * Scripts will be executed in the order they are added.
     *
     * Note that execution order is not guaranteed with
     * fields annotated with {@link com.klarna.hiverunner.annotations.HiveSetupScript}
     */
    void addSetupScript(String script);

    /**
     * Add hive scripts that will be executed when the hive shell is started. Scripts will be executed in given order.
     *
     * Note that execution order is not guaranteed with
     * fields annotated with {@link com.klarna.hiverunner.annotations.HiveSetupScript}
     */
    void addSetupScripts(Charset charset, File... scripts);

    /**
     * Add hive scripts that will be executed when the hive shell is started. Scripts will be executed in given order.
     *
     * Note that execution order is not guaranteed with
     * fields annotated with {@link com.klarna.hiverunner.annotations.HiveSetupScript}
     */
    void addSetupScripts(Charset charset, Path... scripts);


    /**
     * Add hive scripts that will be executed when the hive shell is started. Scripts will be executed in given order.
     *
     * Default charset will be used to read the given files
     *
     * Note that execution order is not guaranteed with
     * fields annotated with {@link com.klarna.hiverunner.annotations.HiveSetupScript}
     */
    void addSetupScripts(File... scripts);

    /**
     * Add hive scripts that will be executed when the hive shell is started. Scripts will be executed in given order.
     *
     * Default charset will be used to read the given files
     *
     * Note that execution order is not guaranteed with
     * fields annotated with {@link com.klarna.hiverunner.annotations.HiveSetupScript}
     */
    void addSetupScripts(Path... scripts);


    /**
     * Get the test case sand box base dir
     */
    TemporaryFolder getBaseDir();

    /**
     * Resolve all substituted variables with the hive conf.
     * @throws IllegalArgumentException if not all substitutes could be resolved
     * @throws IllegalStateException if the HiveShell was not started yet.
     */
    String expandVariableSubstitutes(String expression);

    /**
     * Open up a stream to write test data into hdfs.
     *
     * May only be called pre #start()
     * No writes to the stream will be allowed post #start()
     *
     * @param targetFile The path to the target file relative to the hive work space
     */
    OutputStream getResourceOutputStream(String targetFile);
}
