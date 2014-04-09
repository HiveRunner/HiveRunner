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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.TException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * HiveServer wrapper
 */
 public class HiveServerContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServerContainer.class);

    private HiveServer.HiveServerHandler client;

    private HiveServerContext context;

    HiveServerContainer() {
    }

    public HiveServer.HiveServerHandler getClient() {
        return client;
    }

    /**
     * Will start the HiveServer.
     * @param testConfig Specific test case properties. Will be merged with the HiveConf of the context
     * @param context    The context configuring the HiveServer and it's environment
     */
    public void init(Map<String, String> testConfig, HiveServerContext context) {

        this.context = context;

        HiveConf hiveConf = context.getHiveConf();

        // merge test case properties with hive conf before HiveServer is started.
        for (Map.Entry<String, String> property : testConfig.entrySet()) {
            hiveConf.set(property.getKey(), property.getValue());
        }

        try {
            client = new HiveServer.HiveServerHandler(hiveConf);
        } catch (MetaException e) {
            throw new IllegalStateException("Failed to create HiveServer :" + e.getMessage(), e);
        }

        // Smoke test HiveServer started
        pingHiveServer();
    }


    public TemporaryFolder getBaseDir() {
        return context.getBaseDir();
    }


    /**
     * Executes a single hql statement.
     * @param hiveql to execute
     * @return the result of the statement
     */
    public List<String> executeQuery(String hiveql) {
        try {
            client.execute(hiveql);
            return client.fetchAll();
        } catch (TException e) {
            throw new IllegalStateException("Failed to executeQuery Hive query " + hiveql + ": " + e.getMessage(), e);
        }
    }

    /**
     * Executes a hive script.
     * @param hiveql hive script statements.
     */
    public void executeScript(String hiveql) {
        for (String statement : splitStatements(hiveql)) {
            try {
                client.execute(statement);
            } catch (TException e) {
                throw new IllegalStateException(
                        "Failed to executeQuery Hive query " + statement + ": " + e.getMessage(), e);
            }
        }

    }

    /**
     * Release all resources.
     */
    public void tearDown() {
        try {
            // Reset to default schema
            client.execute("USE default");
        } catch (Throwable e) {
            throw new IllegalStateException("Failed to reset to default schema: " + e.getMessage(), e);
        } finally {
            client.shutdown();
            client = null;

            // Force reset of static field 'createDefaultDB' since Hive will not recreate the meta store otherwise.
            ReflectionUtils.setStaticField(HiveMetaStore.HMSHandler.class, "createDefaultDB", false);
            LOGGER.info("Tore down HiveServer instance");
        }
    }

    public String expandVariableSubstitutes(String expression) {
        return new VariableSubstitution().substitute(getClient().getHiveConf(), expression);
    }

    /**
     * Package protected due to testability convenience.
     */
    String[] splitStatements(String hiveql) {
        return hiveql.split("(?<=[^\\\\]);");
    }

    private void pingHiveServer() {
        // Ping hive server before we do anything more with it! If validation
        // is switched on, this will fail if metastorage is not set up properly
        try {
            client.execute("SHOW TABLES");
        } catch (TException e) {
            throw new IllegalStateException("Failed to ping HiveServer: " + e.getMessage(), e);
        }
    }


}

