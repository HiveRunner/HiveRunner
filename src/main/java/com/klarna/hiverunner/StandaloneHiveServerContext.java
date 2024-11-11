/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HADOOP_BIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_CONVERT_JOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_HISTORY_FILE_LOC;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_METADATA_ONLY_QUERIES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SKEW_JOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSAUTOGATHER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_CBO_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCAL_SCRATCH_DIR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_WAREHOUSE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_COLUMNS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_CONSTRAINTS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_TABLES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCH_DIR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COUNTERS_PULL_INTERVAL;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_IN_TEST;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.klarna.hiverunner.config.HiveRunnerConfig;

/**
 * Responsible for common configuration for running the HiveServer within this JVM with zero external dependencies.
 * <p>
 * This class contains a bunch of methods meant to be overridden in order to create slightly different contexts.
 * </p>
 * <p>
 * This context configures HiveServer for both mr and tez. There's nothing contradicting with those configurations so
 * they may coexist in order to allow test cases to alter execution engines within the same test by e.g: 'set
 * hive.execution.engine=tez;'.
 * </p>
 */
public class StandaloneHiveServerContext implements HiveServerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneHiveServerContext.class);

    private String metaStorageUrl;

    protected HiveConf hiveConf = new HiveConf();

    private final Path basedir;
    private final HiveRunnerConfig hiveRunnerConfig;

    public StandaloneHiveServerContext(Path basedir, HiveRunnerConfig hiveRunnerConfig) {
        this.basedir = basedir;
        this.hiveRunnerConfig = hiveRunnerConfig;
    }

    @Override
    public final void init() {

        configureMiscHiveSettings(hiveConf);

        configureMetaStore(hiveConf);

        configureMrExecutionEngine(hiveConf);

        configureTezExecutionEngine(hiveConf);

        configureJavaSecurityRealm(hiveConf);

        configureSupportConcurrency(hiveConf);

        try {
            configureFileSystem(basedir, hiveConf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        configureAssertionStatus(hiveConf);

        overrideHiveConf(hiveConf);
    }

    protected void configureMiscHiveSettings(HiveConf hiveConf) {
        hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);

        // Turn of dependency to calcite library
        hiveConf.setBoolVar(HIVE_CBO_ENABLED, false);

        // Disable to get rid of clean up exception when stopping the Session.
        hiveConf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

        hiveConf.setVar(HADOOP_BIN, "NO_BIN!");
    }

    protected void overrideHiveConf(HiveConf hiveConf) {
        for (Map.Entry<String, String> hiveConfEntry : hiveRunnerConfig.getHiveConfSystemOverride().entrySet()) {
            hiveConf.set(hiveConfEntry.getKey(), hiveConfEntry.getValue());
        }
    }

    protected void configureMrExecutionEngine(HiveConf conf) {
        /*
         * Switch off all optimizers otherwise we didn't manage to contain the map reduction within this JVM.
         */
        conf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
        conf.setBoolVar(HIVE_METADATA_ONLY_QUERIES, false);
        conf.setBoolVar(HIVE_OPT_INDEX_FILTER, false);
        conf.setBoolVar(HIVE_CONVERT_JOIN, false);
        conf.setBoolVar(HIVE_SKEW_JOIN, false);

        // Defaults to a 1000 millis sleep in. We can speed up the tests a bit by setting this to 1 millis instead.
        // org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
        hiveConf.setLongVar(HIVE_COUNTERS_PULL_INTERVAL, 1L);

        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
    }

    protected void configureTezExecutionEngine(HiveConf conf) {
        /*
         * Tez local mode settings
         */
        conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
        conf.set("fs.defaultFS", "file:///");
        conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

        /*
         * Set to be able to run tests offline
         */
        conf.set(TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK, "true");

        /*
         * General attempts to strip of unnecessary functionality to speed up test execution and increase stability
         */
        conf.set(TezConfiguration.TEZ_AM_USE_CONCURRENT_DISPATCHER, "false");
        conf.set(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, "false");
        conf.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
        conf.set(TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX, "1");
        conf.set(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, "false");
        conf.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
        conf.set(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, "false");
    }

    protected void configureJavaSecurityRealm(HiveConf hiveConf) {
        // These three properties gets rid of: 'Unable to load realm info from SCDynamicStore'
        // which seems to have a timeout of about 5 secs.
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        System.setProperty("java.security.krb5.conf", "/dev/null");
    }

    protected void configureAssertionStatus(HiveConf conf) {
        ClassLoader
                .getSystemClassLoader()
                .setPackageAssertionStatus("org.apache.hadoop.hive.serde2.objectinspector", false);
    }

    protected void configureSupportConcurrency(HiveConf conf) {
        hiveConf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
    }

    protected void configureMetaStore(HiveConf conf) {
        configureDerbyLog();

        String jdbcDriver = org.apache.derby.jdbc.EmbeddedDriver.class.getName();
        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // https://lists.apache.org/thread/crlomzvvn3hrzwc7t933rzkxpf8p9wgt
        // Set the Hive Metastore DB driver
        metaStorageUrl = "jdbc:derby:memory:db_" + UUID.randomUUID();
        setMetastoreProperty("hive.metastore.local", "true");
        setMetastoreProperty("hive.metastore.schema.verification.record.version", "false");
        setMetastoreProperty("hive.metastore.fastpath", "true");
        setMetastoreProperty("metastore.try.direct.sql", "false");
        setMetastoreProperty("fs.permissions.umask-mode", "022");

        // No pooling needed. This will save us a lot of threads
        setMetastoreProperty("datanucleus.connectionPoolingType", "None");
        setMetastoreProperty("datanucleus.schema.autoCreateColumns", "true");
        setMetastoreProperty("datanucleus.schema.autoCreateTables", "true");
        setMetastoreProperty("datanucleus.schema.autoCreateSchema", "true");
        setMetastoreProperty("datanucleus.schema.autoCreateAll", "true");
        setMetastoreProperty("datanucleus.fixedDatastore", "true");
        setMetastoreProperty("datanucleus.autoStartMechanism", "SchemaTable");
        setMetastoreProperty("datanucleus.connectiondrivername", jdbcDriver);
        setMetastoreProperty("hive.metastore.schema.verification", "false");
        setMetastoreProperty("metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");
        setMetastoreProperty("javax.jdo.option.ConnectionUserName", "noUser");
        setMetastoreProperty("javax.jdo.option.ConnectionPassword", "noPassword");
        setMetastoreProperty("javax.jdo.option.ConnectionDriverName", jdbcDriver);

        /**
         * If hive.in.test=false (default), Hive will assume that the metastore rdbms has already been initialized
         * with some basic tables and will try to run initial test queries against them.
         * This results in multiple warning stacktraces if the rdbms has not actually been initialized.
         */
        setMetastoreProperty(HIVE_IN_TEST.varname, "true");
        setMetastoreProperty(METASTORE_VALIDATE_CONSTRAINTS.varname, "false");
        setMetastoreProperty(METASTORE_VALIDATE_COLUMNS.varname, "false");
        setMetastoreProperty(METASTORE_VALIDATE_TABLES.varname, "false");
    }

    private void configureDerbyLog() {
        // overriding default derby log path to not go to root of project
        File derbyLogFile;
        try {
            derbyLogFile = File.createTempFile("derby", ".log");
            LOGGER.debug("Derby set to log to " + derbyLogFile.getAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("Error creating temporary derby log file", e);
        }
        System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
    }

    protected void configureFileSystem(Path basedir, HiveConf conf) throws IOException {
        setMetastoreProperty(METASTORE_CONNECT_URL_KEY.varname, metaStorageUrl + ";create=true");

        createAndSetFolderProperty(METASTORE_WAREHOUSE, "warehouse", conf, basedir);
        createAndSetFolderProperty(SCRATCH_DIR, "scratch_dir", conf, basedir);
        createAndSetFolderProperty(LOCAL_SCRATCH_DIR, "local_scratch_dir", conf, basedir);
        createAndSetFolderProperty(HIVE_HISTORY_FILE_LOC, "tmp", conf, basedir);

        createAndSetFolderProperty("hadoop.tmp.dir", "hadoop_tmp_dir", conf, basedir);
        createAndSetFolderProperty("test.log.dir", "logs", conf, basedir);
        createAndSetFolderProperty("hive.metastore.metadb.dir", "warehouse", conf, basedir);

        /*
         * Tez specific configurations below
         */
        /*
         * Tez will upload a hive-exec.jar to this location. It looks like it will do this only once per test suite so it
         * makes sense to keep this in a central location rather than in the tmp dir of each test.
         */
        File installation_dir = newFolder(basedir, "tez_installation_dir").toFile();

        conf.setVar(HiveConf.ConfVars.HIVE_JAR_DIRECTORY, installation_dir.getAbsolutePath());
        conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, installation_dir.getAbsolutePath());
    }

    Path newFolder(Path basedir, String folder) throws IOException {
        Path newFolder = Files.createTempDirectory(basedir, folder);
        FileUtil.setPermission(newFolder.toFile(), FsPermission.getDirDefault());
        return newFolder;
    }

    @Override
    public HiveConf getHiveConf() {
        return hiveConf;
    }

    @Override
    public Path getBaseDir() {
        return basedir;
    }

    protected final void createAndSetFolderProperty(HiveConf.ConfVars var, String folder, HiveConf conf, Path basedir)
            throws IOException {
        setMetastoreProperty(var.varname, newFolder(basedir, folder).toAbsolutePath().toString());
    }

    protected final void createAndSetFolderProperty(String key, String folder, HiveConf conf, Path basedir)
            throws IOException {
        setMetastoreProperty(key, newFolder(basedir, folder).toAbsolutePath().toString());
    }

    protected final void setMetastoreProperty(String key, String value) {
        hiveConf.set(key, value);
        System.setProperty(key, value);
    }
}
