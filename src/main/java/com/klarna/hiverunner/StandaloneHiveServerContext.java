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
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;

/**
 * Configuration for running the HiveServer within this JVM with zero external dependencies.
 * <p/>
 * This class contains a bunch of methods meant to be overridden in order to create slightly different contexts.
 */
class StandaloneHiveServerContext implements HiveServerContext {

    private String metaStorageUrl;

    private HiveConf hiveConf = new HiveConf();

    private TemporaryFolder basedir;

    StandaloneHiveServerContext(TemporaryFolder basedir) {
        this.basedir = basedir;

        this.metaStorageUrl = "jdbc:hsqldb:mem:" + UUID.randomUUID().toString();

        hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);

        // Set the hsqldb driver. datanucleus will
        hiveConf.set("datanucleus.connectiondrivername", "org.hsqldb.jdbc.JDBCDriver");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.hsqldb.jdbc.JDBCDriver");

        // No pooling needed. This will save us a lot of threads
        hiveConf.set("datanucleus.connectionPoolingType", "None");

        // Defaults to a 1000 millis sleep in
        // org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
        hiveConf.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);

        hiveConf.setVar(HADOOPBIN, "NO_BIN!");

        try {
            Class.forName(JDBCDriver.class.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }


        configureJavaSecurityRealm(hiveConf);

        configureJobTrackerMode(hiveConf);

        configureSupportConcurrency(hiveConf);

        configureFileSystem(basedir, hiveConf);

        configureMetaStoreValidation(hiveConf);

        configureMapReduceOptimizations(hiveConf);

        configureCheckForDefaultDb(hiveConf);

        configureAssertionStatus(hiveConf);
    }

    protected void configureJavaSecurityRealm(HiveConf hiveConf) {
        // These two properties gets rid of: 'Unable to load realm info from SCDynamicStore'
        // which seems to have a timeout of about 5 secs.
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
    }

    protected void configureAssertionStatus(HiveConf conf) {
        ClassLoader.getSystemClassLoader().setPackageAssertionStatus("org.apache.hadoop.hive.serde2.objectinspector",
                false);
    }

    protected void configureCheckForDefaultDb(HiveConf conf) {
        hiveConf.setBoolean("hive.metastore.checkForDefaultDb", true);
    }

    protected void configureSupportConcurrency(HiveConf conf) {
        hiveConf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
    }

    protected void configureMetaStoreValidation(HiveConf conf) {
        conf.setBoolVar(METASTORE_VALIDATE_CONSTRAINTS, true);
        conf.setBoolVar(METASTORE_VALIDATE_COLUMNS, true);
        conf.setBoolVar(METASTORE_VALIDATE_TABLES, true);
    }

    protected void configureJobTrackerMode(HiveConf conf) {
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

    protected void configureFileSystem(TemporaryFolder basedir, HiveConf conf) {
        conf.setVar(METASTORECONNECTURLKEY, metaStorageUrl + ";create=true");

        createAndSetFolderProperty(METASTOREWAREHOUSE, "warehouse", conf, basedir);
        createAndSetFolderProperty(SCRATCHDIR, "scratchdir", conf, basedir);
        createAndSetFolderProperty(LOCALSCRATCHDIR, "localscratchdir", conf, basedir);
        createAndSetFolderProperty(METASTOREDIRECTORY, "metastore", conf, basedir);
        createAndSetFolderProperty(HIVEHISTORYFILELOC, "tmp", conf, basedir);

        conf.setBoolVar(HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);

        createAndSetFolderProperty("hadoop.tmp.dir", "hadooptmp", conf, basedir);
        createAndSetFolderProperty("test.log.dir", "logs", conf, basedir);
        createAndSetFolderProperty("hive.vs", "vs", conf, basedir);
    }

    private File newFolder(TemporaryFolder basedir, String folder) {
        try {
            return basedir.newFolder(folder);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create tmp dir: " + e.getMessage(), e);
        }
    }

    private File newFile(TemporaryFolder basedir, String fileName) {
        try {
            return basedir.newFile(fileName);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create tmp file: " + e.getMessage(), e);
        }
    }

    protected void configureMapReduceOptimizations(HiveConf conf) {
        /*
        * Switch off all optimizers otherwise we didn't
        * manage to contain the map reduction within this JVM.
        */
        conf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
        conf.setBoolVar(HIVEMETADATAONLYQUERIES, false);
        conf.setBoolVar(HIVEOPTINDEXFILTER, false);
        conf.setBoolVar(HIVECONVERTJOIN, false);
        conf.setBoolVar(HIVESKEWJOIN, false);
    }

    @Override
    public String getMetaStoreUrl() {
        return metaStorageUrl;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    @Override
    public TemporaryFolder getBaseDir() {
        return basedir;
    }

    protected final void createAndSetFolderProperty(HiveConf.ConfVars var, String folder, HiveConf conf,
                                                    TemporaryFolder basedir) {
        conf.setVar(var, newFolder(basedir, folder).getAbsolutePath());
    }

    protected final void createAndSetFolderProperty(String key, String folder, HiveConf conf, TemporaryFolder basedir) {
        conf.set(key, newFolder(basedir, folder).getAbsolutePath());
    }


}
