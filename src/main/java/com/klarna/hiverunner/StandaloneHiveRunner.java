/**
 * Copyright (C) 2013-2021 Klarna AB
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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.klarna.hiverunner.annotations.*;
import com.klarna.hiverunner.builder.Script;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import com.klarna.reflection.ReflectionUtils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.reflections.ReflectionUtils.withAnnotation;
import static org.reflections.ReflectionUtils.withType;

/**
 * JUnit 4 runner that runs hive sql on a HiveServer residing in this JVM. No external dependencies needed.
 */
public class StandaloneHiveRunner extends BlockJUnit4ClassRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneHiveRunner.class);

    private HiveShellContainer container;

    /**
     * We need to init config because we're going to pass
     * it around before it is actually fully loaded from the testcase.
     */
    private final HiveRunnerConfig config = new HiveRunnerConfig();

    public StandaloneHiveRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
        initializeConfig();
    }

    /**
     * Set some properties which make good defaults for test scenarios. These can be overridden
     * in the individual HiveShell instances though, if needed.
     */
    private void initializeConfig() {
        /**
         * If hive.in.test=false (default), Hive 3 will assume that the metastore rdbms has already been initialized
         * with some basic tables and will try to run initial test queries against them.
         * This results in multiple warning stacktraces if the rdbms has not actually been initialized.
         */
        config.getHiveConfSystemOverride().put(HIVE_IN_TEST.getVarname(), "true");
    }

    protected HiveRunnerConfig getHiveRunnerConfig() {
      return config;
    }

    @Override
    protected List<TestRule> getTestRules(Object target) {
        Path testBaseDir = null;
        try {
            testBaseDir = Files.createTempDirectory("hiverunner_tests");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        HiveRunnerRule hiveRunnerRule = new HiveRunnerRule(this, target, testBaseDir);

        /*
         * Note that rules will be executed in reverse order to how they're added.
         */

        List<TestRule> rules = new ArrayList<>();
        rules.addAll(super.getTestRules(target));
        rules.add(hiveRunnerRule);
        rules.add(ThrowOnTimeout.create(config, getName()));

        /*
         Make sure hive runner config rule is the first rule on the list to be executed so that any subsequent
         statements has access to the final config.
          */
        rules.add(getHiveRunnerConfigRule(target));
        return rules;
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        if (method.getAnnotation(Ignore.class) != null) {
            notifier.fireTestIgnored(description);
        } else {
            setLogContext(method);
            EachTestNotifier eachNotifier = new EachTestNotifier(notifier, description);
            eachNotifier.fireTestStarted();
            try {
                runTestMethod(method, eachNotifier, config.getTimeoutRetries());
            } finally {
                eachNotifier.fireTestFinished();
                clearLogContext();
            }
        }
    }

    /**
     * Runs a {@link Statement} that represents a leaf (aka atomic) test.
     */
    protected final void runTestMethod(FrameworkMethod method,
        EachTestNotifier notifier, int retriesLeft) {

        Statement statement = methodBlock(method);

        try {
            statement.evaluate();
        } catch (AssumptionViolatedException e) {
            notifier.addFailedAssumption(e);
        } catch (TimeoutException e) {
            /*
             TimeoutException thrown by ThrowOnTimeout statement. Handling is kept in this class since this is where the
             retry needs to be triggered in order to get the right tear down and test setup between retries.
              */
            if (--retriesLeft >= 0) {
                LOGGER.warn(
                        "Test case timed out. Will attempt retry {} more times. Turn on log level DEBUG for stacktrace",
                        retriesLeft);
                LOGGER.debug(e.getMessage(), e);
                tearDown();
                runTestMethod(method, notifier, retriesLeft);
            } else {
                notifier.addFailure(e);
            }
        } catch (Throwable e) {
            notifier.addFailure(e);
        }
    }

    /**
     * Drives the unit test.
     */
    public HiveShellContainer evaluateStatement(List<? extends Script> scripts, Object target,
        Path temporaryFolder, Statement base) throws Throwable {
        if(scripts==null){
            scripts = new ArrayList<>();
        }
        container = null;
        File temporaryFile = temporaryFolder.toFile();
        if (!temporaryFile.exists()) {
            temporaryFile.mkdirs();
        }
        FileUtil.setPermission(temporaryFile, FsPermission.getDirDefault());
        try {
            LOGGER.info("Setting up {} in {}", getName(), temporaryFolder.getRoot());
            container = createHiveServerContainer(scripts, target, temporaryFolder);
            base.evaluate();
            return container;
        } finally {
            tearDown();
        }
    }

    private void tearDown(){
        tearDownContainer();
        if (container != null) {
          deleteTempFolder(container.getBaseDir());
        }
    }

    private void tearDownContainer(){
        if (container != null) {
            LOGGER.info("Tearing down {}", getName());
            try {
                container.tearDown();
            } catch (Throwable e) {
                LOGGER.warn("Tear down failed: " + e.getMessage(), e);
            }
        }
    }

    private void deleteTempFolder(Path directory) {
        try {
            FileUtils.deleteDirectory(directory.toFile());
        } catch (IOException e) {
          LOGGER.debug("Temporary folder was not deleted successfully: " + directory);
        }
    }

    /**
     * Traverses the test case annotations. Will inject a HiveShell in the test case that envelopes the HiveServer.
     */
    private HiveShellContainer createHiveServerContainer(List<? extends Script> scripts, Object testCase,
        Path baseDir)
        throws IOException {
        HiveRunnerCore core = new HiveRunnerCore();
        return core.createHiveServerContainer(scripts, testCase, baseDir, config);
    }

    private TestRule getHiveRunnerConfigRule(Object target) {
        return new TestRule() {
            @Override
            public Statement apply(Statement base, Description description) {
                Set<Field> fields = ReflectionUtils.getAllFields(target.getClass(),
                        Predicates.and(
                                withAnnotation(HiveRunnerSetup.class),
                                withType(HiveRunnerConfig.class)));

                Preconditions.checkState(fields.size() <= 1,
                        "Exact one field of type HiveRunnerConfig should to be annotated with @HiveRunnerSetup");

                /*
                 Override the config with test case config. Taking care to not replace the config instance since it
                  has been passes around and referenced by some of the other test rules.
                  */
                if (!fields.isEmpty()) {
                    config.override(ReflectionUtils
                            .getFieldValue(target, fields.iterator().next().getName(), HiveRunnerConfig.class));
                }
                return base;
            }
        };
    }

    private void clearLogContext() {
        MDC.clear();
    }

    private void setLogContext(FrameworkMethod method) {
        MDC.put("testClassShort", getTestClass().getJavaClass().getSimpleName());
        MDC.put("testClass", getTestClass().getJavaClass().getName());
        MDC.put("testMethod", method.getName());
    }
}
