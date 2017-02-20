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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.io.Resources;
import com.klarna.hiverunner.annotations.*;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import com.klarna.reflection.ReflectionUtils;
import org.apache.log4j.MDC;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private HiveRunnerConfig config = new HiveRunnerConfig();

    public StandaloneHiveRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }


    @Override
    protected List<TestRule> getTestRules(final Object target) {
        final TemporaryFolder testBaseDir = new TemporaryFolder();

        TestRule hiveRunnerRule = new TestRule() {
            @Override
            public Statement apply(final Statement base, Description description) {
                Statement statement = new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        evaluateStatement(target, testBaseDir, base);
                    }
                };
                return statement;
            }
        };

        /*
         *  Note that rules will be executed in reverse order to how they're added.
         */

        List<TestRule> rules = new ArrayList<>();
        rules.addAll(super.getTestRules(target));
        rules.add(hiveRunnerRule);
        rules.add(testBaseDir);
        rules.add(ThrowOnTimeout.create(config, getName()));

        /*
         Make sure hive runner config rule is the first rule on the list to be executed so that any subsequent
         statements has access to the final config.
          */
        rules.add(getHiveRunnerConfigRule(target));
        return rules;
    }

    @Override
    protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
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
    private void evaluateStatement(Object target, TemporaryFolder temporaryFolder, Statement base) throws Throwable {
        container = null;
        setAndCheckIfWritable(temporaryFolder);
        try {
            LOGGER.info("Setting up {} in {}", getName(), temporaryFolder.getRoot().getAbsolutePath());
            container = createHiveServerContainer(target, temporaryFolder);
            base.evaluate();
        } finally {
            tearDown();
        }
    }

    private void tearDown() {
        if (container != null) {
            LOGGER.info("Tearing down {}", getName());
            try {
                container.tearDown();
            } catch (Throwable e) {
                LOGGER.warn("Tear down failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Traverses the test case annotations. Will inject a HiveShell in the test case that envelopes the HiveServer.
     */
    private HiveShellContainer createHiveServerContainer(final Object testCase, TemporaryFolder baseDir)
            throws IOException {

        HiveServerContext context = new StandaloneHiveServerContext(baseDir, config);

        final HiveServerContainer hiveTestHarness = new HiveServerContainer(context);

        HiveShellBuilder hiveShellBuilder = new HiveShellBuilder();
        hiveShellBuilder.setCommandShellEmulation(config.getCommandShellEmulation());

        HiveShellField shellSetter = loadScriptUnderTest(testCase, hiveShellBuilder);

        hiveShellBuilder.setHiveServerContainer(hiveTestHarness);

        loadAnnotatedResources(testCase, hiveShellBuilder);

        loadAnnotatedProperties(testCase, hiveShellBuilder);

        loadAnnotatedSetupScripts(testCase, hiveShellBuilder);

        // Build shell
        final HiveShellContainer shell = hiveShellBuilder.buildShell();

        // Set shell
        shellSetter.setShell(shell);

        if (shellSetter.isAutoStart()) {
            shell.start();
        }

        return shell;
    }

    private HiveShellField loadScriptUnderTest(final Object testCaseInstance, HiveShellBuilder hiveShellBuilder) {
        try {
            Set<Field> fields = ReflectionUtils.getAllFields(
                    testCaseInstance.getClass(), withAnnotation(HiveSQL.class));

            Preconditions.checkState(fields.size() == 1, "Exact one field should to be annotated with @HiveSQL");

            final Field field = fields.iterator().next();
            List<Path> scripts = new ArrayList<>();
            HiveSQL annotation = field.getAnnotation(HiveSQL.class);
            for (String scriptFilePath : annotation.files()) {
                Path file = Paths.get(Resources.getResource(scriptFilePath).toURI());
                assertFileExists(file);
                scripts.add(file);
            }

            Charset charset = annotation.encoding().equals("") ?
                    Charset.defaultCharset() : Charset.forName(annotation.encoding());

            final boolean isAutoStart = annotation.autoStart();

            hiveShellBuilder.setScriptsUnderTest(scripts, charset);

            return new HiveShellField() {
                @Override
                public void setShell(HiveShell shell) {
                    ReflectionUtils.setField(testCaseInstance, field.getName(), shell);
                }

                @Override
                public boolean isAutoStart() {
                    return isAutoStart;
                }
            };
        } catch (Throwable t) {
            throw new IllegalArgumentException("Failed to init field annotated with @HiveSQL: " + t.getMessage(), t);
        }
    }

    private void assertFileExists(Path file) {
        Preconditions.checkState(Files.exists(file), "File " + file + " does not exist");
    }


    private void loadAnnotatedSetupScripts(Object testCase, HiveShellBuilder workFlowBuilder) {
        Set<Field> setupScriptFields = ReflectionUtils.getAllFields(testCase.getClass(),
                withAnnotation(HiveSetupScript.class));

        for (Field setupScriptField : setupScriptFields) {
            if (ReflectionUtils.isOfType(setupScriptField, String.class)) {
                String script = ReflectionUtils.getFieldValue(testCase, setupScriptField.getName(), String.class);
                workFlowBuilder.addSetupScript(script);
            } else if (ReflectionUtils.isOfType(setupScriptField, File.class) ||
                    ReflectionUtils.isOfType(setupScriptField, Path.class)) {
                Path path = getMandatoryPathFromField(testCase, setupScriptField);
                workFlowBuilder.addSetupScript(readAll(path));
            } else {
                throw new IllegalArgumentException(
                        "Field annotated with @HiveSetupScript currently only supports type String, File and Path");
            }
        }
    }

    private static String readAll(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read " + path + ": " + e.getMessage(), e);
        }
    }

    private void loadAnnotatedResources(Object testCase, HiveShellBuilder workFlowBuilder) throws IOException {
        Set<Field> fields = ReflectionUtils.getAllFields(testCase.getClass(), withAnnotation(HiveResource.class));

        for (Field resourceField : fields) {

            HiveResource annotation = resourceField.getAnnotation(HiveResource.class);
            String targetFile = annotation.targetFile();

            if (ReflectionUtils.isOfType(resourceField, String.class)) {
                String data = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), String.class);
                workFlowBuilder.addResource(targetFile, data);
            } else if (ReflectionUtils.isOfType(resourceField, File.class) ||
                    ReflectionUtils.isOfType(resourceField, Path.class)) {
                Path dataFile = getMandatoryPathFromField(testCase, resourceField);
                workFlowBuilder.addResource(targetFile, dataFile);
            } else {
                throw new IllegalArgumentException(
                        "Fields annotated with @HiveResource currently only supports field type String, File or Path");
            }
        }
    }

    private Path getMandatoryPathFromField(Object testCase, Field resourceField) {
        Path path;
        if (ReflectionUtils.isOfType(resourceField, File.class)) {
            File dataFile = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), File.class);
            path = Paths.get(dataFile.toURI());
        } else if (ReflectionUtils.isOfType(resourceField, Path.class)) {
            path = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), Path.class);
        } else {
            throw new IllegalArgumentException(
                    "Only Path or File type is allowed on annotated field " + resourceField);
        }

        Preconditions.checkArgument(Files.exists(path), "File %s does not exist", path);
        return path;
    }

    private void loadAnnotatedProperties(Object testCase, HiveShellBuilder workFlowBuilder) {
        for (Field hivePropertyField : ReflectionUtils.getAllFields(testCase.getClass(),
                withAnnotation(HiveProperties.class))) {
            Preconditions.checkState(ReflectionUtils.isOfType(hivePropertyField, Map.class),
                    "Field annotated with @HiveProperties should be of type Map<String, String>");
            workFlowBuilder.putAllProperties(
                    ReflectionUtils.getFieldValue(testCase, hivePropertyField.getName(), Map.class));
        }
    }

    private TestRule getHiveRunnerConfigRule(final Object target) {
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

    private void setAndCheckIfWritable(TemporaryFolder temporaryFolder) throws IOException {
        HiveFolder folder = new HiveFolder(temporaryFolder.getRoot(), container.getHiveConf());
        Assert.assertTrue(folder.markAsWritable());
    }

    /**
     * Used as a handle for the HiveShell field in the test case so that we may set it once the
     * HiveShell has been instantiated.
     */
    interface HiveShellField {
        void setShell(HiveShell shell);

        boolean isAutoStart();
    }
}