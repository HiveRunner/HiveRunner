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
import com.google.common.io.Resources;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.reflection.ReflectionUtils;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.reflections.ReflectionUtils.withAnnotation;

/**
 * JUnit 4 runner that runs hive sql on a HiveServer residing in this JVM. No external dependencies needed.
 */
public class StandaloneHiveRunner extends BlockJUnit4ClassRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneHiveRunner.class);
    private static final String HIVE_EXECUTION_ENGINE_PROPERTY_NAME = "hiveExecutionEngine";
    private static final String MAP_REDUCE="mr";
    private static final String TEZ="tez";

    private int retries = 2;
    private int timeoutSeconds = 30;
    private boolean timeoutEnabled = false;

    private HiveShellContainer container;

    public StandaloneHiveRunner(Class<?> clazz) throws InitializationError {
        super(clazz);

        String enableTimeoutProperty = "enableTimeout";
        String enableTimeout = System.getProperty(enableTimeoutProperty);
        timeoutEnabled = enableTimeout == null ? timeoutEnabled : Boolean.parseBoolean(enableTimeout);

        String timeoutRetriesProperty = "timeoutRetries";
        String timeoutRetries = System.getProperty(timeoutRetriesProperty);
        retries = timeoutRetries == null ? retries : Integer.parseInt(timeoutRetries);

        String timeoutsSecondsProperty = "timeoutSeconds";
        String timeoutSecondsStr = System.getProperty(timeoutsSecondsProperty);
        timeoutSeconds = timeoutSecondsStr == null ? timeoutSeconds : Integer.parseInt(timeoutSecondsStr);

        if (timeoutEnabled) {
            LOGGER.warn(String.format(
                    "Timeout enabled. Setting timeout to %ss and retries to %s. Configurable via system properties " +
                            "'%s' and '%s'",
                    timeoutSeconds, retries, timeoutRetriesProperty, timeoutsSecondsProperty));
        } else {
            LOGGER.warn("Timeout disabled.");
        }
    }

    /**
     * Override this to provide another context.
     */
    protected HiveServerContext getContext(TemporaryFolder basedir) {
        String executionEngine = System.getProperty(HIVE_EXECUTION_ENGINE_PROPERTY_NAME, MAP_REDUCE);
        switch(executionEngine) {
            case MAP_REDUCE:
                return new MapReduceStandaloneHiveServerContext(basedir);
            case TEZ:
                return new TezStandaloneHiveServerContext(basedir);
            default:
                throw new IllegalArgumentException("Invalid value of system property '"+HIVE_EXECUTION_ENGINE_PROPERTY_NAME+"': '" + executionEngine + "'. Only '"+MAP_REDUCE+"' and '"+TEZ+"' are allowed");
        }
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

        List<TestRule> rules = new ArrayList<TestRule>();
        rules.addAll(super.getTestRules(target));
        rules.add(hiveRunnerRule);
        rules.add(testBaseDir);
        if (timeoutEnabled) {
            rules.add(getTimeoutRule(timeoutSeconds * 1000, getName()));
        }
        return rules;
    }

    private TestRule getTimeoutRule(final int timeoutMillis, final Object target) {
        return new TestRule() {
            @Override
            public Statement apply(Statement base, Description description) {
                return new ThrowOnTimeout(base, timeoutMillis, target);
            }
        };
    }

    @Override
    protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        if (method.getAnnotation(Ignore.class) != null) {
            notifier.fireTestIgnored(description);
        } else {
            EachTestNotifier eachNotifier = new EachTestNotifier(notifier, description);
            eachNotifier.fireTestStarted();
            try {
                runTestMethod(method, eachNotifier, retries);
            } finally {
                eachNotifier.fireTestFinished();
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
            if (--retriesLeft >= 0) {
                LOGGER.warn(String.format("%s : %s. Will attempt retry %s more times.",
                        getName(), e.getMessage(), retriesLeft), e);
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
        Assert.assertTrue(temporaryFolder.getRoot().setWritable(true, false));
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

        final HiveServerContainer hiveTestHarness =
                new HiveServerContainer();

        HiveShellBuilder hiveShellBuilder = new HiveShellBuilder();

        HiveShellField shellSetter = loadScriptUnderTest(testCase, hiveShellBuilder);

        HiveServerContext context = getContext(baseDir);

        hiveShellBuilder.setContext(context);

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
            List<Path> scripts = new ArrayList<Path>();
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
            if (isStringField(setupScriptField)) {
                String script = getMandatoryStringField(testCase, setupScriptField);
                workFlowBuilder.addSetupScript(script);
            } else if (isFileField(setupScriptField) || isPathField(setupScriptField)) {
                Path path = getMandatoryPathFromField(testCase, setupScriptField);
                workFlowBuilder.addSetupScript(readAll(path));
            } else {
                throw new IllegalArgumentException(
                        "Field annotated with @HiveSetupScript currently only supports type String, File and Path");
            }
        }
    }

    private String readAll(Path path) {
        try {
            return new String(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read " + path + ": " + e.getMessage(), e);
        }
    }

    private void loadAnnotatedResources(Object testCase, HiveShellBuilder workFlowBuilder) throws IOException {
        Set<Field> fields = ReflectionUtils.getAllFields(testCase.getClass(), withAnnotation(HiveResource.class));

        for (Field resourceField : fields) {

            HiveResource annotation = resourceField.getAnnotation(HiveResource.class);
            String targetFile = annotation.targetFile();

            if (isStringField(resourceField)) {
                String data = getMandatoryStringField(testCase, resourceField);
                workFlowBuilder.addResource(targetFile, data);
            } else if (isFileField(resourceField) || isPathField(resourceField)) {
                Path dataFile = getMandatoryPathFromField(testCase, resourceField);
                workFlowBuilder.addResource(targetFile, dataFile);
            } else {
                throw new IllegalArgumentException(
                        "Fields annotated with @HiveResource currently only supports field type String, File or Path");
            }
        }
    }

    private String getMandatoryStringField(Object testCase, Field resourceField) {
        return ReflectionUtils.getFieldValue(testCase, resourceField.getName(), String.class);
    }

    private Path getMandatoryPathFromField(Object testCase, Field resourceField) {
        Path path;
        if (isFileField(resourceField)) {
            File dataFile = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), File.class);
            path = Paths.get(dataFile.toURI());
        } else if (isPathField(resourceField)) {
            path = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), Path.class);
        } else {
            throw new IllegalArgumentException(
                    "Only Path or File type is allowed on annotated field " + resourceField);
        }

        Preconditions.checkArgument(Files.exists(path), "File %s does not exist", path);
        return path;
    }

    private boolean isPathField(Field resourceField) {
        return resourceField.getType().isAssignableFrom(Path.class);
    }

    private void loadAnnotatedProperties(Object testCase, HiveShellBuilder workFlowBuilder) {
        for (Field hivePropertyField : ReflectionUtils.getAllFields(testCase.getClass(),
                withAnnotation(HiveProperties.class))) {
            Preconditions.checkState(isMapField(hivePropertyField),
                    "Field annotated with @HiveProperties should be of type Map<String, String>");
            workFlowBuilder.putAllProperties(
                    ReflectionUtils.getFieldValue(testCase, hivePropertyField.getName(), Map.class));
        }
    }

    private boolean isStringField(Field setupScriptField) {
        return setupScriptField.getType().isAssignableFrom(String.class);
    }

    private boolean isFileField(Field resourceField) {
        return resourceField.getType().isAssignableFrom(File.class);
    }

    private boolean isMapField(Field hivePropertyField) {
        return hivePropertyField.getType().isAssignableFrom(Map.class);
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




