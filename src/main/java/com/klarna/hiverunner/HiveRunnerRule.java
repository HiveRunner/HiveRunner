/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) ${license.git.copyrightYears} The HiveRunner Contributors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner;

import com.klarna.hiverunner.builder.Script;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A rule that executes the scripts under test
 */
public class HiveRunnerRule implements TestRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveRunnerRule.class);
    private final StandaloneHiveRunner runner;
    private final Object target;
    private final Path testBaseDir;
    private List<? extends Script> scriptsUnderTest = new ArrayList<>();

    HiveRunnerRule(StandaloneHiveRunner runner, Object target, Path testBaseDir) {
        this.runner = runner;
        this.target = target;
        this.testBaseDir = testBaseDir;
    }

    public List<? extends Script> getScriptsUnderTest() {
        return scriptsUnderTest;
    }

    public void setScriptsUnderTest(List<? extends Script> scriptsUnderTest) {
        LOGGER.debug("Setting up hive runner scripts under test");
        this.scriptsUnderTest = scriptsUnderTest;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        LOGGER.debug("Running hive runner rule apply");
        return new HiveRunnerRuleStatement(runner, target, base, testBaseDir);
    }

    class HiveRunnerRuleStatement extends Statement {

        private final Object target;
        private final Statement base;
        private final Path testBaseDir;
        private final StandaloneHiveRunner runner;

        private HiveRunnerRuleStatement(
                StandaloneHiveRunner runner,
                Object target,
                Statement base,
                Path testBaseDir) {
            this.runner = runner;
            this.target = target;
            this.base = base;
            this.testBaseDir = testBaseDir;
        }

        @Override
        public void evaluate() throws Throwable {
            LOGGER.debug("Hive runner rule evaluate method");
            HiveShellContainer container = runner.evaluateStatement(scriptsUnderTest, target, testBaseDir, base);

            /**
             * Script list will initially be null. 'evaluateStatement' sets up the script list.
             * Need to set the value here to allow for mutation inside the mutantSwarmRule.
             */
            scriptsUnderTest = container.getScriptsUnderTest();
        }
    }
}
