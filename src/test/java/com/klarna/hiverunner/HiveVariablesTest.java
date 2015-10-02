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

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class HiveVariablesTest {

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell shell;

    @Test
    public void substitutedVariablesShouldBeExpanded() {
        shell.setHiveConfValue("origin", "spanish");
        shell.start();

        Assert.assertEquals("The spanish fox", shell.expandVariableSubstitutes("The ${hiveconf:origin} fox"));
    }

    @Test
    public void nestedSubstitutesShouldBeExpanded() {
        shell.setHiveVarValue("origin", "${hiveconf:origin2}");
        shell.setHiveConfValue("origin2", "spanish");
        shell.setHiveConfValue("animal", "fox");
        shell.setHiveConfValue("origin_animal", "${hivevar:origin} ${hiveconf:animal}");
        shell.setHiveConfValue("substitute", "origin_animal");
        shell.start();

        Assert.assertEquals("The spanish fox",
                shell.expandVariableSubstitutes("The ${hiveconf:${hiveconf:substitute}}"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void nestedSubstitutesShouldBeExpandedUsingDeprecatedSetProperty() {
        shell.setHiveVarValue("origin", "${hiveconf:origin2}");
        shell.setProperty("origin2", "spanish");
        shell.setProperty("animal", "fox");
        shell.setProperty("origin_animal", "${hivevar:origin} ${hiveconf:animal}");
        shell.setProperty("substitute", "origin_animal");
        shell.start();

        Assert.assertEquals("The spanish fox",
                shell.expandVariableSubstitutes("The ${hiveconf:${hiveconf:substitute}}"));
    }

    @Test
    public void unexpandableSubstitutesShouldNotBeExpanded() {
        shell.setHiveConfValue("origin", "spanish");
        shell.start();
        Assert.assertEquals("The spanish ${hiveconf:animal}",
                shell.expandVariableSubstitutes("The ${hiveconf:origin} ${hiveconf:animal}"));
    }

    @Test
    public void testHiveVarCli() {
        shell.addSetupScript("set hivevar:foobar=fox");
        shell.start();
        Assert.assertEquals("fox love fox", shell.expandVariableSubstitutes("${hivevar:foobar} love ${foobar}"));
    }

    @Test
    public void testHiveVar() {
        shell.setHiveVarValue("foobar", "fox");
        shell.start();
        Assert.assertEquals("fox love fox", shell.expandVariableSubstitutes("${hivevar:foobar} love ${foobar}"));
    }

    @Test
    public void testSystemVar() {
        System.setProperty("foo", "dog");
        System.setProperty("bar", "nice");
        shell.start();
        shell.execute("Create database ${system:bar}${system:foo}");
        Assert.assertEquals("nice dog", shell.expandVariableSubstitutes("${system:bar} ${system:foo}"));
    }
}
