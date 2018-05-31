/**
 * Copyright (C) 2013-2018 Klarna AB
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
package com.klarna.hiverunner.config;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

import com.klarna.hiverunner.CommandShellEmulation;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HiveRunnerConfigTest {

    @Test
    public void testSetHiveconfFromSystemProperty() {

        Properties sysProps = new Properties();
        sysProps.put("hiveconf_foo.bar", "false");
        sysProps.put("hiveconf_fox.love", "1000");

        Map<String, String> expected = new HashMap<>();
        expected.put("foo.bar", "false");
        expected.put("fox.love", "1000");

        HiveRunnerConfig config = new HiveRunnerConfig(sysProps);

        Assert.assertEquals(expected, config.getHiveConfSystemOverride());
    }

    @Test
    public void testSetHiveExecutionEngine() {
        Properties sysProps = new Properties();
        sysProps.put("hiveconf_" + HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "foo");
        HiveRunnerConfig config = new HiveRunnerConfig(sysProps);
        Assert.assertEquals("foo", config.getHiveExecutionEngine());
    }

    @Test
    public void testEnableTimeout() {
        Properties sysProps = new Properties();
        sysProps.put(HiveRunnerConfig.ENABLE_TIMEOUT_PROPERTY_NAME,
                String.valueOf(!HiveRunnerConfig.ENABLE_TIMEOUT_DEFAULT));
        HiveRunnerConfig config = new HiveRunnerConfig(sysProps);
        Assert.assertEquals(!HiveRunnerConfig.ENABLE_TIMEOUT_DEFAULT, config.isTimeoutEnabled());
    }

    @Test
    public void testTimeoutSeconds() {
        Properties sysProps = new Properties();
        sysProps.put(HiveRunnerConfig.TIMEOUT_SECONDS_PROPERTY_NAME, "567");
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties(sysProps));
        Assert.assertEquals(567, config.getTimeoutSeconds());
    }

    @Test
    public void testTimeoutRetries() {
        Properties sysProps = new Properties();
        sysProps.put(HiveRunnerConfig.TIMEOUT_RETRIES_PROPERTY_NAME, "678");
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties(sysProps));
        Assert.assertEquals(678, config.getTimeoutRetries());
    }

    @Test
    public void testCommandShellEmulation() {
      Properties sysProps = new Properties();
      sysProps.put(HiveRunnerConfig.COMMAND_SHELL_EMULATION_PROPERTY_NAME, "BEELINE");
      HiveRunnerConfig config = new HiveRunnerConfig(new Properties(sysProps));
      Assert.assertEquals(CommandShellEmulation.BEELINE, config.getCommandShellEmulation());

      sysProps.put(HiveRunnerConfig.COMMAND_SHELL_EMULATION_PROPERTY_NAME, "beeline");
      config = new HiveRunnerConfig(new Properties(sysProps));
      Assert.assertEquals(CommandShellEmulation.BEELINE, config.getCommandShellEmulation());

      sysProps.put(HiveRunnerConfig.COMMAND_SHELL_EMULATION_PROPERTY_NAME, "BeElInE");
      config = new HiveRunnerConfig(new Properties(sysProps));
      Assert.assertEquals(CommandShellEmulation.BEELINE, config.getCommandShellEmulation());
    }

    @Test
    public void testSetCommandShellEmulation() {
      HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
      config.setCommandShellEmulation(CommandShellEmulation.HIVE_CLI);
      Assert.assertEquals(CommandShellEmulation.HIVE_CLI, config.getCommandShellEmulation());
      config.setCommandShellEmulation(CommandShellEmulation.BEELINE);
      Assert.assertEquals(CommandShellEmulation.BEELINE, config.getCommandShellEmulation());
    }

    @Test
    public void testEnableTimeoutDefault() {
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
        Assert.assertEquals(HiveRunnerConfig.ENABLE_TIMEOUT_DEFAULT, config.isTimeoutEnabled());
    }

    @Test
    public void testTimeoutSecondsDefault() {
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
        Assert.assertEquals(HiveRunnerConfig.TIMEOUT_SECONDS_DEFAULT, config.getTimeoutSeconds());
    }

    @Test
    public void testTimeoutRetriesDefault() {
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
        Assert.assertEquals(HiveRunnerConfig.TIMEOUT_RETRIES_DEFAULT, config.getTimeoutRetries());
    }
    
    @Test
    public void testCommandShellEmulationDefault() {
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
        Assert.assertEquals(CommandShellEmulation.HIVE_CLI, CommandShellEmulation.valueOf(HiveRunnerConfig.COMMAND_SHELL_EMULATION_DEFAULT));
        Assert.assertEquals(CommandShellEmulation.HIVE_CLI, config.getCommandShellEmulation());
    }

}