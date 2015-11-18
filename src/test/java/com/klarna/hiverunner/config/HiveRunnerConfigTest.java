package com.klarna.hiverunner.config;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

import com.klarna.hiverunner.CompatibilityMode;

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
    public void testDefaultHiveExecutionEngine() {
        HiveRunnerConfig config = new HiveRunnerConfig();
        Assert.assertEquals("mr", config.getHiveExecutionEngine());
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
    public void testCompatibilityMode() {
      Properties sysProps = new Properties();
      sysProps.put(HiveRunnerConfig.COMPATIBILITY_MODE_PROPERTY_NAME, "BEELINE");
      HiveRunnerConfig config = new HiveRunnerConfig(new Properties(sysProps));
      Assert.assertEquals(CompatibilityMode.BEELINE, config.getCompatibilityMode());

      sysProps.put(HiveRunnerConfig.COMPATIBILITY_MODE_PROPERTY_NAME, "beeline");
      config = new HiveRunnerConfig(new Properties(sysProps));
      Assert.assertEquals(CompatibilityMode.BEELINE, config.getCompatibilityMode());

      sysProps.put(HiveRunnerConfig.COMPATIBILITY_MODE_PROPERTY_NAME, "BeElInE");
      config = new HiveRunnerConfig(new Properties(sysProps));
      Assert.assertEquals(CompatibilityMode.BEELINE, config.getCompatibilityMode());
    }

    @Test
    public void testSetCompatibilityMode() {
      HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
      config.setCompatibilityMode(CompatibilityMode.HIVE_CLI);
      Assert.assertEquals(CompatibilityMode.HIVE_CLI, config.getCompatibilityMode());
      config.setCompatibilityMode(CompatibilityMode.BEELINE);
      Assert.assertEquals(CompatibilityMode.BEELINE, config.getCompatibilityMode());
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
    public void testCompatibilityModeDefault() {
        HiveRunnerConfig config = new HiveRunnerConfig(new Properties());
        Assert.assertEquals(CompatibilityMode.HIVE_CLI, CompatibilityMode.valueOf(HiveRunnerConfig.COMPATIBILITY_MODE_DEFAULT));
        Assert.assertEquals(CompatibilityMode.HIVE_CLI, config.getCompatibilityMode());
    }

}