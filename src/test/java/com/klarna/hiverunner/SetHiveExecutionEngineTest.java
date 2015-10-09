package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class SetHiveExecutionEngineTest {

    @HiveRunnerSetup
    public HiveRunnerConfig config = new HiveRunnerConfig(){{
        setHiveExecutionEngine("tez");
    }};

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell hiveShell;

    @Test
    public void test() {
        hiveShell.start();
        Assert.assertEquals("tez", hiveShell.getHiveConf().getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
    }

}
