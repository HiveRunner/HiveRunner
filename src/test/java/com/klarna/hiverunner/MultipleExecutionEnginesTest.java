package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;

@RunWith(StandaloneHiveRunner.class)
public class MultipleExecutionEnginesTest {

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell shell;


    @Test
    public void test() throws IOException {
        shell.getResourceOutputStream("${hiveconf:hadoop.tmp.dir}/foo/data.txt").write("a,b,c\nd,e,f".getBytes());
        shell.addSetupScript(
                "create external table foo (s1 string, s2 string, s3 string) " +
                        "ROW FORMAT DELIMITED " +
                        "FIELDS TERMINATED BY ',' " +
                        "LOCATION '${hiveconf:hadoop.tmp.dir}/foo/'");
        shell.start();

        Assert.assertEquals(Arrays.asList("a\tb\tc", "d\te\tf"), shell.executeQuery("select * from foo"));

        shell.execute("set hive.execution.engine=tez");
        Assert.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));

        shell.execute("set hive.execution.engine=mr");
        Assert.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));

        shell.execute("set hive.execution.engine=tez");
        Assert.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));

        shell.execute("set hive.execution.engine=mr");
        Assert.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));


    }


}
