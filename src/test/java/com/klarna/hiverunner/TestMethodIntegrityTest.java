package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class TestMethodIntegrityTest {

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell shell;

    @Test
    public void collisionCourseTestMethodOne() {
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data1.csv", "1\n2\n3");
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data2.csv", "4\n5");
        shell.addSetupScript("create database foo;");
        shell.addSetupScript("" +
                " CREATE table foo.bar(id int)" +
                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                " STORED AS TEXTFILE" +
                " LOCATION '${hiveconf:hadoop.tmp.dir}/foo/bar';");
        shell.start();
        List<String> actual = shell.executeQuery("select * from foo.bar");
        List<String> expected = Arrays.asList("1", "2", "3", "4", "5");
        Assert.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

    }

    @Test
    public void collisionCourseTestMethodTwo() {
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data1.csv", "9\n2\n8");
        shell.addResource("${hiveconf:hadoop.tmp.dir}/foo/bar/data3.csv", "6\n7");
        shell.addSetupScript("create database foo;");
        shell.addSetupScript("" +
                " CREATE table foo.bar(id int)" +
                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                " STORED AS TEXTFILE" +
                " LOCATION '${hiveconf:hadoop.tmp.dir}/foo/bar';");
        shell.start();
        List<String> actual = shell.executeQuery("select * from foo.bar");
        List<String> expected = Arrays.asList("2", "6", "7", "8", "9");
        Assert.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

    }

}
