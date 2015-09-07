package com.klarna.hiverunner;

import org.apache.hive.service.cli.HiveSQLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.List;

public class HiveServerContainerTest {


    @Rule
    public TemporaryFolder basedir = new TemporaryFolder();
    private HiveServerContainer container;


    @Before
    public void setup() {
        container = new HiveServerContainer();
    }

    @After
    public void tearDown() {
        container.tearDown();
    }

    @Test
    public void testGetBasedir() {
        container.init(new HashMap<String, String>(), new MapReduceStandaloneHiveServerContext(basedir));
        Assert.assertEquals(basedir.getRoot(), container.getBaseDir().getRoot());
    }

    @Test
    public void testExecuteStatementMR() {
        container.init(new HashMap<String, String>(), new MapReduceStandaloneHiveServerContext(basedir));
        List<Object[]> actual = container.executeStatement("show databases");
        Assert.assertEquals(1, actual.size());
        Assert.assertArrayEquals(new Object[]{"default"}, actual.get(0));
    }

    @Test
    public void testExecuteStatementTez() {
        container.init(new HashMap<String, String>(), new TezStandaloneHiveServerContext(basedir));
        List<Object[]> actual = container.executeStatement("show databases");
        Assert.assertEquals(1, actual.size());
        Assert.assertArrayEquals(new Object[]{"default"}, actual.get(0));
    }

    @Test
    public void testTearDownShouldNotThrowException() {
        container.init(new HashMap<String, String>(), new TezStandaloneHiveServerContext(basedir));
        container.tearDown();
        container.tearDown();
        container.tearDown();
    }

    @Test(expected = HiveSQLException.class)
    public void testInvalidQuery() throws Throwable {
        container.init(new HashMap<String, String>(), new MapReduceStandaloneHiveServerContext(basedir));
        try {
            container.executeStatement("use foo");
        } catch (IllegalArgumentException e) {
            throw e.getCause();
        }
    }
}