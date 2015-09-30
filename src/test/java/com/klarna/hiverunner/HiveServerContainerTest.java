package com.klarna.hiverunner;

import com.klarna.hiverunner.config.HiveRunnerConfig;
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
        StandaloneHiveServerContext context = new StandaloneHiveServerContext(basedir, new HiveRunnerConfig());
        context.init();
        container.init(new HashMap<String, String>(), context);
    }

    @After
    public void tearDown() {
        container.tearDown();
    }

    @Test
    public void testGetBasedir() {

        Assert.assertEquals(basedir.getRoot(), container.getBaseDir().getRoot());
    }

    @Test
    public void testExecuteStatementMR() {
        List<Object[]> actual = container.executeStatement("show databases");
        Assert.assertEquals(1, actual.size());
        Assert.assertArrayEquals(new Object[]{"default"}, actual.get(0));
    }

    @Test
    public void testExecuteStatementTez() {
        List<Object[]> actual = container.executeStatement("show databases");
        Assert.assertEquals(1, actual.size());
        Assert.assertArrayEquals(new Object[]{"default"}, actual.get(0));
    }

    @Test
    public void testTearDownShouldNotThrowException() {
        container.tearDown();
        container.tearDown();
        container.tearDown();
    }

    @Test(expected = HiveSQLException.class)
    public void testInvalidQuery() throws Throwable {
        try {
            container.executeStatement("use foo");
        } catch (IllegalArgumentException e) {
            throw e.getCause();
        }
    }
}