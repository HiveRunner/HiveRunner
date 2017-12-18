package com.klarna.hiverunner.examples;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * A basic Hive Runner example showing how to setup the test source database and target database, execute the query
 * and then validate the result.
 *
 * In this example we want to test some very simple code, calculate_max.sql, that calculate a max value by year.
 *
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner and have a reference to HiveShell.
 */
@RunWith(StandaloneHiveRunner.class)
public class HelloHiveRunner {
    @HiveSQL(files = {})
    private HiveShell shell;

    @Before
    public void setupSourceDatabase() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table (")
                .append("year STRING, value INT")
                .append(")")
                .toString());
    }

    @Before
    public void setupTargetDatabase() {
        shell.execute(Paths.get("src/test/resources/helloHiveRunner/create_max.sql"));
    }

    @Test
    public void testMaxValueByYear() {
        /*
         * Insert some source data
         */
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        /*
         * Execute the query
         */
        shell.execute(Paths.get("src/test/resources/helloHiveRunner/calculate_max.sql"));

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("select * from my_schema.result");

        assertEquals(2, result.size());
        assertArrayEquals(new Object[]{"2014", 4}, result.get(0));
        assertArrayEquals(new Object[]{"2015", 5}, result.get(1));
    }
}
