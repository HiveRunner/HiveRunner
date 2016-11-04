package com.klarna.hiverunner.examples;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.data.TsvFileParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/*
    This example is intended to show how to set HiveConf (or HiveVar) values in HIveRunner.

    HiveConf can be very useful. For instance you might have a global cutoff value that could be set outside your code
    and used in many places in your queries. Common example would be a threshold value or a cutoff timestamp.

    To use the HiveConf values in HiveRunner, you must first make sure to switch off the autoStart flag. Then you can
    set the HiveConf values and, before executing any queries, manually start the HIveRunner shell. Make sure this is
    done first in your test setup. like shown in the example below.
 */
@RunWith(StandaloneHiveRunner.class)
public class SetHiveConfValues {

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell shell;

    @Before
    public void setupDatabases() {
        shell.setHiveConfValue("cutoff", "50");
        shell.start();

        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.table_a (")
                .append("message STRING, value INT")
                .append(")")
                .toString());

        shell.insertInto("source_db", "table_a")
                .withAllColumns()
                .addRow("An ignored message", 1)
                .addRow("Hello", 51)
                .addRow("World", 99)
                .commit();
    }

    @Test
    public void useHiveConfValues() {
        final List<Object[]> result = shell.executeStatement("select message from source_db.table_a where value > ${hiveconf:cutoff}");

        for (Object[] row : result) {
            System.out.print(row[0] + " ");
        }
    }
}
