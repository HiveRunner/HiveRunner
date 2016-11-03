package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public abstract class AnnotatedBaseTestClass {
    @HiveSQL(files = {})
    protected HiveShell shell;

    @Before
    public void setup() {
        shell.execute("create database test_db");

        shell.execute(new StringBuilder()
                .append("create table test_db.test_table (")
                .append("c0 string")
                .append(")")
                .toString());

        shell.insertInto("test_db", "test_table")
                .addRow("v1")
                .commit();
    }
}
