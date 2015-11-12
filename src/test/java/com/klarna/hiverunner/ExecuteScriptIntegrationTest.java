package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class ExecuteScriptIntegrationTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @HiveSQL(files = {})
  private HiveShell hiveShell;

  @Test
  public void testInsertRowWithExecuteScript() throws IOException {
    File file = new File(temp.getRoot(), "insert_data.hql");

    try (PrintStream out = new PrintStream(file)) {
      out.println("create database test_db;");
      out.println("create table test_db.test_table (");
      out.println("  c0 string");
      out.println(")");
      out.println("stored as orc;");
      out.println("insert into table test_db.test_table values ('v1');");
    }

    hiveShell.execute(file);

    List<String> result = hiveShell.executeQuery("select c0 from test_db.test_table");

    assertThat(result.size(), is(1));
    assertThat(result.get(0), is("v1"));
  }
}
