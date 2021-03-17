package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class HiveRunnerExtensionTest {

  @HiveSQL(files = {"HiveRunnerExtensionTest/test_query.sql"})
  protected HiveShell shell;

  @Test
  public void shellFindFiles(){
    shell.insertInto("testdb", "test_table").addRow("v1", "v2").commit();
    List<String> actual = shell.executeQuery("select * from testdb.test_table");
    List<String> expected = Arrays.asList("v1\tv2");
    assertThat(actual,is(expected));
  }

}
