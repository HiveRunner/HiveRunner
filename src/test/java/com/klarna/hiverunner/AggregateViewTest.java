package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class AggregateViewTest {

  @HiveSQL(files = {"AggregateViewTest/create_table.sql"}, autoStart = true)
  protected HiveShell shell;

  @Test
  public void aggregateView() {

    shell.insertInto("db", "mvtdescriptionchangeinfo").addRow("123", "testname", "REMOVED", "contents of test...", "hostname", "6/21/17","20").commit();
    List<String> result = shell.executeQuery("SELECT * FROM db.latesttestchangepairs");
    List<String> expected = Arrays.asList("testname\tREMOVED");
    assertThat(result,is(expected));

  }

}
