package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class AggregateViewTest {

  @HiveSQL(files = {"AggregateViewTest/create_table.sql"}, autoStart = true)
  protected HiveShell shell;

  /**
   * Adding unit test to check that issue#70 (https://github.com/klarna/HiveRunner/issues/70) doesn't happen anymore.
   * This bug is solved when upgrading HiveRunner to any version above 3.2.1 (at least 4.0.0).
   */
  @Test
  public void aggregateView() {
    shell.insertInto("db", "mvtdescriptionchangeinfo").addRow("123", "testname", "REMOVED", "contents of test...", "hostname", "6/21/17","20").commit();
    List<String> result = shell.executeQuery("SELECT * FROM db.latesttestchangepairs");
    List<String> expected = Arrays.asList("testname\tREMOVED");
    assertThat(result,is(expected));
  }

}
