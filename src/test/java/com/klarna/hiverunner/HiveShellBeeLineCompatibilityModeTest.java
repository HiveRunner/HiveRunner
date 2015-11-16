package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class HiveShellBeeLineCompatibilityModeTest {

  @HiveSQL(files = {}, encoding = "UTF-8", compatibilityMode = CompatibilityMode.BEELINE)
  private HiveShell beeLineShell;

  /**
   * Failure described in HIVE-8396 should be avoided for beeline.
   */
  @Test
  public void testStripsFullLineComments() {
    beeLineShell.execute("create database test_db");
    beeLineShell.execute("create table test_db.test_table (c1 string) stored as textfile");
    beeLineShell.insertInto("test_db", "test_table").addRow("v1").commit();

    String hql = "-- hello\n"
        + "select * \n"
        + "-- ignored\n"
        + "from test_db.test_table";
    List<String> results = beeLineShell.executeQuery(hql);
    assertThat(results, is(Arrays.asList("v1")));
  }
  
}
