package com.klarna.hiverunner;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class HiveShellHiveCliCompatibilityModeTest {

  @HiveSQL(files = {}, encoding = "UTF-8", compatibilityMode = CompatibilityMode.HIVE_CLI)
  private HiveShell beeLineShell;

  /**
   * Retains the behaviour described in HIVE-8396.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testStripsFullLineComments() {
    String hql = "-- hello\n"
        + "set x=1;\n"
        + "set x;\n";
    beeLineShell.executeQuery(hql);
  }
  
}
