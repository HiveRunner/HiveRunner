package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class HiveShellHiveCliCompatibilityModeTest {

  @HiveSQL(files = {}, encoding = "UTF-8", compatibilityMode = CompatibilityMode.HIVE_CLI)
  private HiveShell hiveCliShell;

  /** Retains the behaviour described in HIVE-8396. */
  @Test(expected = IllegalArgumentException.class)
  public void testQueryStripFullLineCommentFirstLine() {
    hiveCliShell.executeQuery("-- a\nset x=1");
  }

  /** Hive CLI captures comment as value. */
  @Test
  public void testQueryStripFullLineCommentNested() {
    hiveCliShell.executeQuery("set x=\n-- a\n1");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=-- a", "1")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryStripFullLineComment() {
    hiveCliShell.executeQuery("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentFirstLine() {
    hiveCliShell.execute("-- a\nset x=1;");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineCommentLastLine() {
    hiveCliShell.execute("set x=1;\n-- a");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineComment() {
    hiveCliShell.execute("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentNested() {
    hiveCliShell.execute("set x=\n-- a\n1;");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

}
