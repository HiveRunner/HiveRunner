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

  /** Failure described in HIVE-8396 should be avoided for beeline. */
  @Test
  public void testQueryStripFullLineCommentFirstLine() {
    beeLineShell.executeQuery("-- a\nset x=1");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  /** Beeline strips comment before assignment. */
  @Test
  public void testQueryStripFullLineCommentNested() {
    beeLineShell.executeQuery("set x=\n-- a\n1");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryStripFullLineComment() {
    beeLineShell.executeQuery("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentFirstLine() {
    beeLineShell.execute("-- a\nset x=1;");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineCommentLastLine() {
    beeLineShell.execute("set x=1;\n-- a");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineComment() {
    beeLineShell.execute("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentNested() {
    beeLineShell.execute("set x=\n-- a\n1;");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

}
