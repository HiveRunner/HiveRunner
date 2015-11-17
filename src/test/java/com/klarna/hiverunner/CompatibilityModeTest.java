package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class CompatibilityModeTest {

  @Test
  public void testFullLineCommentAndSetStatementBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(CompatibilityMode.BEELINE.transformStatement(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentAndSetStatementHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(CompatibilityMode.HIVE_CLI.transformStatement(hql), is(hql));
  }

  @Test
  public void testFullLineCommentStatementBeeLine() {
    String hql = "-- hello";
    assertThat(CompatibilityMode.BEELINE.transformStatement(hql), is(""));
  }

  @Test
  public void testFullLineCommentStatementHiveCli() {
    String hql = "-- hello";
    assertThat(CompatibilityMode.HIVE_CLI.transformStatement(hql), is(hql));
  }

  @Test
  public void testFullLineCommentAndSetScriptBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(CompatibilityMode.BEELINE.transformScript(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentAndSetScriptHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(CompatibilityMode.HIVE_CLI.transformScript(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentScriptBeeLine() {
    String hql = "-- hello";
    assertThat(CompatibilityMode.BEELINE.transformScript(hql), is(""));
  }

  @Test
  public void testFullLineCommentScriptHiveCli() {
    String hql = "-- hello";
    assertThat(CompatibilityMode.HIVE_CLI.transformScript(hql), is(""));
  }
  
}
