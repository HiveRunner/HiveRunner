package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class CompatibilityModeTest {

  @Test
  public void testFullLineCommentAndSetBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(CompatibilityMode.BEELINE.transform(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentBeeLine() {
    String hql = "-- hello";
    assertThat(CompatibilityMode.BEELINE.transform(hql), is(""));
  }

  @Test
  public void testFullLineCommentAndSetHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(CompatibilityMode.HIVE_CLI.transform(hql), is(hql));
  }

  @Test
  public void testFullLineCommentHiveCli() {
    String hql = "-- hello";
    assertThat(CompatibilityMode.HIVE_CLI.transform(hql), is(hql));
  }
}
