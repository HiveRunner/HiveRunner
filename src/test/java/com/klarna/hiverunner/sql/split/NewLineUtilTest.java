package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.klarna.hiverunner.sql.split.NewLineUtil.removeLeadingTrailingNewLines;

import org.junit.Test;

public class NewLineUtilTest {

  @Test
  public void typical() {
    assertThat(removeLeadingTrailingNewLines(""), is(""));
    assertThat(removeLeadingTrailingNewLines(" "), is(" "));
    assertThat(removeLeadingTrailingNewLines(" a "), is(" a "));
    assertThat(removeLeadingTrailingNewLines("\n"), is(""));
    assertThat(removeLeadingTrailingNewLines(" \n "), is(""));
    assertThat(removeLeadingTrailingNewLines(" \n \n "), is(""));
    assertThat(removeLeadingTrailingNewLines("\n a \n"), is(" a "));
    assertThat(removeLeadingTrailingNewLines(" \n a b \n "), is(" a b "));
  }
}
