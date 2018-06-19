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
    assertThat(removeLeadingTrailingNewLines("a"), is("a"));
    assertThat(removeLeadingTrailingNewLines(" a"), is(" a"));
    assertThat(removeLeadingTrailingNewLines("a "), is("a "));
    assertThat(removeLeadingTrailingNewLines("\n"), is(""));
    assertThat(removeLeadingTrailingNewLines(" \n "), is(""));
    assertThat(removeLeadingTrailingNewLines(" \n \n "), is(""));
    assertThat(removeLeadingTrailingNewLines("\n a \n"), is(" a "));
    assertThat(removeLeadingTrailingNewLines(" \n a b \n "), is(" a b "));
    assertThat(removeLeadingTrailingNewLines(" \n \n a b \n \n "), is(" a b "));
    assertThat(removeLeadingTrailingNewLines(" \n a b \n "), is(" a b "));
    assertThat(removeLeadingTrailingNewLines(""), is(""));
    assertThat(removeLeadingTrailingNewLines("\t"), is("\t"));
    assertThat(removeLeadingTrailingNewLines("\ta\t"), is("\ta\t"));
    assertThat(removeLeadingTrailingNewLines("a"), is("a"));
    assertThat(removeLeadingTrailingNewLines("\ta"), is("\ta"));
    assertThat(removeLeadingTrailingNewLines("a\t"), is("a\t"));
    assertThat(removeLeadingTrailingNewLines("\n"), is(""));
    assertThat(removeLeadingTrailingNewLines("\t\n\t"), is(""));
    assertThat(removeLeadingTrailingNewLines("\t\n\t\n\t"), is(""));
    assertThat(removeLeadingTrailingNewLines("\n\ta\t\n"), is("\ta\t"));
    assertThat(removeLeadingTrailingNewLines("\t\n\ta\tb\t\n\t"), is("\ta\tb\t"));
    assertThat(removeLeadingTrailingNewLines("\t\n\t\n\ta\tb\t\n\t\n\t"), is("\ta\tb\t"));
  }
}
