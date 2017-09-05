/*
 * Copyright 2015 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import java.io.File;

public class CommandShellEmulationTest {

  @Test
  public void testFullLineCommentAndSetStatementBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(CommandShellEmulation.BEELINE.transformStatement(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentAndSetStatementHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(CommandShellEmulation.HIVE_CLI.transformStatement(hql), is(hql));
  }

  @Test
  public void testFullLineCommentStatementBeeLine() {
    String hql = "-- hello";
    assertThat(CommandShellEmulation.BEELINE.transformStatement(hql), is(""));
  }

  @Test
  public void testFullLineCommentStatementHiveCli() {
    String hql = "-- hello";
    assertThat(CommandShellEmulation.HIVE_CLI.transformStatement(hql), is(hql));
  }

  @Test
  public void testFullLineCommentAndSetScriptBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(CommandShellEmulation.BEELINE.transformScript(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentAndSetScriptHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(CommandShellEmulation.HIVE_CLI.transformScript(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentScriptBeeLine() {
    String hql = "-- hello";
    assertThat(CommandShellEmulation.BEELINE.transformScript(hql), is(""));
  }

  @Test
  public void testFullLineCommentScriptHiveCli() {
    String hql = "-- hello";
    assertThat(CommandShellEmulation.HIVE_CLI.transformScript(hql), is(""));
  }

  @Test
  public void hiveCliEmulationSupportsImportingScriptFiles() {
    assertThat(CommandShellEmulation.HIVE_CLI.isImportFileStatement("source script.hql"), is(true));
    assertThat(CommandShellEmulation.HIVE_CLI.isImportFileStatement("   source script.hql  "), is(true));
    assertThat(CommandShellEmulation.HIVE_CLI.isImportFileStatement("SOURCE script.hql"), is(true));

    assertThat(CommandShellEmulation.HIVE_CLI.isImportFileStatement("!run script.hql"), is(false));
    assertThat(CommandShellEmulation.HIVE_CLI.isImportFileStatement("select * from table"), is(false));
  }

  @Test
  public void hiveCliEmulationReturnsScriptFileToImport() {
    File expected = new File("script.hql");

    assertThat(CommandShellEmulation.HIVE_CLI.getImportFileFromStatement("source script.hql"), is(expected));
    assertThat(CommandShellEmulation.HIVE_CLI.getImportFileFromStatement("   source   script.hql   "), is(expected));
  }

  @Test
  public void hiveCliEmulationReturnsFileWithoutNameWhenImportStatementIsMalformed() {
    File actual = CommandShellEmulation.HIVE_CLI.getImportFileFromStatement("source");
    assertThat(actual.getName(), isEmptyString());
  }

  @Test
  public void beeLineEmulationSupportsImportingScriptFiles() {
    assertThat(CommandShellEmulation.BEELINE.isImportFileStatement("!run script.hql"), is(true));
    assertThat(CommandShellEmulation.BEELINE.isImportFileStatement("   !run script.hql   "), is(true));

    assertThat(CommandShellEmulation.BEELINE.isImportFileStatement("!RUN script.hql"), is(false));
    assertThat(CommandShellEmulation.BEELINE.isImportFileStatement("source script.hql"), is(false));
    assertThat(CommandShellEmulation.BEELINE.isImportFileStatement("select * from table"), is(false));
  }

  @Test
  public void beeLineEmulationReturnsScriptFileToImport() {
    File expected = new File("script.hql");

    assertThat(CommandShellEmulation.BEELINE.getImportFileFromStatement("!run script.hql"), is(expected));
    assertThat(CommandShellEmulation.BEELINE.getImportFileFromStatement("   !run script.hql   "), is(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentExceptionIsThrownWhenBeeLineEmulationCannotGetImportFileFromStatement() {
    CommandShellEmulation.BEELINE.getImportFileFromStatement("!run script.hql another_script.hql");
  }

}
