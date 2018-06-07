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
package com.klarna.hiverunner.sql.cli;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Test;

import com.klarna.hiverunner.sql.cli.beeline.BeelineEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

public class CommandShellEmulationTest {

  @Test
  public void testFullLineCommentAndSetStatementBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(BeelineEmulator.INSTANCE.preProcessor().statement(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentAndSetStatementHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(HiveCliEmulator.INSTANCE.preProcessor().statement(hql), is(hql));
  }

  @Test
  public void testFullLineCommentStatementBeeLine() {
    String hql = "-- hello";
    assertThat(BeelineEmulator.INSTANCE.preProcessor().statement(hql), is(""));
  }

  @Test
  public void testFullLineCommentStatementHiveCli() {
    String hql = "-- hello";
    assertThat(HiveCliEmulator.INSTANCE.preProcessor().statement(hql), is(hql));
  }

  @Test
  public void testFullLineCommentAndSetScriptBeeLine() {
    String hql = "-- hello\nset x=1;";
    assertThat(BeelineEmulator.INSTANCE.preProcessor().script(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentAndSetScriptHiveCli() {
    String hql = "-- hello\nset x=1;";
    assertThat(HiveCliEmulator.INSTANCE.preProcessor().script(hql), is("set x=1;"));
  }

  @Test
  public void testFullLineCommentScriptBeeLine() {
    String hql = "-- hello";
    assertThat(BeelineEmulator.INSTANCE.preProcessor().script(hql), is(""));
  }

  @Test
  public void testFullLineCommentScriptHiveCli() {
    String hql = "-- hello";
    assertThat(HiveCliEmulator.INSTANCE.preProcessor().script(hql), is(""));
  }
//
//  @Test
//  public void hiveCliEmulationSupportsImportingScriptFiles() {
//    assertThat(HiveCliEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("source script.hql")), is(true));
//    assertThat(HiveCliEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("   source script.hql  ")), is(true));
//    assertThat(HiveCliEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("SOURCE script.hql")), is(true));
//
//    assertThat(HiveCliEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("!run script.hql")), is(false));
//    assertThat(HiveCliEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("select * from table")), is(false));
//  }
//
//  @Test
//  public void hiveCliEmulationReturnsScriptFileToImport() {
//    File expected = new File("script.hql");
//
//    assertThat(HiveCliEmulator.INSTANCE.getImportFileFromStatement(new HiveSqlStatement("source script.hql")), is(expected));
//    assertThat(HiveCliEmulator.INSTANCE.getImportFileFromStatement(new HiveSqlStatement("   source   script.hql   ")), is(expected));
//  }
//
//  @Test
//  public void hiveCliEmulationReturnsFileWithoutNameWhenImportStatementIsMalformed() {
//    File actual = HiveCliEmulator.INSTANCE.getImportFileFromStatement(new HiveSqlStatement("source"));
//    assertThat(actual.getName(), isEmptyString());
//  }
//
//  @Test
//  public void beeLineEmulationSupportsImportingScriptFiles() {
//    assertThat(BeelineEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("!run script.hql")), is(true));
//    assertThat(BeelineEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("   !run script.hql   ")), is(true));
//
//    assertThat(BeelineEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("!RUN script.hql")), is(false));
//    assertThat(BeelineEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("source script.hql")), is(false));
//    assertThat(BeelineEmulator.INSTANCE.isImportFileStatement(new HiveSqlStatement("select * from table")), is(false));
//  }
//
//  @Test
//  public void beeLineEmulationReturnsScriptFileToImport() {
//    File expected = new File("script.hql");
//
//    assertThat(BeelineEmulator.INSTANCE.getImportFileFromStatement(new HiveSqlStatement("!run script.hql")), is(expected));
//    assertThat(BeelineEmulator.INSTANCE.getImportFileFromStatement(new HiveSqlStatement("   !run script.hql   ")), is(expected));
//  }
//
//  @Test(expected = IllegalArgumentException.class)
//  public void illegalArgumentExceptionIsThrownWhenBeeLineEmulationCannotGetImportFileFromStatement() {
//    BeelineEmulator.INSTANCE.getImportFileFromStatement(new HiveSqlStatement("!run script.hql another_script.hql"));
//  }

}
