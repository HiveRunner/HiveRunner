/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) ${license.git.copyrightYears} The HiveRunner Contributors
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
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class ExecuteScriptIntegrationTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @HiveSQL(files = {})
  private HiveShell hiveShell;

  @Test
  public void testInsertRowWithExecuteScript() throws IOException {
    File file = new File(temp.getRoot(), "insert_data.hql");

    try (PrintStream out = new PrintStream(file)) {
      out.println("create database test_db;");
      out.println("create table test_db.test_table (");
      out.println("  c0 string");
      out.println(")");
      out.println("stored as orc;");
      out.println("insert into table test_db.test_table values ('v1');");
    }

    hiveShell.execute(file);

    List<String> result = hiveShell.executeQuery("select c0 from test_db.test_table");

    assertThat(result.size(), is(1));
    assertThat(result.get(0), is("v1"));
  }
}
