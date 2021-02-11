/**
 * Copyright (C) 2013-2021 Klarna AB
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

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class ViewTest {
  @HiveSQL(files = {})
  protected HiveShell shell;

  @Test
  public void createView() {
    shell.execute("create database test_db");

    shell
        .execute(new StringBuilder()
            .append("create table test_db.tableA (")
            .append("id int, ")
            .append("value string")
            .append(")")
            .toString());

    shell
        .execute(new StringBuilder()
            .append("create table test_db.tableB (")
            .append("id int, ")
            .append("value string")
            .append(")")
            .toString());

    shell.insertInto("test_db", "tableA").addRow(1, "v1").addRow(2, "v2").commit();
    shell.insertInto("test_db", "tableB").addRow(1, "v3").addRow(2, "v4").commit();

    // Using alias names is fine
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view1 ")
            .append("as select 1 from test_db.tableA a ")
            .append("join test_db.tableB b ")
            .append("on a.id = b.id;")
            .toString());

    // Using all lowercase is fine
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view2 ")
            .append("as select 1 from test_db.tablea ")
            .append("join test_db.tableb ")
            .append("on tablea.id = tableb.id;")
            .toString());

    shell.executeStatement("select * from test_db.test_view1");
  }

  // (expected = IllegalArgumentException.class)
  @Test(expected = IllegalArgumentException.class)

  public void createViewMixedCases() {
    shell.execute("create database test_db");

    shell
        .execute(new StringBuilder()
            .append("create table test_db.tableA (")
            .append("id int, ")
            .append("value string")
            .append(")")
            .toString());

    shell
        .execute(new StringBuilder()
            .append("create table test_db.tableB (")
            .append("id int, ")
            .append("value string")
            .append(")")
            .toString());

    shell.insertInto("test_db", "tableA").addRow(1, "v1").addRow(2, "v2").commit();
    shell.insertInto("test_db", "tableB").addRow(1, "v3").addRow(2, "v4").commit();

    // Using mixed case in create VIEW statement (with a JOIN ON construction) is not OK
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view3 ")
            .append("as select 1 from test_db.tableA ")
            .append("join test_db.tableB ")
            .append("on tableA.id = tableB.id;")
            .toString());

  }

}