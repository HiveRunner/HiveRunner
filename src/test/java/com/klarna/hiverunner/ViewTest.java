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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.util.Arrays;
import java.util.List;

import com.klarna.hiverunner.annotations.HiveSQL;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class ViewTest {
  @HiveSQL(files = {})
  protected HiveShell shell;

  @Before
  public void setUpTables(){
    //creating tables
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
  }

  @Test
  public void testing() {
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view0 ")
            .append("as select a.value as value1, b.value as value2 from test_db.tableA a ")
            .append("join test_db.tableB b ")
            .append("on a.id = b.id;")
            .toString());

    List<String> result = shell.executeQuery("select * from test_db.test_view0");
    List<String> expected = Arrays.asList("v1\tv3", "v2\tv4");
    assertThat(expected, is(result));
  }

  @Test
  public void createView() {

    // Using alias names
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view1 ")
            .append("as select a.value from test_db.tableA a ")
            .append("join test_db.tableB b ")
            .append("on a.id = b.id;")
            .toString());

    // Using all lowercase
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view2 ")
            .append("as select 2 from test_db.tablea ")
            .append("join test_db.tableb ")
            .append("on tablea.id = tableb.id;")
            .toString());

    List<String> result = shell.executeQuery("select * from test_db.test_view1");
    System.out.println("result2:"+result);
    List<String> result2 = shell.executeQuery("select * from test_db.test_view2");
    System.out.println("result3:"+result2);
  }

  @Test
  public void createViewMixedCases() {

    // Using mixed case in create VIEW statement (with a JOIN ON construction)
    shell
        .execute(new StringBuilder()
            .append("create view test_db.test_view3 ")
            .append("as select 1 from test_db.tableA ")
            .append("join test_db.tableB ")
            .append("on tableA.id = tableB.id;")
            .toString());

    // Even more mixed cases
    shell
        .execute(new StringBuilder()
            .append("CREAte view test_db.test_View4 ")
            .append("as select 2 from test_db.tABleA ")
            .append("join test_db.TABLEB ")
            .append("on tAbleA.id = tabLeB.id;")
            .toString());

    shell.executeStatement("select * from test_db.test_view3");
    shell.executeStatement("select * from test_db.test_view4");
  }

  @Test
  public void createViewMoreMixedCases() {

    shell
        .execute(new StringBuilder()
            .append("CREAte viEW tEst_Db.test_ViEw5 ")
            .append("as select 1 from tesT_db.tABleA ")
            .append("joIN teSt_db.TABLEB ")
            .append("On tAbleA.Id = TabLeB.id;")
            .toString());

    shell.executeStatement("select * from test_db.test_view5");
  }

}
