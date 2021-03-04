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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

    shell
        .execute(new StringBuilder()
            .append("create table test_db.tableC (")
            .append("id int, ")
            .append("value string")
            .append(")")
            .toString());

    shell.insertInto("test_db", "tableA").addRow(1, "v1").addRow(2, "V2:MiXedCases").commit();
    shell.insertInto("test_db", "tableB").addRow(1, "v3").addRow(2, "V4:MiXedCases").commit();
  }

  @Test
  public void viewJoin() {
    shell
        .execute(new StringBuilder()
            .append("CREAte viEW tEst_Db.test_ViEw ")
            .append("as select taBlEA.vAlue aS vaLue2, taBleb.vaLue as value1 from tesT_db.TABLEA ")
            .append("joIN teSt_db.TABLEB ")
            .append("On    tablEA.id     =    TAbleb.id ")
            .toString());


    List<String> result = shell.executeQuery("select * from test_db.test_view");
    List<String> expected = Arrays.asList("v1\tv3","V2:MiXedCases\tV4:MiXedCases");
    System.out.println("result: "+result);
    System.out.println("expected :"+expected);
    assertThat(expected, is(result));
  }

  @Test
  public void viewJoinCaseSensitiveString() {

    shell
        .execute(new StringBuilder()
            .append("CREAte viEW tEst_Db.test_ViEw ")
            .append("as select taBlEA.vAlue aS vaLue2, taBleb.vaLue as value1 from tesT_db.TABLEA ")
            .append("joIN teSt_db.TABLEB ")
            .append("On    tablEA.id     =    TAbleb.id WHERE tablea.value ='V2:MiXedCases';")
            .toString());


    List<String> result = shell.executeQuery("select * from test_db.test_view");
    List<String> expected = Arrays.asList("V2:MiXedCases\tV4:MiXedCases");
    System.out.println("result: "+result);
    System.out.println("expected :"+expected);
    assertThat(expected, is(result));
  }

  @Test
  public void viewJoinCaseMultipleSensitiveStrings() {

    shell
        .execute(new StringBuilder()
            .append("CREAte viEW tEst_Db.test_ViEw ")
            .append("as select taBlEA.vAlue aS vaLue2, taBleb.vaLue as value1 from tesT_db.TABLEA ")
            .append("joIN teSt_db.TABLEB ")
            .append("On    tAblea.id     =    taBleb.id    WHERE (tabLea.value ='V2:MiXedCases' and Tablea.value ='V2:MiXedCases' anD Tablea.value='V2:MiXedCases') ;")
            .toString());

    List<String> result = shell.executeQuery("select * from test_db.test_view");
    List<String> expected = Arrays.asList("V2:MiXedCases\tV4:MiXedCases");
    System.out.println("result: "+result);
    System.out.println("expected :"+expected);
    assertThat(expected, is(result));
  }

}