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

    shell
        .execute(new StringBuilder()
            .append("create table test_db.tableC (")
            .append("id int, ")
            .append("value string")
            .append(")")
            .toString());

    shell.insertInto("test_db", "tableA").addRow(1, "v1").addRow(2, "V2:MiXedCases").commit();
    shell.insertInto("test_db", "tableB").addRow(1, "v1").addRow(2, "V2:MiXedCases").commit();
  }

//  @Test
//  public void viewAlias() {
//    shell
//        .execute(new StringBuilder()
//            .append("create view test_db.test_view1 ")
//            .append("as select a.value as value1, b.value as value2 from test_db.tableA a ")
//            .append("join test_db.tableB b ")
//            .append("on a.id = b.id AND a.id = b.id;")
//            .toString());
//
//    List<String> result = shell.executeQuery("select * from test_db.test_view1");
//    List<String> expected = Arrays.asList("v1\tv3", "v2\tV4");
//    assertThat(expected, is(result));
//  }
//
//  @Test
//  public void viewLowercase() {
//    shell
//        .execute(new StringBuilder()
//            .append("create view test_db.test_view2 ")
//            .append("as select tablea.value as value2, tableb.value as value1 from test_db.tablea ")
//            .append("join test_db.tableb ")
//            .append("on tablea.id = tableb.id;")
//            .toString());
//
//    List<String> result = shell.executeQuery("select * from test_db.test_view2");
//    List<String> expected = Arrays.asList("v1\tv3", "v2\tV4");
//    assertThat(expected, is(result));
//  }

  @Test
  public void viewMixedCases5() {

    // Using mixed case in create VIEW statement (with a JOIN ON construction)
    //tAbleA.value ='v2'
    shell
        .execute(new StringBuilder()
            .append("CREAte viEW tEst_Db.test_ViEw3 ")
            .append("as select taBlEA.vAlue aS vaLue2, taBleb.vaLue as value1 from tesT_db.TABLEA ")
            .append("joIN teSt_db.TABLEB ")
            .append("On    tablea.id     =    tableb.id WHERE tablea.value ='V2:MiXedCases'")
            .toString());


    List<String> result = shell.executeQuery("select * from test_db.test_view3");
    List<String> expected = Arrays.asList("V2:MiXedCases\tV2:MiXedCases");
    System.out.println("result: "+result);
    System.out.println("expected :"+expected);
    assertThat(expected, is(result));
  }

  @Test
  public void viewMixedCases15() {

    // Using mixed case in create VIEW statement (with a JOIN ON construction)
    //tAbleA.value ='v2'
    shell
        .execute(new StringBuilder()
            .append("CREAte viEW tEst_Db.test_ViEw3 ")
            .append("as select taBlEA.vAlue aS vaLue2, taBleb.vaLue as value1 from tesT_db.TABLEA ")
            .append("joIN teSt_db.TABLEB ")
            .append("On    tAblea.id     =    taBleb.id    WHERE (tabLea.value ='V2:MiXedCases' and Tablea.value ='V2:MiXedCases' anD Tablea.value='V2:MiXedCases') ")
            .toString());

    List<String> result = shell.executeQuery("select * from test_db.test_view3");
    List<String> expected = Arrays.asList("V2:MiXedCases\tV2:MiXedCases");
    System.out.println("result: "+result);
    System.out.println("expected :"+expected);
    assertThat(expected, is(result));
  }

//  @Test
//  public void viewMixedCases3() {
//
//    // Using mixed case in create VIEW statement (with a JOIN ON construction)
//    //tAbleA.value ='v2'
//    shell
//        .execute(new StringBuilder()
//            .append("CREAte viEW tEst_Db.test_ViEw3 ")
//            .append("as select taBlEA.vAlue aS vaLue2 from tesT_db.tABleA ")
//            .append(" ")
//            .append("WHERE tAblea.value ='V2'")
//            .toString());
//
//    List<String> result = shell.executeQuery("select * from test_db.test_view3");
//    List<String> expected = Arrays.asList("V2");
//    System.out.println("result: "+result);
//    System.out.println("expected :"+expected);
//    assertThat(expected, is(result));
//  }

//  @Test
//  public void viewMixedCases2() {
//
//    // Using mixed case in create VIEW statement (with a JOIN ON construction)
//    //tAbleA.value ='v2'
//    shell
//        .execute(new StringBuilder()
//            .append("CREAte viEW tEst_Db.test_ViEw3 ")
//            .append("as (select taBlEA.vAlue aS vaLue2, taBleb.vaLue as value1 from tesT_db.tABleA ")
//            .append("joIN teSt_db.TABLEB ")
//            .append("On    tAbleA.Id     =    TabLeB.id    WHERE tAbleA.value ='V2')")
//            .append("UNION (select taBlEA.vAlue aS vaLue3, taBleb.vaLue as value4 from tesT_db.tABleA ")
//            .append("joIN teSt_db.TABLEB ")
//            .append("On tAbleA.Id = TabLeB.id WHERE tAbleA.value ='V2');")
//            .toString());
//
//    List<String> result = shell.executeQuery("select * from test_db.test_view3");
//    List<String> expected = Arrays.asList("V2\tv2");
//    System.out.println("result: "+result);
//    System.out.println("expected :"+expected);
//    assertThat(expected, is(result));
//  }

}

//This one doesn't create an error even thought it has a view and a join (or could uit be because the lowercae method is working)
//    shell
//        .execute(new StringBuilder()
//        .append("CREAte viEW tEst_Db.test_ViEw3 ")
//        .append("as select taBlEA.vAlue aS vaLue2 from tesT_db.TABLEA ")
//
//        .append(" joIN teSt_db.TABLEB ")
//        .append("On    tAbleA.Id     =    TabLeB.id   ")
//        .toString());