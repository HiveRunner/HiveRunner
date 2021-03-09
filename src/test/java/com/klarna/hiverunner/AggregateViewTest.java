package com.klarna.hiverunner;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class AggregateViewTest {

  @HiveSQL(files = {"AggregateViewTest/create_table.sql"}, autoStart = true)
  protected HiveShell shell;

  @Before
  public void setUpTables(){
    //creating tables
    //shell.execute("CREATE DATABASE db;");

//    shell
//        .execute(new StringBuilder()
//            .append("CREATE EXTERNAL TABLE `db.mvtdescriptionchangeinfo`(")
//            .append("  `timestamp` bigint COMMENT '',")
//            .append("  `testid` string COMMENT '',")
//            .append("  `type` string COMMENT '',")
//            .append("  `contents` string COMMENT '',")
//            .append("  `hostname` string COMMENT '')")
//            .append("    PARTITIONED BY (")
//            .append("  `request_log_date` string,")
//            .append("  `request_log_hour` string)")
//            .toString());

  }

  @Test
  public void simpleView() {
//    shell.execute(new StringBuilder()
//        .append("CREATE VIEW db.latestnodemvtchanges AS ")
//        .append("SELECT testid, hostname, max(`timestamp`) AS mts ")
//        .append("FROM db.mvtdescriptionchangeinfo ")
//        .append("WHERE `timestamp` IS NOT NULL ")
//        .append("GROUP BY testid, hostname;").toString());
//
//    shell.execute(new StringBuilder()
//        .append("    CREATE VIEW db.latesttestchangepairs AS ")
//        .append("SELECT a.testid, a.type ")
//        .append("  FROM db.mvtdescriptionchangeinfo a ")
//        .append("INNER JOIN db.latestnodemvtchanges b ON a.testid = b.testid AND a.`timestamp` = b.mts ")
//        .append("    GROUP BY a.testid, a.type;").toString());

    shell.insertInto("db", "mvtdescriptionchangeinfo").addRow("123", "testname", "REMOVED", "contents of test...", "hostname", "6/21/17","20").commit();


    List<String> result1 = shell.executeQuery("SELECT * FROM db.mvtdescriptionchangeinfo");
    System.out.println("result1:"+ result1);
    List<String> result2 = shell.executeQuery("SELECT * FROM db.latestnodemvtchanges");
    System.out.println("result2:"+ result2);
    List<String> result3 = shell.executeQuery("SELECT * FROM db.latesttestchangepairs");
    System.out.println("result3:"+ result3);

  }



}
