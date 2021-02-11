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

        shell.execute(new StringBuilder()
                .append("create table test_db.tableA (")
                .append("id int, ")
                .append("value string")
                .append(")")
                .toString());

        shell.execute(new StringBuilder()
                .append("create table test_db.tableB (")
                .append("id int, ")
                .append("value string")
                .append(")")
                .toString());

        shell.insertInto("test_db", "tableA")
                .addRow(1, "v1")
                .addRow(2, "v2")
                .commit();
        shell.insertInto("test_db", "tableB")
                .addRow(1, "v3")
                .addRow(2, "v4")
                .commit();

        // Using alias names is fine
        shell.execute(new StringBuilder()
                .append("create view test_db.test_view1 ")
                .append("as select 1 from test_db.tableA a ")
                .append("join test_db.tableB b ")
                .append("on a.id = b.id;")
                .toString());

        // Using all lowercase is fine
        shell.execute(new StringBuilder()
                .append("create view test_db.test_view2 ")
                .append("as select 1 from test_db.tablea ")
                .append("join test_db.tableb ")
                .append("on tablea.id = tableb.id;")
                .toString());

        shell.executeStatement("select * from test_db.test_view1");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void createViewError() {
   // Using mixed case in create VIEW statement (with a JOIN ON construction) is not OK
      shell.execute(new StringBuilder()
          .append("create view test_db.test_view3 ")
          .append("as select 1 from test_db.tableA ")
          .append("join test_db.tableB ")
          .append("on tableA.id = tableB.id;")
          .toString());
      
    }


}