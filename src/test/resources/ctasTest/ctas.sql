CREATE EXTERNAL TABLE foo (s1 string, s2 string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '${hiveconf:hive.vs}/foo/';


CREATE TABLE foo_prim as select * from foo;


