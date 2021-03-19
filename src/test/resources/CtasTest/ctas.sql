CREATE EXTERNAL TABLE foo (s1 string, s2 string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '${hiveconf:hadoop.tmp.dir}/foo/';


CREATE TABLE foo_prim as select * from foo;


