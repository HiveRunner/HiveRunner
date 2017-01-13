CREATE EXTERNAL TABLE foo (s1 string, s2 string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '${hiveconf:hadoop.tmp.dir}/foo/';

SET hive.default.fileformat.managed=ORC;

CREATE TABLE foo_orc_nocomp as select * from foo;

SET hive.exec.orc.default.compress=SNAPPY;

CREATE TABLE foo_orc_snappy as select * from foo;
