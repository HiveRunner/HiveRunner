CREATE EXTERNAL TABLE foo (s1 string, s2 string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '${hiveconf:hadoop.tmp.dir}/foo/';

SET hive.default.fileformat.managed=ORC;
SET hive.default.fileformat=ORC;

CREATE TABLE foo_orc_nocomp as select * from foo;

SET hive.exec.orc.default.compress=SNAPPY;

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

SET mapreduce.map.output.compress=true;

SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

CREATE TABLE foo_orc_snappy as select * from foo;
