CREATE DATABASE testdb;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

CREATE EXTERNAL TABLE testdb.test 
(
  field1 string, 
  field2 string
)
STORED AS ORC;
