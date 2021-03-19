create database my_schema;

CREATE EXTERNAL TABLE my_schema.result (year STRING, value INT)
  stored as sequencefile
;