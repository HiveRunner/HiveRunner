CREATE TABLE serde_test (
  key STRING,
  value STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES  (
"input.regex" = "([0-9]*)#([A-Z]*).*"
)
STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/serde';