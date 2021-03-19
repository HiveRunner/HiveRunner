CREATE EXTERNAL TABLE customSerdeTable (s1 string, s2 string, s3 string)
    ROW FORMAT SERDE 'com.klarna.hiverunner.ToUpperCaseSerDe'
        WITH SERDEPROPERTIES (
            "key"="value",
            "KEY"= "VALUE"
        )
        STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/customSerde';




