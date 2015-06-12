package com.klarna.hiverunner;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.security.SecureRandom;


public class NeverEndingUdf extends UDF {

    public Text evaluate(Text value) {
        while (true) {
            System.out.println(new SecureRandom(value.copyBytes()).generateSeed(12332123));
        }
    }
}
