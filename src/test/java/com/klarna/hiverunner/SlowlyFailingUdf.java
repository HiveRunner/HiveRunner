package com.klarna.hiverunner;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SlowlyFailingUdf extends UDF {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlowlyFailingUdf.class);



    public Text evaluate(Text value) throws InterruptedException {
        /**
         * Sleep a little while so that the timeout thread will have time to take the synchronize lock
         */
        Thread.sleep(1000);
        // Fail!
        throw new RuntimeException("FAIL");
    }
}
