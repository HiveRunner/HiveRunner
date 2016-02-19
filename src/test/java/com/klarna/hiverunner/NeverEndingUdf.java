package com.klarna.hiverunner;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;


public class NeverEndingUdf extends UDF {

    private static final Logger LOGGER = LoggerFactory.getLogger(NeverEndingUdf.class);

    public Text evaluate(Text value) {
        LOGGER.warn("Entering infinite loop");
        while (true) {
            LOGGER.debug("Looping and generating random seed: {}",
                    new SecureRandom(value.copyBytes()).generateSeed(12332123));
        }
    }
}
