/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
