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

import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@ExtendWith(HiveRunnerExtension.class)
public class SerdeTest {

    private final String hdfsSource = "${hiveconf:hadoop.tmp.dir}/serde";

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/customSerde/data.csv")
    private String data1 = "" +
            "a,b,c\n" +
            "f,g,h\n" +
            "t,j,k\n" +
            "q,w,e\n" +
            "r,t,y\n" +
            "u,i,o";


    @HiveSQL(files = {"SerdeTest/create_table.sql", "SerdeTest/hql_custom_serde.sql"}, autoStart = false)
    private HiveShell hiveShell;

    @Test
    public void testWithProvidedRegexSerde() {
        hiveShell.addResource(hdfsSource + "/data.csv", "123#FOO");
        hiveShell.start();
        Assertions.assertEquals(Arrays.asList("123\tFOO"), hiveShell.executeQuery("SELECT * FROM serde_test"));
    }

    @Test
    public void testWithCustomSerde() throws TException, IOException {
        hiveShell.start();
        List<String> actual = hiveShell.executeQuery(String.format("select * from customSerdeTable"));
        List<String> expected = Arrays.asList(
                "Q\tW\tE",
                "R\tT\tY",
                "U\tI\tO",
                "A\tB\tC",
                "F\tG\tH",
                "T\tJ\tK");

        Collections.sort(actual);
        Collections.sort(expected);

        Assertions.assertEquals(expected, actual);
    }


}

