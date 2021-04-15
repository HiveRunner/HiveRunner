/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class AggregateViewTest {

  @HiveSQL(files = {})
  private HiveShell shell;

  /**
   * Adding unit test to check that issue#70 (https://github.com/klarna/HiveRunner/issues/70) doesn't happen anymore.
   * This bug is solved when upgrading HiveRunner to version 4.0.0 or above (most likely due to move from Hive 1.x to 2.x).
   */
  @Test
  public void aggregateView() {
    this.shell.execute(Paths.get("src/test/resources/aggregateViewTest/create_table.sql"));
    shell.insertInto("db", "mvtdescriptionchangeinfo").addRow("123", "testname", "REMOVED", "contents of test...", "hostname", "6/21/17","20").commit();
    List<String> result = shell.executeQuery("SELECT * FROM db.latesttestchangepairs");
    List<String> expected = Arrays.asList("testname\tREMOVED");
    assertThat(result,is(expected));
  }

}
