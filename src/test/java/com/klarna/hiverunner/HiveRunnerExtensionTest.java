/**
 * Copyright (C) 2013-2021 Klarna AB
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
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class HiveRunnerExtensionTest {

  @HiveSQL(files = { "HiveRunnerExtensionTest/test_query.sql" })
  private HiveShell shell;

  @Test
  public void shellFindFiles(){
    shell.insertInto("testdb", "test_table").addRow("v1", "v2").commit();
    List<String> actual = shell.executeQuery("select * from testdb.test_table");
    List<String> expected = Arrays.asList("v1\tv2");
    assertThat(actual,is(expected));
  }

}
