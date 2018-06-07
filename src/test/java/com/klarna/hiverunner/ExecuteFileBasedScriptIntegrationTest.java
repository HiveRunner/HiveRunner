/**
 * Copyright (C) 2013-2018 Klarna AB
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

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class ExecuteFileBasedScriptIntegrationTest {

      @Rule
      public TemporaryFolder temp = new TemporaryFolder();

      @HiveSQL(files = {})
      private HiveShell hiveShell;

      @Test
      public void testExecuteFileBasedScript() throws IOException {
        File hqlScriptFile = temp.newFile("get_current_database.hql");

        try (PrintStream out = new PrintStream(hqlScriptFile)) {
          out.println("select current_database(), NULL, 100;");
        }

        hiveShell.execute(hqlScriptFile);

        Charset optionalCharset = UTF_8;
        List<String> results = hiveShell.executeQuery(optionalCharset, hqlScriptFile, " optional_column_delimiter ", "optional_null_replacement");
        
        assertThat(results.size(), is(1));
        assertThat(results.get(0), is("default optional_column_delimiter optional_null_replacement optional_column_delimiter 100"));
      }
}
