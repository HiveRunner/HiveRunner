/*
 * Copyright 2015 Klarna AB
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

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;

@RunWith(StandaloneHiveRunner.class)
public class HiveShellHiveCliEmulationTest {

  @HiveRunnerSetup
  public final static HiveRunnerConfig CONFIG = new HiveRunnerConfig() {{
      setCommandShellEmulation(CommandShellEmulation.HIVE_CLI);
  }};
  
  @HiveSQL(files = {}, encoding = "UTF-8")
  private HiveShell hiveCliShell;

  /** Retains the behaviour described in HIVE-8396. */
  @Test(expected = IllegalArgumentException.class)
  public void testQueryStripFullLineCommentFirstLine() {
    hiveCliShell.executeQuery("-- a\nset x=1");
  }

  /** Hive CLI captures comment as value. */
  @Test
  public void testQueryStripFullLineCommentNested() {
    hiveCliShell.executeQuery("set x=\n-- a\n1");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=-- a", "1")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryStripFullLineComment() {
    hiveCliShell.executeQuery("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentFirstLine() {
    hiveCliShell.execute("-- a\nset x=1;");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineCommentLastLine() {
    hiveCliShell.execute("set x=1;\n-- a");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineComment() {
    hiveCliShell.execute("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentNested() {
    hiveCliShell.execute("set x=\n-- a\n1;");
    List<String> results = hiveCliShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

}
