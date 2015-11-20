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
public class HiveShellBeeLineEmulationTest {

  @HiveRunnerSetup
  public final static HiveRunnerConfig CONFIG = new HiveRunnerConfig() {{
      setCommandShellEmulation(CommandShellEmulation.BEELINE);
  }};
  
  @HiveSQL(files = {}, encoding = "UTF-8")
  private HiveShell beeLineShell;

  /** Failure described in HIVE-8396 should be avoided for beeline. */
  @Test
  public void testQueryStripFullLineCommentFirstLine() {
    beeLineShell.executeQuery("-- a\nset x=1");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  /** Beeline strips comment before assignment. */
  @Test
  public void testQueryStripFullLineCommentNested() {
    beeLineShell.executeQuery("set x=\n-- a\n1");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryStripFullLineComment() {
    beeLineShell.executeQuery("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentFirstLine() {
    beeLineShell.execute("-- a\nset x=1;");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineCommentLastLine() {
    beeLineShell.execute("set x=1;\n-- a");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

  @Test
  public void testScriptStripFullLineComment() {
    beeLineShell.execute("-- a");
  }

  @Test
  public void testScriptStripFullLineCommentNested() {
    beeLineShell.execute("set x=\n-- a\n1;");
    List<String> results = beeLineShell.executeQuery("set x");
    assertThat(results, is(Arrays.asList("x=1")));
  }

}
