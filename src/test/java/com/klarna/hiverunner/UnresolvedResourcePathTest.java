/*
 * Copyright 2013 Klarna AB
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

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

@RunWith(StandaloneHiveRunner.class)
public class UnresolvedResourcePathTest {


    @HiveSQL(files = {}, autoStart = false)
    private HiveShell shell;


    @Test(expected = IllegalArgumentException.class)
    public void resourceFileShouldNotBeCreatedIfReferencesAreUnresolved() {
        shell.addResource("${hiveconf:foo}/bar/baz.csv", "A,B,C");
        shell.start();
    }

    @Test
    public void resourceFileShouldBeCreatedInsideTempDir() {
        shell.addResource("${hiveconf:hadoop.tmp.dir}/bar/baz.csv", "A,B,C");
        shell.start();
        Assert.assertTrue(new File(shell.getHiveConf().get("hadoop.tmp.dir"), "bar/baz.csv").exists());
    }

    @Test(expected = IllegalArgumentException.class)
    public void resourceFilePathShouldAlwaysBeInsideTempDir() {
        shell.addResource("/bar/baz.csv", "A,B,C");
        shell.start();
    }


}
