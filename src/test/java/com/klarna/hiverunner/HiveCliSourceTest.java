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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

@RunWith(StandaloneHiveRunner.class)
public class HiveCliSourceTest {

    private static final String TEST_DB = "test_db";

    @HiveRunnerSetup
    public final static HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
        {
            setCommandShellEmulator(HiveCliEmulator.INSTANCE);
        }
    };

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @HiveSQL(files = {}, encoding = "UTF-8", autoStart = false)
    private HiveShell hiveCliShell;

    @Test
    public void testNestedImport() throws Exception {
        File a = new File(temp.getRoot(), "a.hql");
        try (PrintStream out = new PrintStream(a)) {
            // single statement case
            out.println("create view ${db}.a as select * from ${db}.src where c1 <> 'z'");
        }

        File b = new File(temp.getRoot(), "b.hql");
        try (PrintStream out = new PrintStream(b)) {
            // multi statement case with script import
            out.println("source a.hql;");
            out.println("create database db_b;");
            out.println("create view db_b.b as select c0, count(*) as c1_cnt from ${db}.a group by c0;");
        }

        File c = new File(temp.getRoot(), "c.hql");
        try (PrintStream out = new PrintStream(c)) {
            // multi statement case
            out.println("create database db_c;");
            out.println("create view db_c.c as select * from db_b.b where c1_cnt > 1;");
        }

        File main = new File(temp.getRoot(), "main.hql");
        try (PrintStream out = new PrintStream(main)) {
            // multi import case
            out.println("source b.hql;");
            out.println("source\nc.hql\n;");
        }

        hiveCliShell.setHiveVarValue("db", TEST_DB);
        hiveCliShell.setCwd(temp.getRoot().toPath());
        hiveCliShell.start();
        hiveCliShell.execute(new StringBuilder()
                .append("create database ${db};")
                .append("create table ${db}.src (")
                .append("c0 string, ")
                .append("c1 string")
                .append(");")
                .toString());
        hiveCliShell.insertInto(TEST_DB, "src")
                .addRow("A", "x")
                .addRow("A", "y")
                .addRow("B", "z")
                .addRow("B", "y")
                .addRow("C", "z")
                .commit();


        hiveCliShell.execute(main);

        List<String> results = hiveCliShell.executeQuery("select * from db_c.c");
        assertThat(results.size(), is(1));
        assertThat(results.get(0), is("A\t2"));
    }

}
