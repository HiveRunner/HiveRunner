/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) ${license.git.copyrightYears} The HiveRunner Contributors
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
package com.klarna.hiverunner.sql.cli.hive;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

public class PreV200HiveCliEmulatorTest {
    @Test
    public void testFullLineCommentAndSetStatementHiveCli() {
        String hiveSql = "-- hello\nset x=1;";
        assertThat(PreV200HiveCliEmulator.INSTANCE.preProcessor().statement(hiveSql), is(hiveSql));
    }

    @Test
    public void testFullLineCommentStatementHiveCli() {
        String hiveSql = "-- hello";
        assertThat(PreV200HiveCliEmulator.INSTANCE.preProcessor().statement(hiveSql), is(hiveSql));
    }

    @Test
    public void testFullLineCommentAndSetScriptHiveCli() {
        String hiveSql = "-- hello\nset x=1;";
        assertThat(PreV200HiveCliEmulator.INSTANCE.preProcessor().script(hiveSql), is("set x=1;"));
    }

    @Test
    public void testFullLineCommentScriptHiveCli() {
        String hiveSql = "-- hello";
        assertThat(PreV200HiveCliEmulator.INSTANCE.preProcessor().script(hiveSql), is(""));
    }

}
