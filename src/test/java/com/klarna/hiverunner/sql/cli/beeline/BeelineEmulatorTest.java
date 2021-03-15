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
package com.klarna.hiverunner.sql.cli.beeline;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

public class BeelineEmulatorTest {

    @Test
    public void testFullLineCommentAndSetStatementBeeLine() {
        String hiveSql = "-- hello\nset x=1;";
        assertThat(BeelineEmulator.INSTANCE.preProcessor().statement(hiveSql), is("set x=1;"));
    }

    @Test
    public void testFullLineCommentStatementBeeLine() {
        String hiveSql = "-- hello";
        assertThat(BeelineEmulator.INSTANCE.preProcessor().statement(hiveSql), is(""));
    }

    @Test
    public void testFullLineCommentAndSetScriptBeeLine() {
        String hiveSql = "-- hello\nset x=1;";
        assertThat(BeelineEmulator.INSTANCE.preProcessor().script(hiveSql), is("set x=1;"));
    }

    @Test
    public void testFullLineCommentScriptBeeLine() {
        String hiveSql = "-- hello";
        assertThat(BeelineEmulator.INSTANCE.preProcessor().script(hiveSql), is(""));
    }

}
