/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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
package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import static com.klarna.hiverunner.sql.split.StatementSplitter.SQL_SPECIAL_CHARS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.klarna.hiverunner.builder.Statement;
import com.klarna.hiverunner.sql.HiveRunnerStatement;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;

// Checks the application of rules, not specific emulator implementations. See other tests for that.
@ExtendWith(MockitoExtension.class)
public class StatementSplitterTest {

    @Mock
    private CommandShellEmulator emulator;

    private StatementSplitter splitter;

    private List<Statement> asStatementList(String... strings) {
        List<Statement> statements = new ArrayList<>();
        int index = 0;
        for (String string : strings) {
            statements.add(new HiveRunnerStatement(index++, string));
        }
        return statements;
    }

    @BeforeEach
    public void setupEmulator() {
        // Creates a simple emulator that understands ';' only
        when(emulator.specialCharacters()).thenReturn(SQL_SPECIAL_CHARS);
        when(emulator.splitterRules())
                .thenReturn(Arrays.<TokenRule>asList(CloseStatementRule.INSTANCE, DefaultTokenRule.INSTANCE));
        splitter = new StatementSplitter(emulator);
    }

    @Test
    public void defaultRule() {
        assertThat(splitter.split("foo"), is(asStatementList("foo")));
    }

    @Test
    public void multipleRules() {
        assertThat(splitter.split("foo;bar;baz"), is(asStatementList("foo", "bar", "baz")));
    }

}
