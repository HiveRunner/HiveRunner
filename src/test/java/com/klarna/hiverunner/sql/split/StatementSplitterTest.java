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
package com.klarna.hiverunner.sql.split;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.klarna.hiverunner.sql.split.StatementSplitter.SQL_SPECIAL_CHARS;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.klarna.hiverunner.sql.cli.CommandShellEmulator;

// Checks the application of rules, not specific emulator implementations. See other tests for that.
@RunWith(MockitoJUnitRunner.class)
public class StatementSplitterTest {

    @Mock
    private CommandShellEmulator emulator;

    private StatementSplitter splitter;

    @Before
    public void setupEmulator() {
        // Creates a simple emulator that understands ';' only
        when(emulator.specialCharacters()).thenReturn(SQL_SPECIAL_CHARS);
        when(emulator.splitterRules())
                .thenReturn(Arrays.<TokenRule> asList(CloseStatementRule.INSTANCE, DefaultTokenRule.INSTANCE));
        splitter = new StatementSplitter(emulator);
    }

    @Test
    public void defaultRule() {
        assertThat(splitter.split("foo"), is(asList("foo")));
    }

    @Test
    public void multipleRules() {
        assertThat(splitter.split("foo;bar;baz"), is(asList("foo", "bar", "baz")));
    }

}
