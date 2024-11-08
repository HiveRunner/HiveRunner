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
package com.klarna.hiverunner.sql.cli;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.klarna.hiverunner.sql.StatementLexer;

@ExtendWith(MockitoExtension.class)
public class AbstractImportPostProcessorTest {

    private static final String PATH = "path";
    private static final String IMPORT_STATEMENT = "importStatement";
    private static final String NON_IMPORT_STATEMENT = "nonImportStatement";

    @Mock
    private StatementLexer lexer;

    private List<String> expected;

    @BeforeEach
    public void setup() {
        expected = singletonList(NON_IMPORT_STATEMENT);
    }

    @Test
    public void scriptImport() {
        when(lexer.applyToPath(Paths.get(PATH))).thenReturn(expected);
        PostProcessor processor = new TestAbstractImportPostProcessor(true, PATH, lexer);
        List<String> actual = processor.statement(IMPORT_STATEMENT);
        assertThat(actual, is(equalTo(expected)));
    }

    @Test
    public void nonScriptImport() {
        PostProcessor processor = new TestAbstractImportPostProcessor(false, null, lexer);
        List<String> actual = processor.statement(NON_IMPORT_STATEMENT);
        assertThat(actual, is(equalTo(expected)));
    }

    private static class TestAbstractImportPostProcessor extends AbstractImportPostProcessor {

        private final String path;
        private final boolean isImport;

        public TestAbstractImportPostProcessor(boolean isImport, String path, StatementLexer lexer) {
            super(lexer);
            this.isImport = isImport;
            this.path = path;
        }

        @Override
        public String getImportPath(String statement) {
            return path;
        }

        @Override
        public boolean isImport(String statement) {
            return isImport;
        }

    }
}
