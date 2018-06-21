/*
 * Copyright 2015-2018 Klarna AB
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

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.nio.file.Paths;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.klarna.hiverunner.sql.StatementLexer;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;

@RunWith(MockitoJUnitRunner.class)
public class RunCommandPostProcessorTest {

    @Mock
    private StatementLexer lexer;

    private AbstractImportPostProcessor processor;

    @Before
    public void setup() {
        processor = new RunCommandPostProcessor(lexer);
    }

    @Test
    public void isImport() {
        assertThat(processor.isImport("!run x;"), is(true));
    }
    
    @Test
    public void isImportSpaces() {
      assertThat(processor.isImport("   !run x   ;   "), is(true));
    }

    @Test
    public void isNotImport() {
        assertThat(processor.isImport("SELECT * FROM x;"), is(false));
    }

    @Test
    public void importPathValid() {
        assertThat(processor.getImportPath("!run x"), is("x"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void importPathInvalid() {
        processor.getImportPath("!run;");
    }

    @Test
    public void importStatement() {
        List<String> expected = asList("statement x");
        when(lexer.applyToPath(Paths.get("x"))).thenReturn(expected);

        assertThat(processor.statement("!run x"), is(expected));
    }
    
    @Test
    public void importStatementSpaces() {
      List<String> expected = asList("statement x");
      when(lexer.applyToPath(Paths.get("x"))).thenReturn(expected);
      
      assertThat(processor.statement("   !run x   "), is(expected));
    }

    @Test
    public void generalStatement() {
        List<String> expected = asList("SELECT * FROM x");
        assertThat(processor.statement("SELECT * FROM x"), is(expected));
    }

}
