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

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.StringTokenizer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseContextTest {

    @Mock
    private Consumer consumer;
    
    private BaseContext context = new BaseContext(new StringTokenizer(""));

    @Test
    public void appendAndFlush() {
        context.append("abc");
        assertThat(context.getStatements(), is(Collections.<String> emptyList()));
        context.append("def");
        context.flush();
        assertThat(context.getStatements(), is(singletonList("abcdef")));
    }

    @Test
    public void statementAndFlush() {
        context.append("abc");
        assertThat(context.statement(), is("abc"));
        context.flush();
        assertThat(context.statement(), is(""));
    }
    
    @Test
    public void appendWith() {
        when(consumer.consume(context)).thenReturn("statement");
        context.appendWith(consumer);
        assertThat(context.statement(), is("statement"));
        context.flush();
        assertThat(context.getStatements(), is(singletonList("statement")));
    }
}
