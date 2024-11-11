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
package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.StringTokenizer;

import org.junit.jupiter.api.Test;

public class PreserveQuotesRuleTest {

    private static TokenRule rule = PreserveQuotesRule.INSTANCE;

    @Test
    public void singleQuotes() {
        StringTokenizer tokenizer = new StringTokenizer("'b c' d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context); // "'"
        assertThat(context.statement(), is("'b c'"));
    }

    @Test
    public void singleQuotesCrossLine() {
        StringTokenizer tokenizer = new StringTokenizer("'b \n c' d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context); // "'"
        assertThat(context.statement(), is("'b \n c'"));
    }

    @Test
    public void singleEscapedQuotes() {
        StringTokenizer tokenizer = new StringTokenizer("'b \\' c' d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context); // "'"
        assertThat(context.statement(), is("'b \\' c'"));
    }

    @Test
    public void doubleQuotes() {
        StringTokenizer tokenizer = new StringTokenizer("\"b c\" d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context);
        assertThat(context.statement(), is("\"b c\""));
    }

    @Test
    public void doubleQuotesCrossLine() {
        StringTokenizer tokenizer = new StringTokenizer("\"b \n c\" d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context);
        assertThat(context.statement(), is("\"b \n c\""));
    }

    @Test
    public void doubleEscapedQuotes() {
        StringTokenizer tokenizer = new StringTokenizer("\"b \\\" c\" d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context);
        assertThat(context.statement(), is("\"b \\\" c\""));
    }

    @Test
    public void doubleSingleQuotes() {
        StringTokenizer tokenizer = new StringTokenizer("\"b ' c\" d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context);
        assertThat(context.statement(), is("\"b ' c\""));
    }

    @Test
    public void singleDoubleQuotes() {
        StringTokenizer tokenizer = new StringTokenizer("'b \" c' d\n", " '\"", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context); // "'"
        assertThat(context.statement(), is("'b \" c'"));
    }

}
