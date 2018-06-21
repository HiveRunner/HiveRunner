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
package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.StringTokenizer;

import org.junit.Test;

public class PreserveCommentsRuleTest {

    private static TokenRule rule = PreserveCommentsRule.INSTANCE;

    @Test
    public void withInlineComment() {
        StringTokenizer tokenizer = new StringTokenizer("x -- a b\n", " ", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context); // "x"
        rule.handle(tokenizer.nextToken(), context); // " "
        rule.handle(tokenizer.nextToken(), context); // "--"
        // Should find comment and read until EOL
        assertThat(context.statement(), is("x -- a b\n"));
    }
    
    @Test
    public void noComment() {
        StringTokenizer tokenizer = new StringTokenizer("x a b\n", " ", true);
        Context context = new BaseContext(tokenizer);
        rule.handle(tokenizer.nextToken(), context); // "x"
        rule.handle(tokenizer.nextToken(), context); // " "
        rule.handle(tokenizer.nextToken(), context); // "a"
        rule.handle(tokenizer.nextToken(), context); // " "
        rule.handle(tokenizer.nextToken(), context); // "b\n"
        assertThat(context.statement(), is("x a b\n"));
    }
    
}
