/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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

import static com.klarna.hiverunner.sql.split.Consumer.UNTIL_EOL;

import java.util.StringTokenizer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumerEolTest {

    @Mock
    private Context context;
    @Mock
    private StringTokenizer tokenizer;

    @BeforeEach
    public void setup() {
        when(context.tokenizer()).thenReturn(tokenizer);
    }

    @Test
    public void consumeLine() {
        when(tokenizer.nextElement()).thenReturn("a", " ", "b", "\n");
        when(tokenizer.hasMoreElements()).thenReturn(true, true, true, true, false);
        assertThat(UNTIL_EOL.consume(context), is("a b\n"));
    }

    @Test
    public void consumeNoCR() {
        when(tokenizer.nextElement()).thenReturn("a", " ", "b");
        when(tokenizer.hasMoreElements()).thenReturn(true, true, true, false);
        assertThat(UNTIL_EOL.consume(context), is("a b"));
    }

    @Test
    public void consumeMultiLine() {
        when(tokenizer.nextElement()).thenReturn("a", " ", "b", "\n", "c");
        when(tokenizer.hasMoreElements()).thenReturn(true, true, true, true, true, false);
        assertThat(UNTIL_EOL.consume(context), is("a b\n"));
    }
    
}
