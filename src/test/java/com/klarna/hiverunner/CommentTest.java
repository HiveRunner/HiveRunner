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
package com.klarna.hiverunner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class CommentTest {
    @HiveSQL(files = {"CommentTest/comment.sql"})
    public HiveShell hiveShell;

    @Test
    public void testPreceedingFullLineComment() {
        List<String> results = hiveShell.executeQuery("set x");
        assertEquals(Arrays.asList("x=1"), results);
    }

    @Test
    public void testFullLineCommentInsideDeclaration() {
        List<String> results = hiveShell.executeQuery("set y");
        assertEquals(Arrays.asList("y=\"", "\""), results);
    }

}
