/**
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
package com.klarna.hiverunner.sql.cli;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static com.klarna.hiverunner.sql.cli.CommentUtil.stripFullLineComments;

import org.junit.jupiter.api.Test;

public class CommentUtilTest {

    @Test
    public void nothingToStrip() {
        assertThat(stripFullLineComments("a;\nb;\n"), is(equalTo("a;\nb;")));
    }
    @Test
    public void commentToStrip() {
        assertThat(stripFullLineComments("a;\n-- comment\nb;\n"), is(equalTo("a;\nb;")));
    }
}
