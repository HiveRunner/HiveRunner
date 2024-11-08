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

import static com.klarna.hiverunner.sql.split.NewLineUtil.removeLeadingTrailingNewLines;

import org.junit.jupiter.api.Test;

public class NewLineUtilTest {

    @Test
    public void typical() {
        assertThat(removeLeadingTrailingNewLines(""), is(""));
        assertThat(removeLeadingTrailingNewLines(" "), is(" "));
        assertThat(removeLeadingTrailingNewLines(" a "), is(" a "));
        assertThat(removeLeadingTrailingNewLines("a"), is("a"));
        assertThat(removeLeadingTrailingNewLines(" a"), is(" a"));
        assertThat(removeLeadingTrailingNewLines("a "), is("a "));
        assertThat(removeLeadingTrailingNewLines("\n"), is(""));
        assertThat(removeLeadingTrailingNewLines(" \n "), is(""));
        assertThat(removeLeadingTrailingNewLines(" \n \n "), is(""));
        assertThat(removeLeadingTrailingNewLines("\n a \n"), is(" a "));
        assertThat(removeLeadingTrailingNewLines(" \n a b \n "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \n \n a b \n \n "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \n a b \n "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \n a \n b \n "), is(" a \n b "));
        assertThat(removeLeadingTrailingNewLines("\r"), is(""));
        assertThat(removeLeadingTrailingNewLines(" \r "), is(""));
        assertThat(removeLeadingTrailingNewLines(" \r \r "), is(""));
        assertThat(removeLeadingTrailingNewLines("\r a \r"), is(" a "));
        assertThat(removeLeadingTrailingNewLines(" \r a b \r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \r \r a b \r \r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \r a b \r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \r a \r b \r "), is(" a \r b "));
        assertThat(removeLeadingTrailingNewLines("\f"), is(""));
        assertThat(removeLeadingTrailingNewLines(" \f "), is(""));
        assertThat(removeLeadingTrailingNewLines(" \f \f "), is(""));
        assertThat(removeLeadingTrailingNewLines("\f a \f"), is(" a "));
        assertThat(removeLeadingTrailingNewLines(" \f a b \f "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \f \f a b \f \f "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \f a b \f "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines("\f\r"), is(""));
        assertThat(removeLeadingTrailingNewLines(" \f\r "), is(""));
        assertThat(removeLeadingTrailingNewLines(" \f\r \f\r "), is(""));
        assertThat(removeLeadingTrailingNewLines("\f\r a \f\r"), is(" a "));
        assertThat(removeLeadingTrailingNewLines(" \f\r a b \f\r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \f\r \f\r a b \f\r \f\r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \f\r a b \f\r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \f\r a \f\r b \f\r "), is(" a \f\r b "));
        assertThat(removeLeadingTrailingNewLines("\n\r"), is(""));
        assertThat(removeLeadingTrailingNewLines(" \n\r "), is(""));
        assertThat(removeLeadingTrailingNewLines(" \n\r \n\r "), is(""));
        assertThat(removeLeadingTrailingNewLines("\n\r a \n\r"), is(" a "));
        assertThat(removeLeadingTrailingNewLines(" \n\r a b \n\r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \n\r \n\r a b \n\r \n\r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \n\r a b \n\r "), is(" a b "));
        assertThat(removeLeadingTrailingNewLines(" \n\r a \n\r b \n\r "), is(" a \n\r b "));
        assertThat(removeLeadingTrailingNewLines("\t"), is("\t"));
        assertThat(removeLeadingTrailingNewLines("\ta\t"), is("\ta\t"));
        assertThat(removeLeadingTrailingNewLines("a"), is("a"));
        assertThat(removeLeadingTrailingNewLines("\ta"), is("\ta"));
        assertThat(removeLeadingTrailingNewLines("a\t"), is("a\t"));
        assertThat(removeLeadingTrailingNewLines("\n"), is(""));
        assertThat(removeLeadingTrailingNewLines("\t\n\t"), is(""));
        assertThat(removeLeadingTrailingNewLines("\t\n\t\n\t"), is(""));
        assertThat(removeLeadingTrailingNewLines("\n\ta\t\n"), is("\ta\t"));
        assertThat(removeLeadingTrailingNewLines("\t\n\ta\tb\t\n\t"), is("\ta\tb\t"));
        assertThat(removeLeadingTrailingNewLines("\t\n\t\n\ta\tb\t\n\t\n\t"), is("\ta\tb\t"));
    }
}
