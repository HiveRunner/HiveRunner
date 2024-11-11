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

import static java.lang.Character.isWhitespace;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Removes all white space up to and including the newlines closest to the a sequence of non whitespace characters. The
 * aim here is to preserve the indentation of statements within scripts.
 */
enum NewLineUtil {

    INSTANCE;

    private static final Set<Character> LINE_BREAKS = new HashSet<>(Arrays.<Character>asList('\n', '\r', '\f'));

    static String removeLeadingTrailingNewLines(String in) {
        int leadingBreakPosition = -1;
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            if (!isWhitespace(c)) {
                break;
            }
            if (LINE_BREAKS.contains(c)) {
                leadingBreakPosition = i;
            }
        }

        int trailingBreakPosition = -1;
        for (int i = in.length() - 1; i >= 0; i--) {
            char c = in.charAt(i);
            if (!isWhitespace(c)) {
                break;
            }
            if (LINE_BREAKS.contains(c)) {
                trailingBreakPosition = i;
            }
        }
        if (trailingBreakPosition == -1) {
            trailingBreakPosition = in.length();
        }
        leadingBreakPosition++;
        if (leadingBreakPosition >= trailingBreakPosition + 1) {
            return "";
        }
        return in.substring(leadingBreakPosition, trailingBreakPosition);
    }

}
