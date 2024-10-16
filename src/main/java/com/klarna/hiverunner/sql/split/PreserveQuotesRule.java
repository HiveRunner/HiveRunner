/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) ${license.git.copyrightYears} The HiveRunner Contributors
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/** A {@link TokenRule} for handling quoted character sequences. */
public enum PreserveQuotesRule implements TokenRule {
    INSTANCE;
    
    private static final Pattern LAST_CHAR_NOT_ESCAPED_PATTERN = Pattern.compile(".*[^\\\\].", Pattern.DOTALL);

    @Override
    public Set<String> triggers() {
        return new HashSet<>(Arrays.asList("\"", "'"));
    }

    @Override
    public void handle(final String token, Context context) {
        context.appendWith(new QuotedStringConsumer(token));
    }

    static class QuotedStringConsumer implements Consumer {
        
        private final String token;

        QuotedStringConsumer(String token) {
            this.token = token;
        }
        
        @Override
        public String consume(Context context) {
            String quotedString = token;
            while (context.tokenizer().hasMoreElements()) {
                quotedString += (String) context.tokenizer().nextElement();
                // If the last char is an end of quote token and it was not
                // escaped by the previous token, we break.
                if (quotedString.endsWith(token) && LAST_CHAR_NOT_ESCAPED_PATTERN.matcher(quotedString).matches()) {
                    break;
                }
            }
            return quotedString;
        }
    }
    
}
