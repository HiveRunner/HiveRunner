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

import java.util.StringTokenizer;

/**
 * Provide a means to direct the {@link StatementSplitter} in how it should consume tokens.
 */
public interface Consumer {

    String consume(Context context);

    /** A {@link Consumer} that consumes tokens until the end of the line. */
    public static Consumer UNTIL_EOL = new Consumer() {

        @Override
        public String consume(Context context) {
            StringBuilder builder = new StringBuilder();
            StringTokenizer tokenizer = context.tokenizer();
            while (tokenizer.hasMoreElements()) {
                builder.append(tokenizer.nextElement());
                if (builder.charAt(builder.length() - 1) == '\n') {
                    break;
                }
            }
            return builder.toString();
        }

    };

}
