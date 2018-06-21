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
package com.klarna.hiverunner.sql.cli;

/** Utility methods for handling SQL comments. */
public final class CommentUtil {

    private CommentUtil() {}

    public static String stripFullLineComments(String statement) {
        StringBuilder newStatement = new StringBuilder(statement.length());
        String[] lines = statement.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (!line.trim().startsWith("--")) {
                newStatement.append(line);
                if (i < lines.length - 1) {
                    newStatement.append('\n');
                }
            }
        }
        return newStatement.toString();
    }

}
