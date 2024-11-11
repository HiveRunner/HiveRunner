/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A {@link TokenRule} for handling comments.
 */
public enum PreserveCommentsRule implements TokenRule {
    INSTANCE;

    static final Pattern START_OF_COMMENT_PATTERN = Pattern.compile(".*\\s--", Pattern.DOTALL);

    @Override
    public Set<String> triggers() {
        return Collections.singleton("-");
    }

    @Override
    public void handle(String token, Context context) {
        context.append(token);
        if (START_OF_COMMENT_PATTERN.matcher(context.statement()).matches()) {
            context.appendWith(Consumer.UNTIL_EOL);
        }
    }

}
