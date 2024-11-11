/*
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

/**
 * A {@link PreProcessor} that strips comments from statements and scripts.
 */
public enum DefaultPreProcessor implements PreProcessor {
    INSTANCE;

    @Override
    public String script(String script) {
        return CommentUtil.stripFullLineComments(script);
    }

    @Override
    public String statement(String statement) {
        return CommentUtil.stripFullLineComments(statement);
    }

    ;
}
