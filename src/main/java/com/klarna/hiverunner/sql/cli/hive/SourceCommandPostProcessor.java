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
package com.klarna.hiverunner.sql.cli.hive;

import com.klarna.hiverunner.sql.StatementLexer;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;
import com.klarna.hiverunner.sql.cli.PostProcessor;

/**
 * A {@link PostProcessor} that inlines external Hive SQL files referenced in
 * {@code SOURCE} directives.
 */
class SourceCommandPostProcessor extends AbstractImportPostProcessor {

    private static final String TOKEN = "source";

    SourceCommandPostProcessor(StatementLexer lexer) {
        super(lexer);
    }

    @Override
    public String getImportPath(String statement) {
        // everything after 'source' (trimmed) is considered the filename
        return statement.trim().substring(TOKEN.length()).trim();
    }

    @Override
    public boolean isImport(String statement) {
        // case-insensitive
        return statement.trim().toLowerCase().startsWith(TOKEN);
    }

}
