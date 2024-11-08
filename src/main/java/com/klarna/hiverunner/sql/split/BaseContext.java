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
package com.klarna.hiverunner.sql.split;

import static com.klarna.hiverunner.sql.split.NewLineUtil.removeLeadingTrailingNewLines;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Base {@link Context} implementation.
 */
class BaseContext implements Context {

    private final StringTokenizer tokenizer;
    private final List<String> statements = new ArrayList<>();
    private String statement = "";

    BaseContext(StringTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public void flush() {
        if (!statement.trim().isEmpty()) {
            statements.add(removeLeadingTrailingNewLines(statement));
        }
        statement = "";
    }

    @Override
    public String statement() {
        return statement;
    }

    @Override
    public StringTokenizer tokenizer() {
        return tokenizer;
    }

    @Override
    public void append(String chars) {
        statement += chars;
    }

    @Override
    public void appendWith(Consumer consumer) {
        append(consumer.consume(this));
    }

    public List<String> getStatements() {
        return statements;
    }
}
