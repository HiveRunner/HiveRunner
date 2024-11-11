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
package com.klarna.hiverunner.sql.cli.beeline;

import java.util.Arrays;
import java.util.List;

import com.klarna.hiverunner.sql.StatementLexer;
import com.klarna.hiverunner.sql.cli.DefaultPreProcessor;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.cli.PostProcessor;
import com.klarna.hiverunner.sql.cli.PreProcessor;
import com.klarna.hiverunner.sql.split.CloseStatementRule;
import com.klarna.hiverunner.sql.split.DefaultTokenRule;
import com.klarna.hiverunner.sql.split.PreserveCommentsRule;
import com.klarna.hiverunner.sql.split.PreserveQuotesRule;
import com.klarna.hiverunner.sql.split.StatementSplitter;
import com.klarna.hiverunner.sql.split.TokenRule;

/**
 * Emulates CLI behaviours specific to beeline. This includes interpretation of {@code !run} commands, and full line
 * comment handling.
 */
public enum BeelineEmulator implements CommandShellEmulator {
    INSTANCE;

    public static final String BEELINE_SPECIAL_CHARS = "!";

    @Override
    public PreProcessor preProcessor() {
        return DefaultPreProcessor.INSTANCE;
    }

    @Override
    public PostProcessor postProcessor(StatementLexer lexer) {
        return new RunCommandPostProcessor(lexer);
    }

    @Override
    public String getName() {
        return "BEELINE";
    }

    @Override
    public String specialCharacters() {
        return StatementSplitter.SQL_SPECIAL_CHARS + BEELINE_SPECIAL_CHARS;
    }

    @Override
    public List<TokenRule> splitterRules() {
        // This order is important as rules may be progressively greedy. DefaultTokenRule will consume
        // all tokens for example.
        return Arrays.<TokenRule>asList(CloseStatementRule.INSTANCE, PreserveCommentsRule.INSTANCE,
                PreserveQuotesRule.INSTANCE, SqlLineCommandRule.INSTANCE, DefaultTokenRule.INSTANCE);

    }

}
