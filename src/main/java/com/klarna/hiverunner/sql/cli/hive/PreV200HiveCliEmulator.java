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
package com.klarna.hiverunner.sql.cli.hive;

import java.util.List;

import com.klarna.hiverunner.sql.StatementLexer;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.cli.PostProcessor;
import com.klarna.hiverunner.sql.cli.PreProcessor;
import com.klarna.hiverunner.sql.split.TokenRule;

/**
 * Emulates CLI behaviours specific to the Hive CLI. This includes
 * interpretation of {@code source} commands, and the broken full line comment
 * handling.
 */
public enum PreV200HiveCliEmulator implements CommandShellEmulator {
    INSTANCE;

    @Override
    public PreProcessor preProcessor() {
        return PreV200HiveCliPreProcessor.INSTANCE;
    }

    @Override
    public PostProcessor postProcessor(StatementLexer lexer) {
        return HiveCliEmulator.INSTANCE.postProcessor(lexer);
    }

    @Override
    public String getName() {
        return "HIVE_CLI_PRE_V200";
    }

    @Override
    public String specialCharacters() {
        return HiveCliEmulator.INSTANCE.specialCharacters();
    }

    @Override
    public List<TokenRule> splitterRules() {
        return HiveCliEmulator.INSTANCE.splitterRules();
    }
}
