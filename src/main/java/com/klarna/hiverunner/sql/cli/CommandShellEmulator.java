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
package com.klarna.hiverunner.sql.cli;

import java.util.List;

import com.klarna.hiverunner.sql.StatementLexer;
import com.klarna.hiverunner.sql.split.TokenRule;

/**
 * Attempt to accurately emulate the behaviours (good and bad) of different Hive
 * shells.
 */
public interface CommandShellEmulator {
    PreProcessor preProcessor();
    PostProcessor postProcessor(StatementLexer lexer);
    String specialCharacters();
    List<TokenRule> splitterRules();
    String getName();
}
