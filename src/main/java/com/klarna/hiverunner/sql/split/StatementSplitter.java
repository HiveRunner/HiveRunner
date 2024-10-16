/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.klarna.hiverunner.builder.Statement;
import com.klarna.hiverunner.sql.HiveRunnerStatement;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;

/**
 * Splits script text into statements according to a
 * {@link CommandShellEmulator}.
 */
public class StatementSplitter {

    public static final String SQL_SPECIAL_CHARS = ";\"'-\n\r\f";

    private final List<TokenRule> rules;
    private final String specialChars;

    public StatementSplitter(CommandShellEmulator emulator) {
        this(emulator.splitterRules(), emulator.specialCharacters());
    }

    /**
     * @param rules Order of rules defines processing precedence. 
     */
    public StatementSplitter(List<TokenRule> rules, String specialChars) {
        this.rules = rules;
        this.specialChars = specialChars;
    }

    public List<Statement> split(String expression) {
        StringTokenizer tokenizer = new StringTokenizer(expression, specialChars, true);
        BaseContext context = new BaseContext(tokenizer);
        while (tokenizer.hasMoreElements()) {
            String token = (String) tokenizer.nextElement();
            for (TokenRule rule : rules) {
                if (rule.triggers().contains(token) || rule.triggers().isEmpty()) {
                    rule.handle(token, context);
                    break;
                }
            }
        }

        // Only add statement that is not empty
        context.flush();
        
        List<Statement> hiveRunnerStatements  = new ArrayList<>();
        int index = 0;
        for (String statement : context.getStatements()) {
          hiveRunnerStatements.add(new HiveRunnerStatement(index++, statement));
        }
        
        return hiveRunnerStatements;
    }

}
