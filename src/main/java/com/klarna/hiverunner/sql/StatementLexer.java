/**
 * Copyright (C) 2013-2020 Klarna AB
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
package com.klarna.hiverunner.sql;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.klarna.hiverunner.builder.Statement;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.split.StatementSplitter;

public class StatementLexer {

    private final Charset charset;
    private final CommandShellEmulator commandShellEmulation;
    private final Path cwd;

    public StatementLexer(Path cwd, Charset charset, CommandShellEmulator commandShellEmulation) {
        this.cwd = cwd;
        this.charset = charset;
        this.commandShellEmulation = commandShellEmulation;
    }

    private List<String> internalApplyToStatement(String statement) {
        String transformedHiveSql = commandShellEmulation.preProcessor().statement(statement);
        return commandShellEmulation.postProcessor(this).statement(transformedHiveSql);
    }

    public List<String> applyToScript(String script) {
        List<String> hiveSqlStatements = new ArrayList<>();
        List<Statement> statements = new StatementSplitter(commandShellEmulation)
                .split(commandShellEmulation.preProcessor().script(script));
        for (Statement statement : statements) {
            hiveSqlStatements.addAll(internalApplyToStatement(statement.getSql()));
        }
        return hiveSqlStatements;
    }

    public List<String> applyToStatement(String statement) {
        return internalApplyToStatement(statement);
    }

    public List<String> applyToPath(Path path) {
        if (!path.isAbsolute()) {
            path = cwd.resolve(path);
        }
        try {
            String script = new String(Files.readAllBytes(path), charset);
            return applyToScript(script);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read script file '" + path + "': " + e.getMessage(), e);
        }
    }

}
