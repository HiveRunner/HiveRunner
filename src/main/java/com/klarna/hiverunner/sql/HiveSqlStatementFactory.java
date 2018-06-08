/*
 * Copyright 2015-2018 Klarna AB
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

import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.split.StatementSplitter;

public class HiveSqlStatementFactory {

	private final Charset charset;
	private final CommandShellEmulator commandShellEmulation;
	private final Path cwd;

	public HiveSqlStatementFactory(Path cwd, Charset charset, CommandShellEmulator commandShellEmulation) {
		this.cwd = cwd;
		this.charset = charset;
		this.commandShellEmulation = commandShellEmulation;
	}

	private List<HiveSqlStatement> internalNewInstanceForStatement(String statement) {
		String transformedHiveSql = commandShellEmulation.preProcessor().statement(statement.trim());
		return commandShellEmulation.postProcessor(this).statement(new HiveSqlStatement(transformedHiveSql));
	}

	public List<HiveSqlStatement> newInstanceForScript(String script) {
		List<HiveSqlStatement> hiveSqlStatements = new ArrayList<>();
		List<String> statements = new StatementSplitter(commandShellEmulation)
				.split(commandShellEmulation.preProcessor().script(script));
		for (String statement : statements) {
			hiveSqlStatements.addAll(internalNewInstanceForStatement(statement));
		}
		return hiveSqlStatements;
	}

	public List<HiveSqlStatement> newInstanceForStatement(String statement) {
		return internalNewInstanceForStatement(statement);
	}

	public List<HiveSqlStatement> newInstanceForPath(Path path) {
		if (!path.isAbsolute()) {
			path = cwd.resolve(path);
		}
		try {
			String script = new String(Files.readAllBytes(path), charset);
			return newInstanceForScript(script);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read script file '" + path + "': " + e.getMessage(), e);
		}
	}

}
