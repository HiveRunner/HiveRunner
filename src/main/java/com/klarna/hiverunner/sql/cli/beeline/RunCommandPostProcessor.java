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
package com.klarna.hiverunner.sql.cli.beeline;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;
import com.klarna.hiverunner.sql.cli.PostProcessor;

/**
 * A {@link PostProcessor} that inlines external Hive SQL files referenced in
 * {@code !run} directives.
 */
class RunCommandPostProcessor extends AbstractImportPostProcessor {

	private static final String TOKEN = "!run";

	RunCommandPostProcessor(HiveSqlStatementFactory factory) {
		super(factory);
	}

	@Override
	public String getImportPath(HiveSqlStatement statement) {
		// Belline does not allow the filename cannot contain whitespace
		String[] tokens = statement.getRawStatement().split(" ");
		if (tokens.length == 2) {
			return tokens[1];
		}
		throw new IllegalArgumentException("Cannot get file to import from '" + statement + "'");
	}

	@Override
	public boolean isImport(HiveSqlStatement statement) {
		// Beeline is case-sensitive; only accept lower case '!run'
		return statement.getRawStatement().startsWith(TOKEN);
	}

}
