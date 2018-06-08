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
package com.klarna.hiverunner.sql.cli;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;

/**
 * An abstract {@link PostProcessor} implementation that recursively expands
 * import type commands such as Hive CLI's {@code SOURCE}, and Beeline's
 * {@code !run} commands.
 */
public abstract class AbstractImportPostProcessor implements PostProcessor {

	private final HiveSqlStatementFactory factory;

	public AbstractImportPostProcessor(HiveSqlStatementFactory factory) {
		this.factory = factory;
	}

	@Override
	public List<HiveSqlStatement> statement(HiveSqlStatement statement) {
		if (isImport(statement)) {
			String importPath = getImportPath(statement);
			Path path = Paths.get(importPath);
			return factory.newInstanceForPath(path);
		}
		return Collections.singletonList(statement);
	}

	public abstract String getImportPath(HiveSqlStatement statement);

	public abstract boolean isImport(HiveSqlStatement statement);

}
