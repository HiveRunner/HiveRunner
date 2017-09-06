package com.klarna.hiverunner.sql.cli;

import java.io.File;
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
		if (isImportFileStatement(statement)) {
			File importFile = getImportFileFromStatement(statement);
			Path path = Paths.get(importFile.toURI());
			return factory.newInstanceForPath(path);
		}
		return Collections.singletonList(statement);
	}

	public abstract File getImportFileFromStatement(HiveSqlStatement statement);

	public abstract boolean isImportFileStatement(HiveSqlStatement statement);

}
