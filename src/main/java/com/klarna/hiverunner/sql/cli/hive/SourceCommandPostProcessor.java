package com.klarna.hiverunner.sql.cli.hive;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;
import com.klarna.hiverunner.sql.cli.PostProcessor;

/**
 * A {@link PostProcessor} that inlines external HQL files referenced in
 * {@code SOURCE} directives.
 */
class SourceCommandPostProcessor extends AbstractImportPostProcessor {

	private static final String TOKEN = "source";

	SourceCommandPostProcessor(HiveSqlStatementFactory factory) {
		super(factory);
	}

	@Override
	public String getImportPath(HiveSqlStatement statement) {
		// everything after 'source' (trimmed) is considered the filename
		return statement.getRawStatement().substring(TOKEN.length()).trim();
	}

	@Override
	public boolean isImport(HiveSqlStatement statement) {
		// case-insensitive
		return statement.getRawStatement().toLowerCase().startsWith(TOKEN);
	}

}
