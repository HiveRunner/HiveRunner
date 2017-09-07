package com.klarna.hiverunner.sql.cli.beeline;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;
import com.klarna.hiverunner.sql.cli.PostProcessor;

/**
 * A {@link PostProcessor} that inlines external HQL files referenced in
 * {@code !run} directives.
 */
class RunCommandPostProcessor extends AbstractImportPostProcessor {

	private static final String TOKEN = "!run";

	RunCommandPostProcessor(HiveSqlStatementFactory factory) {
		super(factory);
	}

	@Override
	public String getImportPath(HiveSqlStatement statement) {
		// filename cannot contain whitespace
	      String[] tokens = statement.getRawStatement().split(" ");
	      if (tokens.length == 2) {
	        return tokens[1];
	      }
	      throw new IllegalArgumentException("Cannot get file to import from '" + statement + "'");
	}

	@Override
	public boolean isImport(HiveSqlStatement statement) {
		// case-sensitive
	    return statement.getRawStatement().startsWith(TOKEN);
	}

}
