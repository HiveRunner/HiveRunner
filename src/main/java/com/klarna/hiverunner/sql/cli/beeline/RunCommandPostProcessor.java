package com.klarna.hiverunner.sql.cli.beeline;

import java.io.File;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;

class RunCommandPostProcessor extends AbstractImportPostProcessor {

	private static final String TOKEN = "!run";

	RunCommandPostProcessor(HiveSqlStatementFactory factory) {
		super(factory);
	}

	@Override
	public File getImportFileFromStatement(HiveSqlStatement statement) {
		// filename cannot contain whitespace
	      String[] tokens = statement.getRawStatement().split(" ");
	      if (tokens.length == 2) {
	        return new File(tokens[1]);
	      }
	      throw new IllegalArgumentException("Cannot get file to import from '" + statement + "'");
	}

	@Override
	public boolean isImportFileStatement(HiveSqlStatement statement) {
		// case-sensitive
	    return statement.getRawStatement().startsWith(TOKEN);
	}

}
