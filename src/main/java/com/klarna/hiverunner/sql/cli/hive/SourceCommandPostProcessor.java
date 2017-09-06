package com.klarna.hiverunner.sql.cli.hive;

import java.io.File;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;

class SourceCommandPostProcessor extends AbstractImportPostProcessor {

	private static final String TOKEN = "source";

	SourceCommandPostProcessor(HiveSqlStatementFactory factory) {
		super(factory);
	}

	@Override
	public File getImportFileFromStatement(HiveSqlStatement statement) {
		// everything after 'source' (trimmed) is considered the filename
		String filename = statement.getRawStatement().substring(TOKEN.length()).trim();
		return new File(filename);
	}

	@Override
	public boolean isImportFileStatement(HiveSqlStatement statement) {
		// case-insensitive
		return statement.getRawStatement().toLowerCase().startsWith(TOKEN);
	}

}
