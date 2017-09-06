package com.klarna.hiverunner.sql.cli;

import java.util.List;

import com.klarna.hiverunner.sql.HiveSqlStatement;

/**
 * Allows the further processing of {@link HiveSqlStatement HiveSqlStatements}
 * that have been extracted from a script.
 */
public interface PostProcessor {
	public List<HiveSqlStatement> statement(HiveSqlStatement statement);
}
