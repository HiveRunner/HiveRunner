package com.klarna.hiverunner.sql.cli;

import java.util.List;

import com.klarna.hiverunner.sql.HiveSqlStatement;

public interface PostProcessor {
	public List<HiveSqlStatement> statement(HiveSqlStatement statement);
}
