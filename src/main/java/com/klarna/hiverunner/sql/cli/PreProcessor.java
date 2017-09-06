package com.klarna.hiverunner.sql.cli;

public interface PreProcessor {
	public String script(String script);
	public String statement(String statement);
}
