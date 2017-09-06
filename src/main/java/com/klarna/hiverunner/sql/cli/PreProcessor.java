package com.klarna.hiverunner.sql.cli;

/** Allows preprocessing of raw script and statement text. */
public interface PreProcessor {
	public String script(String script);
	public String statement(String statement);
}
