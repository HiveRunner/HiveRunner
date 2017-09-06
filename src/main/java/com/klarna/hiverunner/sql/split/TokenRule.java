package com.klarna.hiverunner.sql.split;

import java.util.Set;

/** Allows the implementation of splitting rules based on specific tokens. */
public interface TokenRule {
	Set<String> triggers();
	void handle(String token, Context context);
}
