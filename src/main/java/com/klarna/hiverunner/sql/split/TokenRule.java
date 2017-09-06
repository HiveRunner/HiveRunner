package com.klarna.hiverunner.sql.split;

import java.util.Set;

public interface TokenRule {
	Set<String> triggers();
	void handle(String token, Context context);
}
