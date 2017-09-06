package com.klarna.hiverunner.sql.split;

import java.util.Collections;
import java.util.Set;

public enum DefaultTokenRule implements TokenRule {
	INSTANCE;
	
	@Override
	public Set<String> triggers() {
		return Collections.emptySet();
	}

	@Override
	public void handle(String token, Context context) {
		context.append(token);
	}

}
