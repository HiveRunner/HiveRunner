package com.klarna.hiverunner.sql.cli.beeline;

import java.util.Collections;
import java.util.Set;

import com.klarna.hiverunner.sql.split.Consumer;
import com.klarna.hiverunner.sql.split.Context;
import com.klarna.hiverunner.sql.split.TokenRule;

public enum SqlLineCommandRule implements TokenRule {
	INSTANCE;
	
	@Override
	public Set<String> triggers() {
		return Collections.singleton("!");
	}

	@Override
	public void handle(String token, Context context) {
		if (context.statement().trim().isEmpty()) {
			context.append(token);
			context.append(Consumer.UNTIL_EOL);
			context.flush();
		} else {
			context.append(token);
		}
	}

}
