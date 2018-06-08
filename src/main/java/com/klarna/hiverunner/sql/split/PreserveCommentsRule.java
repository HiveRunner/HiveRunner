package com.klarna.hiverunner.sql.split;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

/** A {@link TokenRule} for handling comments. */
public enum PreserveCommentsRule implements TokenRule {
	INSTANCE;
	
	static final Pattern START_OF_COMMENT_PATTERN = Pattern.compile(".*\\s--", Pattern.DOTALL);

	@Override
	public  Set<String> triggers() {
		return Collections.singleton("-");
	}

	@Override
	public void handle(String token, Context context) {
		context.append(token);
		if (START_OF_COMMENT_PATTERN.matcher(context.statement()).matches()) {
			context.appendWith(Consumer.UNTIL_EOL);
		}
	}

}
