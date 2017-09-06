package com.klarna.hiverunner.sql.split;

import java.util.StringTokenizer;

/**
 * Provide a means to direct the {@link StatementSplitter} in how it should
 * consume tokens.
 */
public interface Consumer {

	String consume(Context context);

	/** A {@link Consumer} that consumes tokens until the end of the line. */
	public static Consumer UNTIL_EOL = new Consumer() {

		@Override
		public String consume(Context context) {
			String buffer = "";
			StringTokenizer tokenizer = context.tokenizer();
			while (tokenizer.hasMoreElements()) {
				buffer += tokenizer.nextElement();
				if (buffer.endsWith("\n")) {
					break;
				}
			}
			return buffer;
		}

	};

}
