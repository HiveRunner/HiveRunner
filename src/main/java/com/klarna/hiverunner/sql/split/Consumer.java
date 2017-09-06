package com.klarna.hiverunner.sql.split;

import java.util.StringTokenizer;

public interface Consumer {

	String consume(Context context);
	
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
