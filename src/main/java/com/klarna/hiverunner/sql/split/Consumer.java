/*
 * Copyright 2015-2018 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
