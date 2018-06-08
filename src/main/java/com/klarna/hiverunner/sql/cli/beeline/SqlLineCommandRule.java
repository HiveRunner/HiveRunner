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
package com.klarna.hiverunner.sql.cli.beeline;

import java.util.Collections;
import java.util.Set;

import com.klarna.hiverunner.sql.split.Consumer;
import com.klarna.hiverunner.sql.split.Context;
import com.klarna.hiverunner.sql.split.TokenRule;

/**
 * A {@link TokenRule} that causes the splitter to capture beeline commands.
 * Effectively to differentiate between SQL's {@code NOT} operator and Beeline's command prefix.
 */
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
			context.appendWith(Consumer.UNTIL_EOL);
			context.flush();
		} else {
			context.append(token);
		}
	}

}
