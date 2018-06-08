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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.klarna.hiverunner.sql.split.Consumer;
import com.klarna.hiverunner.sql.split.Context;

@RunWith(MockitoJUnitRunner.class)
public class SqlLineCommandRuleTest {

	@Mock
	private Context context;
	
	@Test
	public void handleStart() {
		when(context.statement()).thenReturn(" ");
		SqlLineCommandRule.INSTANCE.handle("token", context);
		verify(context).append("token");
		verify(context).appendWith(Consumer.UNTIL_EOL);
		verify(context).flush();
	}
	
	@Test
	public void handleOther() {
		when(context.statement()).thenReturn("statement");
		SqlLineCommandRule.INSTANCE.handle("token", context);
		verify(context).append("token");
		verify(context, never()).appendWith(any(Consumer.class));
		verify(context, never()).flush();
	}
	
}
