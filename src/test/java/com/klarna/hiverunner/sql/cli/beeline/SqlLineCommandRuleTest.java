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
