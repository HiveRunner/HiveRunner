package com.klarna.hiverunner.sql.split;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CloseStatementRuleTest {

	private static TokenRule rule = CloseStatementRule.INSTANCE;

	@Mock
	private Context context;

	@Test
	public void handle() {
		rule.handle(null, context);
		verify(context).flush();
	}
}
