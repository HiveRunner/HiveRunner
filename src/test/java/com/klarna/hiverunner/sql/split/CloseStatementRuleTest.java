package com.klarna.hiverunner.sql.split;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CloseStatementRuleTest {

	@Mock
	private Context context;

	@Test
	public void handle() {
		CloseStatementRule.INSTANCE.handle(null, context);
		verify(context).flush();
	}
}
