package com.klarna.hiverunner.sql.split;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTokenRuleTest {

	@Mock
	private Context context;

	@Test
	public void handle() {
		DefaultTokenRule.INSTANCE.handle("x", context);
		verify(context).append("x");
	}
	
}
