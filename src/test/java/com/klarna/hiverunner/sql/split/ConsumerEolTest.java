package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.klarna.hiverunner.sql.split.Consumer.UNTIL_EOL;

import java.util.StringTokenizer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerEolTest {

	@Mock
	private Context context;
	@Mock
	private StringTokenizer tokenizer;

	@Before
	public void setup() {
		when(context.tokenizer()).thenReturn(tokenizer);
	}

	@Test
	public void consumeLine() {
		when(tokenizer.nextElement()).thenReturn("a", " ", "b", "\n");
		when(tokenizer.hasMoreElements()).thenReturn(true, true, true, true, false);
		assertThat(UNTIL_EOL.consume(context), is("a b\n"));
	}

	@Test
	public void consumeNoCR() {
		when(tokenizer.nextElement()).thenReturn("a", " ", "b");
		when(tokenizer.hasMoreElements()).thenReturn(true, true, true, false);
		assertThat(UNTIL_EOL.consume(context), is("a b"));
	}

	@Test
	public void consumeMultiLine() {
		when(tokenizer.nextElement()).thenReturn("a", " ", "b", "\n", "c");
		when(tokenizer.hasMoreElements()).thenReturn(true, true, true, true, true, false);
		assertThat(UNTIL_EOL.consume(context), is("a b\n"));
	}
}
