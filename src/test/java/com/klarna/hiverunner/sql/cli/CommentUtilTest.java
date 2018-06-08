package com.klarna.hiverunner.sql.cli;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.klarna.hiverunner.sql.cli.CommentUtil.stripFullLineComments;

import org.junit.Test;

public class CommentUtilTest {

	@Test
	public void nothingToStrip() {
		assertThat(stripFullLineComments("a;\nb;\n"), is(equalTo("a;\nb;")));
	}
	@Test
	public void commentToStrip() {
		assertThat(stripFullLineComments("a;\n-- comment\nb;\n"), is(equalTo("a;\nb;")));
	}
}
