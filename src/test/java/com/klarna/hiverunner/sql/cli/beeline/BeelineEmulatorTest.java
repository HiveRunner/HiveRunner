package com.klarna.hiverunner.sql.cli.beeline;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class BeelineEmulatorTest {

	  @Test
	  public void testFullLineCommentAndSetStatementBeeLine() {
	    String hql = "-- hello\nset x=1;";
	    assertThat(BeelineEmulator.INSTANCE.preProcessor().statement(hql), is("set x=1;"));
	  }
	  
	  @Test
	  public void testFullLineCommentStatementBeeLine() {
	    String hql = "-- hello";
	    assertThat(BeelineEmulator.INSTANCE.preProcessor().statement(hql), is(""));
	  }
	  
	  @Test
	  public void testFullLineCommentAndSetScriptBeeLine() {
	    String hql = "-- hello\nset x=1;";
	    assertThat(BeelineEmulator.INSTANCE.preProcessor().script(hql), is("set x=1;"));
	  }

	  @Test
	  public void testFullLineCommentScriptBeeLine() {
	    String hql = "-- hello";
	    assertThat(BeelineEmulator.INSTANCE.preProcessor().script(hql), is(""));
	  }
	  
}
