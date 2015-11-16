package com.klarna.hiverunner;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class CommentTest {
  @HiveSQL(files = {"commentTest/comment.sql"})
  public HiveShell hiveShell;
  
  @Test
  public void testPreceedingFullLineComment() {
    List<String> results = hiveShell.executeQuery("set x");
    assertEquals(Arrays.asList("x=1"), results);
  }
  
  @Test
  public void testFullLineCommentInsideDeclaration() {
    List<String> results = hiveShell.executeQuery("set y");
    assertEquals(Arrays.asList("y=\"", "\""), results);
  }

}
