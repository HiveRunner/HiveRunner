package com.klarna.hiverunner.sql;

import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class StatementsSplitterTest {

    @Test
    public void testSplitBasic() {
        String str = "foo;bar;baz";
        List expected = Arrays.asList("foo", "bar", "baz");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testRemoveTrailingSemiColon() {
        String str = ";foo;bar;baz;";
        List expected = Arrays.asList("foo", "bar", "baz");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testDiscardRedundantSemiColons() {
        String str = "a;b;;;c";
        List expected = Arrays.asList("a", "b", "c");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testDiscardTrailingSpace() {
        String str = "a;   b\t\n   ;  \n\tc   c;";
        List expected = Arrays.asList("a", "b", "c   c");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testDiscardEmptyStatements() {
        String str = "a;b;     \t\n   ;c;";
        List expected = Arrays.asList("a", "b", "c");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testCommentPreserved() {
        String str = "foo -- bar";
        List expected = Arrays.asList("foo -- bar");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testCommentWithSingleQuote() {
        String str = "foo -- b'ar";
        List expected = Arrays.asList("foo -- b'ar");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testCommentWithDoubleQuote() {
        String str = "foo -- b\"ar";
        List expected = Arrays.asList("foo -- b\"ar");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testCommentWithSemiColon() {
        String str = "foo -- b;ar";
        List expected = Arrays.asList("foo -- b;ar");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testMultilineStatementWithComment() {
        String str = "foo -- b;ar\nbaz";
        List expected = Arrays.asList("foo -- b;ar\nbaz");
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(str));
    }

    @Test
    public void testRealLifeExample() {
        String firstStatamenet =
                "CREATE TABLE serde_test (\n" +
                        "  key STRING,\n" +
                        "  value STRING\n" +
                        ")\n" +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'\n" +
                        "WITH SERDEPROPERTIES  (\n" +
                        "\"input.regex\" = \"(.*);\"                                       \n" +
                        ")\n" +
                        "STORED AS TEXTFILE\n" +
                        "LOCATION '${hiveconf:hadoop.tmp.dir}/serde'";

        String secondStatamenet = "select * from foobar";


        Assert.assertEquals(Arrays.asList(firstStatamenet, secondStatamenet),
                StatementsSplitter.splitStatements(firstStatamenet + ";\n" + secondStatamenet + ";\n"));
    }

    @Test
    public void realLifeWithComments() {
        String firstStatement =
                "CREATE TABLE ${hiveconf:TARGET_SCHEMA_NAME}.pacc_pstatus (\n" +
                        "  cid\tstring, -- The cid of the transaction the balance change is connected to\n" +
                        "  create_date string , -- the date of the pstatus change\n" +
                        "  old_pstatus string, -- The pstatus before the change\n" +
                        "  new_pstatus string, -- The pstatus after the change\n" +
                        "  manual boolean -- true of the pstatus change is manual, currently false for all changes " +
                        "since we can't know about manual pstatus changes\n" +
                        "  -- PRIMARY KEY() -- there no natural primary key for this table, should we add one, e.g. " +
                        "rowno?\n" +
                        "  )";

        Assert.assertEquals(Arrays.asList(firstStatement),
                StatementsSplitter.splitStatements(firstStatement + ";\n"));
    }


    @Test
    public void testPreserveQuoted() {
        List<String> expected = Arrays.asList("\"foo\"", "'bar'", "\"\''\"", "'\"\\\"'", "';'", "\";\"");
        String input = Joiner.on(";").join(expected);
        Assert.assertEquals(expected, StatementsSplitter.splitStatements(input));
    }

    @Test
    public void testReadQuoted() {
        String firstQuote = "\"foo;\\; b  a r\\\"\"";
        String secondQuote = "'foo;\\; \\'b  a r\\\"'";
        String expectedTail = "'\'\"foxlov  e \"";

        String expression = firstQuote + secondQuote + expectedTail;

        StringTokenizer tokenizer = new StringTokenizer(expression, StatementsSplitter.SQL_SPECIAL_CHARS, true);
        String actualFirstQuote = StatementsSplitter.readQuoted(tokenizer, (String) tokenizer.nextElement());
        String actualSecondQuote = StatementsSplitter.readQuoted(tokenizer, (String) tokenizer.nextElement());

        String actualTail = "";
        while (tokenizer.hasMoreElements()) {
            actualTail += tokenizer.nextElement();
        }

        Assert.assertEquals(Arrays.asList(firstQuote, secondQuote, expectedTail),
                Arrays.asList(actualFirstQuote, actualSecondQuote, actualTail));

    }

    @Test
    public void testReadUntilEndOfLine() {
        StringTokenizer tokenizer = new StringTokenizer("foo\nbar\n\n\nbaz", StatementsSplitter.SQL_SPECIAL_CHARS, true);
        Assert.assertEquals("foo\n", StatementsSplitter.readUntilEndOfLine(tokenizer));
        Assert.assertEquals("bar\n", StatementsSplitter.readUntilEndOfLine(tokenizer));
        Assert.assertEquals("\n", StatementsSplitter.readUntilEndOfLine(tokenizer));
        Assert.assertEquals("\n", StatementsSplitter.readUntilEndOfLine(tokenizer));
        Assert.assertEquals("baz", StatementsSplitter.readUntilEndOfLine(tokenizer));
        Assert.assertEquals("", StatementsSplitter.readUntilEndOfLine(tokenizer));
    }

}