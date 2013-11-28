/*
 * Copyright 2013 Klarna AB
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

package com.klarna.hiverunner;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HiveServerContainerTest {

    @Test
    public void testSplitBasic() {
        String str = "foo;bar;baz";
        List expected = Arrays.asList("foo", "bar", "baz");
        Assert.assertEquals(expected, Arrays.asList(new HiveServerContainer().splitStatements(str)));
    }

    @Test
    public void testDontSplitBackslashSemicolon() {
        String str = "bar\\;baz";
        List expected = Arrays.asList("bar\\;baz");
        Assert.assertEquals(expected, Arrays.asList(new HiveServerContainer().splitStatements(str)));
    }

    /**
     * This test shows that the split statement method is really basic.
     * The current version will not cater for semicolon within quotes etc.
     * <p/>
     * TODO: Support quoted semicolons
     */
    @Test(expected = AssertionError.class)
    public void methodWrongfullySplitsOnQuotedDelimiter() {
        String str = "bar\";\"\\baz";
        List expected = Arrays.asList(str);
        Assert.assertEquals(expected, Arrays.asList(new HiveServerContainer().splitStatements(str)));
    }
}
