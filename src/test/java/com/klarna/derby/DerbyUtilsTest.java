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

package com.klarna.derby;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

public class DerbyUtilsTest {


    private String url;

    @Before
    public void setup() throws SQLException {
        url = "jdbc:derby:memory:" + UUID.randomUUID().toString();

        Connection connection = DriverManager.getConnection(url + ";create=true");

        // Ping derby just to make sure we got it up and running
        connection.prepareCall("select * from SYS.SYSTABLES").executeQuery();
    }


    @Test(expected = IllegalStateException.class)
    public void unproperCallToShutdownShouldThrowException() throws SQLException {
        DerbyUtils.dropDerbyDatabase(url + "FOO");
    }


    @Test
    public void shutdownDerbyShouldNotThrowException() throws SQLException {
        // Successful drop of a derby database is signalled by an SQLException. However, we expect the
        // DerbyUtils implementation to catch that.
        try {
            DerbyUtils.dropDerbyDatabase(url);
        } catch (Throwable e) {
            Assert.fail("Exception was not expected here");
        }

    }


    @Test(expected = SQLException.class)
    public void verifyThatDerbyShutdowns() throws SQLException {
        DerbyUtils.dropDerbyDatabase(url);

        // Attempt to connect to dropped database should result in a SQLException
        DriverManager.getConnection(url);
    }
}
