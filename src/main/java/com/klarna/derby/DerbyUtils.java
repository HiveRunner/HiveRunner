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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Collection of Derby related helper functions.
 */
public final class DerbyUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DerbyUtils.class);

    private DerbyUtils() {
    }

    /**
     * Drops the derby data base. Data base will no longer be available
     */
    public static void dropDerbyDatabase(String url) {
        try {
            DriverManager.getConnection(url + ";drop=true");
        } catch (SQLException e) {
            if (e.getSQLState().equals("08006")) {
                LOGGER.debug("Dropped derby db. Exception is expected but make sure it's the right one!: "
                        + e.getMessage(), e);
            } else {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

    }

}
