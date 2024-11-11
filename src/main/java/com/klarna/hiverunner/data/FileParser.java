/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner.data;

import java.io.File;
import java.util.List;

import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * A {@link File} parsing class to be used with {@link InsertIntoTable} for inserting data into a Hive table from a
 * {@link File}.
 */
public interface FileParser {

    /**
     * Parses the given file and returns the rows with the requested columns.
     *
     * @param file   The file to be parsed.
     * @param schema The full schema of the Hive table.
     * @param names  The requested field names.
     * @return A {@link List} of rows, each represented by an {@link Object} array.
     */
    List<Object[]> parse(File file, HCatSchema schema, List<String> names);

    /**
     * Parses the given file and returns the column names that are available in the file.
     *
     * @param file The file to be parsed
     * @return A {@link List} of column names as Strings
     */
    List<String> getColumnNames(File file);

    /**
     * Method that checks if the parser has access to column names.
     *
     * @return
     */
    boolean hasColumnNames();
}
