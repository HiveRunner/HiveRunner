/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) ${license.git.copyrightYears} The HiveRunner Contributors
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
package com.klarna.hiverunner.sql.cli.hive;

import com.klarna.hiverunner.sql.cli.CommentUtil;
import com.klarna.hiverunner.sql.cli.PreProcessor;

/**
 * A {@link PreProcessor} that strips comments from scripts only, replicating
 * Hive CLI's broken functionality present in versions <2.0.0. This is described
 * in <a href="https://issues.apache.org/jira/browse/HIVE-8396">HIVE-8396</a>.
 * <p>
 * Full line comments are stripped from script files as is the case with both
 * {@code hive -f} and {@code beeline -f}. The implementations provided here
 * replicate these modes of operation.
 * </p>
 */
enum PreV200HiveCliPreProcessor implements PreProcessor {
    INSTANCE;

    @Override
    public String script(String script) {
        return CommentUtil.stripFullLineComments(script);
    }

    @Override
    public String statement(String statement) {
        return statement;
    };
}
