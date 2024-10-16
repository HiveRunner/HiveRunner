/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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
package com.klarna.hiverunner.builder;

import java.nio.file.Path;

public class HiveRunnerScript implements Script {

    private final Path path;
    private final String sqlText;
    private final int index;

    public HiveRunnerScript(int index, Path path, String sqlText) {
        this.index = index;
        this.path = path;
        this.sqlText = sqlText;
    }

    @Override
    public int getIndex() {
        return index;
    }

    /* (non-Javadoc)
     * @see com.klarna.hiverunner.builder.Script#getPath()
     */
    @Override
    public Path getPath() {
        return path;
    }

    /* (non-Javadoc)
     * @see com.klarna.hiverunner.builder.Script#getSqlText()
     */
    @Override
    public String getSql() {
        return sqlText;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + index;
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result + ((sqlText == null) ? 0 : sqlText.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HiveRunnerScript other = (HiveRunnerScript) obj;
        if (index != other.index)
            return false;
        if (path == null) {
            if (other.path != null)
                return false;
        } else if (!path.equals(other.path))
            return false;
        if (sqlText == null) {
            if (other.sqlText != null)
                return false;
        } else if (!sqlText.equals(other.sqlText))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "HiveRunnerScript [path=" + path + ", sqlText=" + sqlText + ", index=" + index + "]";
    }

}
