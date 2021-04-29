/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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
package com.klarna.hiverunner.sql;

import com.klarna.hiverunner.builder.Statement;

public class HiveRunnerStatement implements Statement {

  private final int index;
  private final String sql;

  public HiveRunnerStatement(int index, String sql) {
    this.index = index;
    this.sql = sql;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public String getSql() {
    return sql;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + index;
    result = prime * result + ((sql == null) ? 0 : sql.hashCode());
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
    HiveRunnerStatement other = (HiveRunnerStatement) obj;
    if (index != other.index)
      return false;
    if (sql == null) {
      if (other.sql != null)
        return false;
    } else if (!sql.equals(other.sql))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "HiveRunnerStatement [index=" + index + ", sql=" + sql + "]";
  }

  
}
