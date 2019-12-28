/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlDialect;

/**
 * A <code>SqlDialect</code> implementation for the Infobright database.
 */
public class InfobrightSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.INFOBRIGHT)
      .withIdentifierQuoteString("`");

  public static final SqlDialect DEFAULT = new InfobrightSqlDialect(DEFAULT_CONTEXT);

  /** Creates an InfobrightSqlDialect. */
  public InfobrightSqlDialect(Context context) {
    super(context);
  }
<<<<<<< HEAD
=======

  @Override public boolean supportsWindowFunctions() {
    return false;
  }

}
<<<<<<< HEAD
>>>>>>> 70f226063 ([CALCITE-2206] JDBC adapter incorrectly pushes windowed aggregates down to HSQLDB (Pavel Gubin))

  @Override public boolean supportsWindowFunctions() {
    return false;
  }

}
=======
>>>>>>> 8900eaa6c ([CALCITE-3623] Run autostyleApply)
