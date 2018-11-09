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
package org.apache.calcite.test;

import org.apache.calcite.adapter.os.OsAdapterTest;
import org.apache.calcite.adapter.tpcds.TpcdsTest;
import org.apache.calcite.adapter.tpch.TpchTest;
import org.apache.calcite.chinook.EndToEndTest;
import org.apache.calcite.chinook.RemotePreparedStatementParametersTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Supported json encodings that could be passed to a
 * {@code JsonValueExpression}.
 */
<<<<<<< HEAD:core/src/main/java/org/apache/calcite/sql/SqlJsonEncoding.java
=======
package org.apache.calcite.sql;

/**
 * SqlJsonEncoding lists the supported json encodings that could be passed to JsonValueExpression.
 */
>>>>>>> 4ae6fc9d7 ([CALCITE-2266] Implement SQL:2016 JSON functions: JSON_EXISTS, JSON_VALUE, JSON_QUERY, JSON_OBJECT, JSON_OBJECTAGG, JSON_ARRAY, JSON_ARRAYAGG, IS JSON predicate (Hongze Zhang))
public enum SqlJsonEncoding {
  UTF8("UTF8"),
  UTF16("UTF16"),
  UTF32("UTF32");
>>>>>>> 4ae6fc9d7 ([CALCITE-2266] Implement SQL:2016 JSON functions: JSON_EXISTS, JSON_VALUE, JSON_QUERY, JSON_OBJECT, JSON_OBJECTAGG, JSON_ARRAY, JSON_ARRAYAGG, IS JSON predicate (Hongze Zhang))

  private final String standardName;

  SqlJsonEncoding(String standardName) {
    this.standardName = standardName;
  }

  public String getStandardName() {
    return standardName;
  }

  @Override public String toString() {
    return getStandardName();
  }
}
<<<<<<< HEAD
=======

// End SqlJsonEncoding.java
