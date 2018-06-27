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
<<<<<<<< HEAD:core/src/main/java/org/apache/calcite/sql/SqlJsonConstructorNullClause.java
package org.apache.calcite.sql;

/**
 * Indicating that how do Json constructors handle null
 */
public enum SqlJsonConstructorNullClause {
  NULL_ON_NULL("NULL ON NULL"),
  ABSENT_ON_NULL("ABSENT ON NULL");

  public final String sql;

  SqlJsonConstructorNullClause(String sql) {
    this.sql = sql;
  }
}
========
package org.apache.calcite.adapter.elasticsearch;

/**
 * Thrown when {@link org.apache.calcite.rel.RelNode} expression can't be processed
 * (or converted into ES query)
 */
class ExpressionNotAnalyzableException extends Exception {
  ExpressionNotAnalyzableException(String message, Throwable cause) {
    super(message, cause);
  }
}

// End ExpressionNotAnalyzableException.java
>>>>>>>> 8c96cf58c ([CALCITE-2376] Unify ES2 and ES5 adapters. Migrate to low-level ES rest client as main transport. (Andrei Sereda)):elasticsearch/src/main/java/org/apache/calcite/adapter/elasticsearch/ExpressionNotAnalyzableException.java
