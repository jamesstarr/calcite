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
package org.apache.calcite.sql;

<<<<<<< HEAD:core/src/main/java/org/apache/calcite/sql/Symbolizable.java
import org.apache.calcite.sql.parser.SqlParserPos;

/** Sub-class should be a Enum and can convert to a {@link SqlLiteral}. */
public interface Symbolizable {
=======
/**
 * Specifies the type of correlation operation: inner, left, semi, or anti.
 * @deprecated Use {@link JoinType}
 */
@Deprecated // to be removed before 1.21
public enum CorrelateJoinType {
  /**
   * Inner join
   */
  INNER,
>>>>>>> 0a404408f ([CALCITE-3119] Deprecate Linq4j CorrelateJoinType (in favor of JoinType)):linq4j/src/main/java/org/apache/calcite/linq4j/CorrelateJoinType.java

  /**
   * Creates a parse-tree node representing an occurrence of this keyword
   * at a particular position in the parsed text.
   */
<<<<<<< HEAD:core/src/main/java/org/apache/calcite/sql/Symbolizable.java
  default SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol((Enum<?>) this, pos);
=======
  LEFT,

  /**
   * Semi-join.
   *
   * <p>Similar to {@code from A ... where a in (select b from B ...)}</p>
   */
  SEMI,

  /**
   * Anti-join.
   *
   * <p>Similar to {@code from A ... where a NOT in (select b from B ...)}
   *
   * <p>Note: if B.b is nullable and B has nulls, no rows must be returned.
   */
  ANTI;

  /** Transforms this CorrelateJoinType to JoinType. **/
  public JoinType toJoinType() {
    switch (this) {
    case INNER:
      return JoinType.INNER;
    case LEFT:
      return JoinType.LEFT;
    case SEMI:
      return JoinType.SEMI;
    case ANTI:
      return JoinType.ANTI;
    }
    throw new IllegalStateException(
        "Unable to convert " + this + " to JoinType");
>>>>>>> 0a404408f ([CALCITE-3119] Deprecate Linq4j CorrelateJoinType (in favor of JoinType)):linq4j/src/main/java/org/apache/calcite/linq4j/CorrelateJoinType.java
  }
}
