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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import com.google.common.base.Preconditions;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The name-resolution scope of a LATERAL TABLE clause.
 *
 * <p>The objects visible are those in the parameters found on the left side of
 * the LATERAL TABLE clause, and objects inherited from the parent scope.
 */
public class LateralTableScope extends ListScope {
  private final SqlNode node;

<<<<<<< HEAD:core/src/main/java/org/apache/calcite/sql/validate/LateralTableScope.java
=======
  //~ Constructors -----------------------------------------------------------

>>>>>>> ebe036aed (DX-7801 [CALCITE-3752] Add PIVOT operator to SQL):core/src/main/java/org/apache/calcite/sql/validate/TableScope.java
  /**
   * Creates a scope corresponding to a LATERAL TABLE clause.
   *
   * @param parent  Parent scope
   */
<<<<<<< HEAD:core/src/main/java/org/apache/calcite/sql/validate/LateralTableScope.java
  LateralTableScope(SqlValidatorScope parent, SqlNode node) {
    super(Objects.requireNonNull(parent, "parent"));
    this.node = Objects.requireNonNull(node, "node");
  }

  @Override public SqlNode getNode() {
    return node;
  }

  @Override public boolean isWithin(@Nullable SqlValidatorScope scope2) {
    return this == scope2
        || (scope2 instanceof JoinScope && node == scope2.getNode())
        || super.isWithin(scope2);
=======
  TableScope(SqlValidatorScope parent, SqlNode node) {
    super(Preconditions.checkNotNull(parent));
    this.node = Preconditions.checkNotNull(node);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return node;
  }

  @Override public boolean isWithin(SqlValidatorScope scope2) {
    if (this == scope2) {
      return true;
    }
    SqlValidatorScope s = getValidator().getSelectScope((SqlSelect) node);
    return s.isWithin(scope2);
>>>>>>> ebe036aed (DX-7801 [CALCITE-3752] Add PIVOT operator to SQL):core/src/main/java/org/apache/calcite/sql/validate/TableScope.java
  }
}
