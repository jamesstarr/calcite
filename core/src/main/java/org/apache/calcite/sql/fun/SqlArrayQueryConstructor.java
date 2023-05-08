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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * Definition of the SQL:2003 standard ARRAY query constructor, <code>
 * ARRAY (&lt;query&gt;)</code>.
 */
public class SqlArrayQueryConstructor extends SqlMultisetQueryConstructor {
  //~ Constructors -----------------------------------------------------------

  public SqlArrayQueryConstructor() {
    super("ARRAY", SqlKind.ARRAY_QUERY_CONSTRUCTOR);
  }

  @Override public RelDataType deriveType(
          SqlValidator validator,
          SqlValidatorScope scope,
          SqlCall call) {
    // This is a stop gap until we have [CALCITE-4999]
    SqlSelect subSelect = call.operand(0);
    subSelect.validateExpr(validator, scope);

    SqlValidatorNamespace ns = validator.getNamespace(subSelect);
    assert null != ns.getRowType();

    boolean isScalar = (subSelect.getSelectList().size() == 1)
            && !subSelect.getSelectList().get(0).toString().equals("*");
    RelDataType elementType = isScalar
            ? ns.getRowType().getFieldList().get(0).getType()
            : ns.getRowType();

    return SqlTypeUtil.createArrayType(
            validator.getTypeFactory(),
            elementType,
            false);
  }
}

// End SqlArrayQueryConstructor.java
