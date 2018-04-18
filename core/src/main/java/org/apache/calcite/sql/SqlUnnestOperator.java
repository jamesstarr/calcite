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

        // When there is one operand with unknown type (ANY), the return type
        // is dynamic star
        return builder
            .add(DynamicRecordType.DYNAMIC_STAR_PREFIX,
                SqlTypeName.DYNAMIC_STAR)
            .nullable(true)
            .buildDynamic();
      }

      if (type.isStruct()) {
        type = type.getFieldList().get(0).getType();
      }
      assert type instanceof ArraySqlType || type instanceof MultisetSqlType
          || type instanceof MapSqlType;
      if (type instanceof MapSqlType) {
        builder.add(MAP_KEY_COLUMN_NAME, type.getKeyType());
        builder.add(MAP_VALUE_COLUMN_NAME, type.getValueType());
      } else {
        if (!allowAliasUnnestItems(opBinding) && type.getComponentType().isStruct()) {
          builder.addAll(type.getComponentType().getFieldList());
        } else {
          builder.add(SqlUtil.deriveAliasFromOrdinal(operand),
              type.getComponentType());
        }
      }
    }
    if (withOrdinality) {
      builder.add(ORDINALITY_COLUMN_NAME, SqlTypeName.INTEGER);
    }
    return builder.build();
  }

  private boolean allowAliasUnnestItems(SqlOperatorBinding operatorBinding) {
    return (operatorBinding instanceof SqlCallBinding)
        && ((SqlCallBinding) operatorBinding)
        .getValidator()
        .config()
        .sqlConformance()
        .allowAliasUnnestItems();
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    if (call.operandCount() == 1
        && call.getOperandList().get(0).getKind() == SqlKind.SELECT) {
      // avoid double ( ) on unnesting a sub-query
      writer.keyword(getName());
      call.operand(0).unparse(writer, 0, 0);
    } else {
      super.unparse(writer, call, leftPrec, rightPrec);
    }
    if (withOrdinality) {
      writer.keyword("WITH ORDINALITY");
    }
  }

  public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }

}
