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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
>>>>>>> 2a57079b4 ([CALCITE-2744] RelDecorrelator use wrong output map for LogicalAggregate decorrelate (godfreyhe and Danny Chan))
  }

  /** "RAMP" user-defined function. */
  public static class RampFunction extends SqlFunction {
    public RampFunction() {
      super("RAMP",
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.builder()
          .add("I", SqlTypeName.INTEGER)
          .build();
    }
  }

  /** "DEDUP" user-defined table function. */
  public static class DedupFunction extends SqlFunction {
    public DedupFunction() {
      super("DEDUP",
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.builder()
          .add("NAME", SqlTypeName.VARCHAR, 1024)
          .build();
    }
  }
<<<<<<< HEAD
=======

  /** "MY_AVG" user-defined aggregate function. */
  public static class MyAvgAggFunction extends SqlAggFunction {
    public MyAvgAggFunction() {
      super("MY_AVG",
          null,
          SqlKind.AVG,
          ReturnTypes.AVG_AGG_FUNCTION,
          null,
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          SqlFunctionCategory.NUMERIC,
          false,
          false);
    }

    @Override public boolean isDeterministic() {
      return false;
    }
  }
}

  /** "MY_AVG" user-defined aggregate function, with two arguments. */
  public static class MyAvgAggFunction extends SqlAggFunction {
    public MyAvgAggFunction() {
      super("MY_AVG", null, SqlKind.AVG, ReturnTypes.AVG_AGG_FUNCTION,
          null, OperandTypes.NUMERIC_NUMERIC, SqlFunctionCategory.NUMERIC,
          false, false, Optionality.FORBIDDEN);
    }

    @Override public boolean isDeterministic() {
      return false;
    }
  }
  /** "STRUCTURED_FUNC" user-defined function whose return type is structured type. */
  public static class StructuredFunction extends SqlFunction {
    StructuredFunction() {
      super("STRUCTURED_FUNC", new SqlIdentifier("STRUCTURED_FUNC", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION, null, null, OperandTypes.NILADIC, null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      final RelDataType varcharType =
          typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
      return typeFactory.builder()
          .add("F0", bigintType)
          .add("F1", varcharType)
          .build();
    }
  }

  /** "MYFUN" user-defined scalar function. */
  public static class MyFunction extends SqlFunction {
    public MyFunction() {
      super("MYFUN",
          new SqlIdentifier("MYFUN", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.NUMERIC,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }
  }
}
