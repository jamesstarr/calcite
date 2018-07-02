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

import org.apache.calcite.avatica.util.TimeUnit;
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          SqlTypeName sqlTypeName =
              opBinding.getOperandLiteralValue(0, TimeUnit.class) == TimeUnit.NANOSECOND
                  ? SqlTypeName.BIGINT
                  : SqlTypeName.INTEGER;
          return typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(sqlTypeName),
              opBinding.getOperandType(1).isNullable()
              || opBinding.getOperandType(2).isNullable());
        }
      };

  SqlTimestampDiffFunction() {
    super("TIMESTAMPDIFF", SqlKind.TIMESTAMP_DIFF,
        RETURN_TYPE_INFERENCE, null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATETIME,
            SqlTypeFamily.DATETIME),
        SqlFunctionCategory.TIMEDATE);
  }
}
