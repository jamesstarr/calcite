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
package org.apache.calcite.rex;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.IndexExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RexImplicationChecker;
  @Override public boolean implies(RexBuilder rexBuilder, RelDataType rowType, RexNode first,
      RexNode second) {
    RexImplicationChecker checker = new RexImplicationChecker(rexBuilder, this, rowType);
    return checker.implies(first, second);
  }

  /**
   * Implementation of
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter}
   * that reads the values of input fields by calling
   * <code>{@link org.apache.calcite.DataContext#get}("inputRecord")</code>.
   */
  private static class DataContextInputGetter implements InputGetter {
    private final RelDataTypeFactory typeFactory;
    private final RelDataType rowType;

    DataContextInputGetter(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      MethodCallExpression recFromCtx = Expressions.call(
          DataContext.ROOT,
          BuiltInMethod.DATA_CONTEXT_GET.method,
          Expressions.constant("inputRecord"));
      Expression recFromCtxCasted =
          EnumUtils.convert(recFromCtx, Object[].class);
      IndexExpression recordAccess = Expressions.arrayIndex(recFromCtxCasted,
          Expressions.constant(index));
      if (storageType == null) {
        final RelDataType fieldType =
            rowType.getFieldList().get(index).getType();
        storageType = ((JavaTypeFactory) typeFactory).getJavaClass(fieldType);
      }
      return EnumUtils.convert(recordAccess, storageType);
    }
  }
}
