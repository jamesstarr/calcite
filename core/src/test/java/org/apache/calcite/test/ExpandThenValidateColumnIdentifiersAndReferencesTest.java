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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.test.DefaultSqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlTesterImpl;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.base.Preconditions;

import org.junit.Test;

import java.util.Arrays;

/***
 * Ensure that column {@code SqlIdentifier} are expanded before resolution.
 */
public class ExpandThenValidateColumnIdentifiersAndReferencesTest extends SqlValidatorTestCase {

  @Override public SqlTester getTester() {
    return new SqlTesterImpl(
            DefaultSqlTestFactory.INSTANCE
    ) {
      @Override public SqlValidator getValidator() {
        SqlOperatorTable sqlOperatorTable =
                DefaultSqlTestFactory.INSTANCE.createOperatorTable(DefaultSqlTestFactory.INSTANCE);
        JavaTypeFactoryImpl javaTypeFactoryImpl =
                new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        MockCatalogReader catalogReader = DefaultSqlTestFactory.INSTANCE.createCatalogReader(
                DefaultSqlTestFactory.INSTANCE,
                javaTypeFactoryImpl
        );
        SqlValidatorImplForTestingExpandThenValidate impl =
                new SqlValidatorImplForTestingExpandThenValidate(
                  sqlOperatorTable,
                  catalogReader,
                  javaTypeFactoryImpl,
                  SqlConformanceEnum.LENIENT);
        impl.setIdentifierExpansion(true);
        impl.setColumnReferenceExpansion(true);
        return impl;
      }
    };
  }


  @Test void testRewriteWithColumnReferenceExpansion() {
    sql(
        "select unexpanded.deptno from dept \n"
            + " where unexpanded.name = 'Moonracer' \n"
            + " group by unexpanded.deptno\n"
            + " having sum(unexpanded.deptno) > 0\n"
            + " order by unexpanded.deptno"
    ).ok();
  }
}

/***
 * Rewrites columnar sql identifiers 'UNEXPANDED'.'Something' to 'DEPT'.'Something', where
 * 'Something' is any string.
 */
class SqlValidatorImplForTestingExpandThenValidate
    extends SqlValidatorImpl {

  protected SqlValidatorImplForTestingExpandThenValidate(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance
  ) {
    super(opTab, catalogReader, typeFactory, conformance);
  }

  @Override public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
    SqlNode rewrittenNode = rewriteNode(expr);
    return super.expand(rewrittenNode, scope);
  }

//  @Override public SqlNode expandSelectExpr(SqlNode expr, SelectScope scope, SqlSelect select) {
//    SqlNode rewrittenNode = rewriteNode(expr);
//    return super.expandSelectExpr(rewrittenNode, scope, select);
//  }

  @Override public SqlNode expandGroupByOrHavingExpr(
      SqlNode expr,
      SqlValidatorScope scope,
      SqlSelect select,
      boolean havingExpression
  ) {
    SqlNode rewrittenNode = rewriteNode(expr);
    return super.expandGroupByOrHavingExpr(rewrittenNode, scope, select, havingExpression);
  }

  private SqlNode rewriteNode(SqlNode sqlNode) {
    return sqlNode.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlIdentifier id) {
        return rewriteIdentifier(id);
      }
    });
  }

  private SqlIdentifier rewriteIdentifier(SqlIdentifier sqlIdentifier) {
    Preconditions.checkArgument(sqlIdentifier.names.size() == 2);
    if (sqlIdentifier.names.get(0).equals("UNEXPANDED")) {
      return new SqlIdentifier(
          Arrays.asList("DEPT", sqlIdentifier.names.get(1)),
          null,
          sqlIdentifier.getParserPosition(),
          Arrays.asList(
              sqlIdentifier.getComponentParserPosition(0),
              sqlIdentifier.getComponentParserPosition(1)
          )
      );
    } else if (sqlIdentifier.names.get(0).equals("DEPT")) {
      //  Identifiers are expanded multiple times
      return sqlIdentifier;
    } else {
      throw new RuntimeException("Unknown Identifier " + sqlIdentifier);
    }
  }
}
// End ExpandThenValidateColumnIdentifiersAndReferencesTest.java
