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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
        new ExplicitTablesSchema(tableMap));

    return new CalciteCatalogReader(
        schema,
        new ArrayList<>(new ArrayList<>()),
        typeFactory,
        connectionConfig);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over an expression, copying every node, and fully-qualifying every
   * identifier.
   */
  @Deprecated // to be removed before 2.0
  public static class DeepCopier extends SqlScopedShuttle {
    DeepCopier(SqlValidatorScope scope) {
      super(scope);
    }

    /** Copies a list of nodes. */
    public static SqlNodeList copy(SqlValidatorScope scope, SqlNodeList list) {
      //noinspection deprecation
      return (SqlNodeList) list.accept(new DeepCopier(scope));
    }

    public SqlNode visit(SqlNodeList list) {
      SqlNodeList copy = new SqlNodeList(list.getParserPosition());
      for (SqlNode node : list) {
        copy.add(node.accept(this));
      }
      return copy;
    }

    // Override to copy all arguments regardless of whether visitor changes
    // them.
    protected SqlNode visitScoped(SqlCall call) {
      ArgHandler<SqlNode> argHandler =
          new CallCopyingArgHandler(call, true);
      call.getOperator().acceptCall(this, call, false, argHandler);
      return argHandler.result();
    }

    public SqlNode visit(SqlLiteral literal) {
      return SqlNode.clone(literal);
    }

    public SqlNode visit(SqlIdentifier id) {
      // First check for builtin functions which don't have parentheses,
      // like "LOCALTIME".
      SqlValidator validator = getScope().getValidator();
      final SqlCall call = validator.makeNullaryCall(id);
      if (call != null) {
        return call;
      }

      return getScope().fullyQualify(id).identifier;
    }

    public SqlNode visit(SqlDataTypeSpec type) {
      return SqlNode.clone(type);
    }

    public SqlNode visit(SqlDynamicParam param) {
      return SqlNode.clone(param);
    }

    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return SqlNode.clone(intervalQualifier);
    }
  }

  /** Suggests candidates for unique names, given the number of attempts so far
   * and the number of expressions in the project list. */
  public interface Suggester {
    String apply(String original, int attempt, int size);
  }

  public static final Suggester EXPR_SUGGESTER =
      (original, attempt, size) -> Util.first(original, "EXPR$") + attempt;

  public static final Suggester F_SUGGESTER =
      (original, attempt, size) -> Util.first(original, "$f")
          + Math.max(size, attempt);

  public static final Suggester ATTEMPT_SUGGESTER =
      (original, attempt, size) -> Util.first(original, "$") + attempt;

  /** Builds a list of GROUP BY expressions. */
  static class GroupAnalyzer {
    /** Extra expressions, computed from the input as extra GROUP BY
     * expressions. For example, calls to the {@code TUMBLE} functions. */
    final List<SqlNode> extraExprs = new ArrayList<>();
    final List<SqlNode> groupExprs;
    final Map<Integer, Integer> groupExprProjection = new HashMap<>();
    int groupCount;

    GroupAnalyzer(List<SqlNode> groupExprs) {
      this.groupExprs = groupExprs;
    }

    SqlNode createGroupExpr() {
      // TODO: create an expression that could have no other source
      return SqlLiteral.createCharString("xyz" + groupCount++,
          SqlParserPos.ZERO);
    }
  }
<<<<<<< HEAD
=======

  /**
   * A {@link AbstractTable} that can specify the row type explicitly.
   */
  private static class ExplicitRowTypeTable extends AbstractTable {
    private final RelDataType rowType;

    ExplicitRowTypeTable(RelDataType rowType) {
      this.rowType = Objects.requireNonNull(rowType);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return this.rowType;
    }
  }

  /**
   * A {@link AbstractSchema} that can specify the table map explicitly.
   */
  private static class ExplicitTablesSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    ExplicitTablesSchema(Map<String, Table> tableMap) {
      this.tableMap = Objects.requireNonNull(tableMap);
    }

    @Override protected Map<String, Table> getTableMap() {
      return tableMap;
    }
  }
}

  /**
   * A {@link AbstractTable} that can specify the row type explicitly.
   */
  private static class ExplicitRowTypeTable extends AbstractTable {
    private final RelDataType rowType;

    ExplicitRowTypeTable(RelDataType rowType) {
      this.rowType = Objects.requireNonNull(rowType);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return this.rowType;
    }
  }

  /**
   * A {@link AbstractSchema} that can specify the table map explicitly.
   */
  private static class ExplicitTableSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    ExplicitTableSchema(Map<String, Table> tableMap) {
      this.tableMap = Objects.requireNonNull(tableMap);
    }

    @Override protected Map<String, Table> getTableMap() {
      return tableMap;
    }
  }
}
