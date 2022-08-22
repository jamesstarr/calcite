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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlDotOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercions;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Default implementation of {@link SqlValidator}.
 */
public class SqlValidatorImpl implements SqlValidatorWithHints {
  //~ Static fields/initializers ---------------------------------------------

  public static final Logger TRACER = CalciteTrace.PARSER_LOGGER;

  /**
   * Alias generated for the source table when rewriting UPDATE to MERGE.
   */
  public static final String UPDATE_SRC_ALIAS = "SYS$SRC";

  /**
   * Alias generated for the target table when rewriting UPDATE to MERGE if no
   * alias was specified by the user.
   */
  public static final String UPDATE_TGT_ALIAS = "SYS$TGT";

  /**
   * Alias prefix generated for source columns when rewriting UPDATE to MERGE.
   */
  public static final String UPDATE_ANON_PREFIX = "SYS$ANON";

  //~ Instance fields --------------------------------------------------------

  private final SqlOperatorTable opTab;
  final SqlValidatorCatalogReader catalogReader;

  /**
   * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
   * objects at these positions
   */
  protected final Map<String, IdInfo> idPositions = new HashMap<>();

  /**
   * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
   * scope created from them.
   */
  protected final Map<SqlNode, SqlValidatorScope> scopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
   * clauses.
   */
  private final Map<SqlSelect, SqlValidatorScope> whereScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its GROUP BY clause.
   */
  private final Map<SqlSelect, SqlValidatorScope> groupByScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
   * clauses.
   */
  private final Map<SqlSelect, SqlValidatorScope> selectScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
   */
  private final Map<SqlSelect, SqlValidatorScope> orderScopes =
      new IdentityHashMap<>();

  /**
   * Maps a {@link SqlSelect} node that is the argument to a CURSOR
   * constructor to the scope of the result of that select node
   */
  private final Map<SqlSelect, SqlValidatorScope> cursorScopes =
      new IdentityHashMap<>();

  /**
   * The name-resolution scope of a LATERAL TABLE clause.
   */
  private TableScope tableScope = null;

  /**
   * Maps a {@link SqlNode node} to the
   * {@link SqlValidatorNamespace namespace} which describes what columns they
   * contain.
   */
  protected final Map<SqlNode, SqlValidatorNamespace> namespaces =
      new IdentityHashMap<>();

  /**
   * Set of select expressions used as cursor definitions. In standard SQL,
   * only the top-level SELECT is a cursor; Calcite extends this with
   * cursors as inputs to table functions.
   */
  private final Set<SqlNode> cursorSet = Sets.newIdentityHashSet();

  /**
   * Stack of objects that maintain information about function calls. A stack
   * is needed to handle nested function calls. The function call currently
   * being validated is at the top of the stack.
   */
  protected final Deque<FunctionParamInfo> functionCallStack =
      new ArrayDeque<>();

  private int nextGeneratedId;
  protected final RelDataTypeFactory typeFactory;

  /** The type of dynamic parameters until a type is imposed on them. */
  protected final RelDataType unknownType;
  private final RelDataType booleanType;

  /**
   * Map of derived RelDataType for each node. This is an IdentityHashMap
   * since in some cases (such as null literals) we need to discriminate by
   * instance.
   */
  private final Map<SqlNode, RelDataType> nodeToTypeMap =
      new IdentityHashMap<>();
  private final AggFinder aggFinder;
  private final AggFinder aggOrOverFinder;
  private final AggFinder aggOrOverOrGroupFinder;
  private final AggFinder groupFinder;
  private final AggFinder overFinder;
  private Config config;
  private final Map<SqlNode, SqlNode> originalExprs = new HashMap<>();

  private SqlNode top;

  private NullCollation nullCollation = NullCollation.HIGH;

  // TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
  // if it's OK to expand the signature of that method.
  private boolean validatingSqlMerge;

  private boolean inWindow;                        // Allow nested aggregates

  private final SqlValidatorImpl.ValidationErrorFunction validationErrorFunction =
      new SqlValidatorImpl.ValidationErrorFunction();

  // TypeCoercion instance used for implicit type coercion.
  private TypeCoercion typeCoercion;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param config        Config
>>>>>>> e62a96083 ([CALCITE-3900] Add Config for SqlValidator)
   */
  @Deprecated // to be removed before 1.24
  protected SqlValidatorImpl(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      Config config) {
    this.opTab = Preconditions.checkNotNull(opTab);
    this.catalogReader = Preconditions.checkNotNull(catalogReader);
    this.typeFactory = Preconditions.checkNotNull(typeFactory);
    this.config = Preconditions.checkNotNull(config);

    unknownType = typeFactory.createUnknownType();
    booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
    aggFinder = new AggFinder(opTab, false, true, false, null, nameMatcher);
    aggOrOverFinder =
        new AggFinder(opTab, true, true, false, null, nameMatcher);
    overFinder = new AggFinder(opTab, true, false, false, aggOrOverFinder,
        nameMatcher);
    groupFinder = new AggFinder(opTab, false, false, true, null, nameMatcher);
    aggOrOverOrGroupFinder = new AggFinder(opTab, true, true, true, null,
        nameMatcher);
    this.typeCoercion = TypeCoercions.getTypeCoercion(this, config.sqlConformance());
  }

  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param conformance   Compatibility mode
   */
  @Deprecated // to be removed before 1.24
  protected SqlValidatorImpl(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance) {
    this(opTab, catalogReader, typeFactory, Config.DEFAULT.withSqlConformance(conformance));
  }

  //~ Methods ----------------------------------------------------------------

  public SqlConformance getConformance() {
    return config.sqlConformance();
  }

  public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

  public SqlOperatorTable getOperatorTable() {
    return opTab;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public RelDataType getUnknownType() {
    return unknownType;
  }

  public SqlNodeList expandStar(
      SqlNodeList selectList,
      SqlSelect select,
      boolean includeSystemVars) {
    final List<SqlNode> list = new ArrayList<>();
    final List<Map.Entry<String, RelDataType>> types = new ArrayList<>();
    for (int i = 0; i < selectList.size(); i++) {
      final SqlNode selectItem = selectList.get(i);
      final RelDataType originalType = getValidatedNodeTypeIfKnown(selectItem);
      expandSelectItem(
          selectItem,
          select,
          Util.first(originalType, unknownType),
          list,
          catalogReader.nameMatcher().createSet(),

          types,
          includeSystemVars);
    }
    getRawSelectScope(select).setExpandedSelectList(list);
    return new SqlNodeList(list, SqlParserPos.ZERO);
  }

  // implement SqlValidator
  public void declareCursor(SqlSelect select, SqlValidatorScope parentScope) {
    cursorSet.add(select);

    // add the cursor to a map that maps the cursor to its select based on
    // the position of the cursor relative to other cursors in that call
    FunctionParamInfo funcParamInfo = functionCallStack.peek();
    Map<Integer, SqlSelect> cursorMap = funcParamInfo.cursorPosToSelectMap;
    int numCursors = cursorMap.size();
    cursorMap.put(numCursors, select);

    // create a namespace associated with the result of the select
    // that is the argument to the cursor constructor; register it
    // with a scope corresponding to the cursor
    SelectScope cursorScope = new SelectScope(parentScope, null, select);
    cursorScopes.put(select, cursorScope);
    final SelectNamespace selectNs = createSelectNamespace(select, select);
    String alias = deriveAlias(select, nextGeneratedId++);
    registerNamespace(cursorScope, alias, selectNs, false);
  }

  // implement SqlValidator
  public void pushFunctionCall() {
    FunctionParamInfo funcInfo = new FunctionParamInfo();
    functionCallStack.push(funcInfo);
  }

  // implement SqlValidator
  public void popFunctionCall() {
    functionCallStack.pop();
  }

  // implement SqlValidator
  public String getParentCursor(String columnListParamName) {
    FunctionParamInfo funcParamInfo = functionCallStack.peek();
    Map<String, String> parentCursorMap =
        funcParamInfo.columnListParamToParentCursorMap;
    return parentCursorMap.get(columnListParamName);
  }

  /**
   * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
   * true; otherwise writes the unexpanded item.
   *
   * @param selectItem        Select-list item
   * @param select            Containing select clause
   * @param selectItems       List that expanded items are written to
   * @param aliases           Set of aliases
   * @param fields            List of field names and types, in alias order
   * @param includeSystemVars If true include system vars in lists
   * @return Whether the node was expanded
   */
  private boolean expandSelectItem(
      final SqlNode selectItem,
      SqlSelect select,
      RelDataType targetType,
      List<SqlNode> selectItems,
      Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fields,
      final boolean includeSystemVars) {
    final SelectScope scope = (SelectScope) getWhereScope(select);
    if (expandStar(selectItems, aliases, fields, includeSystemVars, scope,
        selectItem)) {
      return true;
    }

    // Expand the select item: fully-qualify columns, and convert
    // parentheses-free functions such as LOCALTIME into explicit function
    // calls.
    SqlNode expanded = expandWithAlias(selectItem, scope, select);
    final String alias =
        deriveAlias(
            selectItem,
            aliases.size());

    // If expansion has altered the natural alias, supply an explicit 'AS'.
    final SqlValidatorScope selectScope = getSelectScope(select);
    if (expanded != selectItem) {
      String newAlias =
          deriveAlias(
              expanded,
              aliases.size());
      if (!newAlias.equals(alias)) {
        expanded =
            SqlStdOperatorTable.AS.createCall(
                selectItem.getParserPosition(),
                expanded,
                new SqlIdentifier(alias, SqlParserPos.ZERO));
        deriveTypeImpl(selectScope, expanded);
      }
    }

    selectItems.add(expanded);
    aliases.add(alias);

    if (expanded != null) {
      inferUnknownTypes(targetType, scope, expanded);
    }
    RelDataType type = deriveType(selectScope, expanded);
    // Re-derive SELECT ITEM's data type that may be nullable in AggregatingSelectScope when it
    // appears in advanced grouping elements such as CUBE, ROLLUP , GROUPING SETS.
    // For example, SELECT CASE WHEN c = 1 THEN '1' ELSE '23' END AS x FROM t GROUP BY CUBE(x),
    // the 'x' should be nullable even if x's literal values are not null.
    if (selectScope instanceof AggregatingSelectScope) {
      type = Objects.requireNonNull(selectScope.nullifyType(stripAs(expanded), type));
    }
    setValidatedNodeType(expanded, type);
    fields.add(Pair.of(alias, type));
    return false;
  }

  private static SqlNode expandExprFromJoin(SqlJoin join, SqlIdentifier identifier,
      SelectScope scope) {
    if (join.getConditionType() != JoinConditionType.USING) {
      return identifier;
    }

    for (SqlNode node : (SqlNodeList) join.getCondition()) {
      final String name = ((SqlIdentifier) node).getSimple();
      if (identifier.getSimple().equals(name)) {
        final List<SqlNode> qualifiedNode = new ArrayList<>();
        for (ScopeChild child : scope.children) {
          if (child.namespace.getRowType()
              .getFieldNames().indexOf(name) >= 0) {
            final SqlIdentifier exp =
                new SqlIdentifier(
                    ImmutableList.of(child.name, name),
                    identifier.getParserPosition());
            qualifiedNode.add(exp);
          }
        }

        assert qualifiedNode.size() == 2;
        final SqlNode finalNode =
            SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.COALESCE.createCall(SqlParserPos.ZERO,
                    qualifiedNode.get(0),
                    qualifiedNode.get(1)),
                new SqlIdentifier(name, SqlParserPos.ZERO));
        return finalNode;
      }
    }

    // Only need to try to expand the expr from the left input of join
    // since it is always left-deep join.
    final SqlNode node = join.getLeft();
    if (node instanceof SqlJoin) {
      return expandExprFromJoin((SqlJoin) node, identifier, scope);
    } else {
      return identifier;
    }
  }

  /** Returns the set of field names in the join condition specified by USING
   * or implicitly by NATURAL, de-duplicated and in order. */
  public List<String> usingNames(SqlJoin join) {
    switch (join.getConditionType()) {
    case USING:
      final ImmutableList.Builder<String> list = ImmutableList.builder();
      final Set<String> names = catalogReader.nameMatcher().createSet();
      for (SqlNode node : (SqlNodeList) join.getCondition()) {
        final String name = ((SqlIdentifier) node).getSimple();
        if (names.add(name)) {
          list.add(name);
        }
      }
      return list.build();
    case NONE:
      if (join.isNatural()) {
        final RelDataType t0 = getValidatedNodeType(join.getLeft());
        final RelDataType t1 = getValidatedNodeType(join.getRight());
        return SqlValidatorUtil.deriveNaturalJoinColumnList(
            catalogReader.nameMatcher(), t0, t1);
      }
    }
    return null;
  }

  private static SqlNode expandCommonColumn(SqlSelect sqlSelect,
      SqlNode selectItem, SelectScope scope, SqlValidatorImpl validator) {
    if (!(selectItem instanceof SqlIdentifier)) {
      return selectItem;
    }

    final SqlNode from = sqlSelect.getFrom();
    if (from == null || !(from instanceof SqlJoin)) {
      return selectItem;
    }

    final SqlIdentifier identifier = (SqlIdentifier) selectItem;
    final SqlConformance conformance = validator.getConformance();
    if (!identifier.isSimple()) {
      if (!conformance.allowQualifyingCommonColumn()) {
        validateQualifiedCommonColumn((SqlJoin) from, identifier, scope, validator);
      }
      return selectItem;
    }

    return expandExprFromJoin((SqlJoin) from, identifier, scope);
  }

  private static void validateQualifiedCommonColumn(SqlJoin join,
      SqlIdentifier identifier, SelectScope scope, SqlValidatorImpl validator) {
    List<String> names = validator.usingNames(join);
    if (names == null) {
      // Not USING or NATURAL.
      return;
    }

    // First we should make sure that the first component is the table name.
    // Then check whether the qualified identifier contains common column.
    for (ScopeChild child : scope.children) {
      if (child.name.equals(identifier.getComponent(0).toString())) {
        if (names.indexOf(identifier.getComponent(1).toString()) >= 0) {
          throw validator.newValidationError(identifier,
              RESOURCE.disallowsQualifyingCommonColumn(identifier.toString()));
        }
      }
    }

    // Only need to try to validate the expr from the left input of join
    // since it is always left-deep join.
    final SqlNode node = join.getLeft();
    if (node instanceof SqlJoin) {
      validateQualifiedCommonColumn((SqlJoin) node, identifier, scope, validator);
    }
  }

  private boolean expandStar(List<SqlNode> selectItems, Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fields, boolean includeSystemVars,
      SelectScope scope, SqlNode node) {
    if (!(node instanceof SqlIdentifier)) {
      return false;
    }
    final SqlIdentifier identifier = (SqlIdentifier) node;
    if (!identifier.isStar()) {
      return false;
    }
    final SqlParserPos startPosition = identifier.getParserPosition();
    switch (identifier.names.size()) {
    case 1:
      boolean hasDynamicStruct = false;
      for (ScopeChild child : scope.children) {
        final int before = fields.size();
        if (child.namespace.getRowType().isDynamicStruct()) {
          hasDynamicStruct = true;
          // don't expand star if the underneath table is dynamic.
          // Treat this star as a special field in validation/conversion and
          // wait until execution time to expand this star.
          final SqlNode exp =
              new SqlIdentifier(
                  ImmutableList.of(child.name,
                      DynamicRecordType.DYNAMIC_STAR_PREFIX),
                  startPosition);
          addToSelectList(
               selectItems,
               aliases,
               fields,
               exp,
               scope,
               includeSystemVars);
        } else {
          final SqlNode from = child.namespace.getNode();
          final SqlValidatorNamespace fromNs = getNamespace(from, scope);
          assert fromNs != null;
          final RelDataType rowType = fromNs.getRowType();
          for (RelDataTypeField field : rowType.getFieldList()) {
            String columnName = field.getName();

            // TODO: do real implicit collation here
            final SqlIdentifier exp =
                new SqlIdentifier(
                    ImmutableList.of(child.name, columnName),
                    startPosition);
            // Don't add expanded rolled up columns
            if (!isRolledUpColumn(exp, scope)) {
              addOrExpandField(
                      selectItems,
                      aliases,
                      types,
                      includeSystemVars,
                      scope,
                      exp,
                      field);
            }
          }
        }
        if (child.nullable) {
          for (int i = before; i < fields.size(); i++) {
            final Map.Entry<String, RelDataType> entry = fields.get(i);
            final RelDataType type = entry.getValue();
            if (!type.isNullable()) {
              fields.set(i,
                  Pair.of(entry.getKey(),
                      typeFactory.createTypeWithNullability(type, true)));
            }
          }
        }
      }
      // If NATURAL JOIN or USING is present, move key fields to the front of
      // the list, per standard SQL. Disabled if there are dynamic fields.
      if (!hasDynamicStruct || Bug.CALCITE_2400_FIXED) {
        SqlNode from =
            requireNonNull(scope.getNode().getFrom(),
                () -> "getFrom for " + scope.getNode());
        // If some fields before star identifier,
        // we should move offset.
        int offset = calculatePermuteOffset(selectItems);
        new Permute(from, offset).permute(selectItems, fields);
      }
      return true;

    default:
      final SqlIdentifier prefixId = identifier.skipLast(1);
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      final SqlNameMatcher nameMatcher =
          scope.validator.catalogReader.nameMatcher();
      scope.resolve(prefixId.names, nameMatcher, true, resolved);
      if (resolved.count() == 0) {
        // e.g. "select s.t.* from e"
        // or "select r.* from e"
        throw newValidationError(prefixId,
            RESOURCE.unknownIdentifier(prefixId.toString()));
      }
      final RelDataType rowType = resolved.only().rowType();
      if (rowType.isDynamicStruct()) {
        // don't expand star if the underneath table is dynamic.
        addToSelectList(
            selectItems,
            aliases,
            fields,
            prefixId.plus(DynamicRecordType.DYNAMIC_STAR_PREFIX, startPosition),
            scope,
            includeSystemVars);
      } else if (rowType.isStruct()) {
        for (RelDataTypeField field : rowType.getFieldList()) {
          String columnName = field.getName();

          // TODO: do real implicit collation here
          addOrExpandField(
              selectItems,
              aliases,
              fields,
              includeSystemVars,
              scope,
              prefixId.plus(columnName, startPosition),
              field);
        }
      } else {
        throw newValidationError(prefixId, RESOURCE.starRequiresRecordType());
      }
      return true;
    }
  }

  private int calculatePermuteOffset(List<SqlNode> selectItems) {
    for (int i = 0; i < selectItems.size(); i++) {
      SqlNode selectItem = selectItems.get(i);
      SqlNode col = SqlUtil.stripAs(selectItem);
      if (col.getKind() == SqlKind.IDENTIFIER
          && selectItem.getKind() != SqlKind.AS) {
        return i;
      }
    }
    return 0;
  }

  private SqlNode maybeCast(SqlNode node, RelDataType currentType,
      RelDataType desiredType) {
    return SqlTypeUtil.equalSansNullability(typeFactory, currentType, desiredType)
        ? node
        : SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO,
            node, SqlTypeUtil.convertTypeToSpec(desiredType));
  }

  private boolean addOrExpandField(List<SqlNode> selectItems, Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fields, boolean includeSystemVars,
      SelectScope scope, SqlIdentifier id, RelDataTypeField field) {
    switch (field.getType().getStructKind()) {
    case PEEK_FIELDS:
    case PEEK_FIELDS_DEFAULT:
      final SqlNode starExp = id.plusStar();
      expandStar(
          selectItems,
          aliases,
          fields,
          includeSystemVars,
          scope,
          starExp);
      return true;

    default:
      addToSelectList(
          selectItems,
          aliases,
          fields,
          id,
          scope,
          includeSystemVars);
    }

    return false;
  }

  public SqlNode validate(SqlNode topNode) {
    SqlValidatorScope scope = new EmptyScope(this);
    scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
    final SqlNode topNode2 = validateScopedExpression(topNode, scope);
    final RelDataType type = getValidatedNodeType(topNode2);
    Util.discard(type);
    return topNode2;
  }

  public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos) {
    SqlValidatorScope scope = new EmptyScope(this);
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
    cursorSet.add(outermostNode);
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(
          scope,
          null,
          scope,
          outermostNode,
          outermostNode,
          null,
          false);
    }
    final SqlValidatorNamespace ns = getNamespace(outermostNode);
    if (ns == null) {
      throw new AssertionError("Not a query: " + outermostNode);
    }
    Collection<SqlMoniker> hintList = Sets.newTreeSet(SqlMoniker.COMPARATOR);
    lookupSelectHints(ns, pos, hintList);
    return ImmutableList.copyOf(hintList);
  }

  public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos) {
    final String posString = pos.toString();
    IdInfo info = idPositions.get(posString);
    if (info != null) {
      final SqlQualified qualified = info.scope.fullyQualify(info.id);
      return new SqlIdentifierMoniker(qualified.identifier);
    } else {
      return null;
    }
  }

  /**
   * Looks up completion hints for a syntactically correct select SQL that has
   * been parsed into an expression tree.
   *
   * @param select   the Select node of the parsed expression tree
   * @param pos      indicates the position in the sql statement we want to get
   *                 completion hints for
   * @param hintList list of {@link SqlMoniker} (sql identifiers) that can
   *                 fill in at the indicated position
   */
  void lookupSelectHints(
      SqlSelect select,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    IdInfo info = idPositions.get(pos.toString());
    if ((info == null) || (info.scope == null)) {
      SqlNode fromNode = select.getFrom();
      final SqlValidatorScope fromScope = getFromScope(select);
      lookupFromHints(fromNode, fromScope, pos, hintList);
    } else {
      lookupNameCompletionHints(info.scope, info.id.names,
          info.id.getParserPosition(), hintList);
    }
  }

  private void lookupSelectHints(
      SqlValidatorNamespace ns,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    final SqlNode node = ns.getNode();
    if (node instanceof SqlSelect) {
      lookupSelectHints((SqlSelect) node, pos, hintList);
    }
  }

  private void lookupFromHints(
      SqlNode node,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    if (node == null) {
      // This can happen in cases like "select * _suggest_", so from clause is absent
      return;
    }
    final SqlValidatorNamespace ns = getNamespace(node);
    if (ns.isWrapperFor(IdentifierNamespace.class)) {
      IdentifierNamespace idNs = ns.unwrap(IdentifierNamespace.class);
      final SqlIdentifier id = idNs.getId();
      for (int i = 0; i < id.names.size(); i++) {
        if (pos.toString().equals(
            id.getComponent(i).getParserPosition().toString())) {
          final List<SqlMoniker> objNames = new ArrayList<>();
          SqlValidatorUtil.getSchemaObjectMonikers(
              getCatalogReader(),
              id.names.subList(0, i + 1),
              objNames);
          for (SqlMoniker objName : objNames) {
            if (objName.getType() != SqlMonikerType.FUNCTION) {
              hintList.add(objName);
            }
          }
          return;
        }
      }
    }
    switch (node.getKind()) {
    case JOIN:
      lookupJoinHints((SqlJoin) node, scope, pos, hintList);
      break;
    default:
      lookupSelectHints(ns, pos, hintList);
      break;
    }
  }

  private void lookupJoinHints(
      SqlJoin join,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    lookupFromHints(left, scope, pos, hintList);
    if (hintList.size() > 0) {
      return;
    }
    lookupFromHints(right, scope, pos, hintList);
    if (hintList.size() > 0) {
      return;
    }
    final JoinConditionType conditionType = join.getConditionType();
    final SqlValidatorScope joinScope = scopes.get(join);
    switch (conditionType) {
    case ON:
      condition.findValidOptions(this, joinScope, pos, hintList);
      return;
    default:

      // No suggestions.
      // Not supporting hints for other types such as 'Using' yet.
      return;
    }
  }

  /**
   * Populates a list of all the valid alternatives for an identifier.
   *
   * @param scope    Validation scope
   * @param names    Components of the identifier
   * @param pos      position
   * @param hintList a list of valid options
   */
  public final void lookupNameCompletionHints(
      SqlValidatorScope scope,
      List<String> names,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    // Remove the last part of name - it is a dummy
    List<String> subNames = Util.skipLast(names);

    if (subNames.size() > 0) {
      // If there's a prefix, resolve it to a namespace.
      SqlValidatorNamespace ns = null;
      for (String name : subNames) {
        if (ns == null) {
          final SqlValidatorScope.ResolvedImpl resolved =
              new SqlValidatorScope.ResolvedImpl();
          final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
          scope.resolve(ImmutableList.of(name), nameMatcher, false, resolved);
          if (resolved.count() == 1) {
            ns = resolved.only().namespace;
          }
        } else {
          ns = ns.lookupChild(name);
        }
        if (ns == null) {
          break;
        }
      }
      if (ns != null) {
        RelDataType rowType = ns.getRowType();
        if (rowType.isStruct()) {
          for (RelDataTypeField field : rowType.getFieldList()) {
            hintList.add(
                new SqlMonikerImpl(
                    field.getName(),
                    SqlMonikerType.COLUMN));
          }
        }
      }

      // builtin function names are valid completion hints when the
      // identifier has only 1 name part
      findAllValidFunctionNames(names, this, hintList, pos);
    } else {
      // No prefix; use the children of the current scope (that is,
      // the aliases in the FROM clause)
      scope.findAliases(hintList);

      // If there's only one alias, add all child columns
      SelectScope selectScope =
          SqlValidatorUtil.getEnclosingSelectScope(scope);
      if ((selectScope != null)
          && (selectScope.getChildren().size() == 1)) {
        RelDataType rowType =
            selectScope.getChildren().get(0).getRowType();
        for (RelDataTypeField field : rowType.getFieldList()) {
          hintList.add(
              new SqlMonikerImpl(
                  field.getName(),
                  SqlMonikerType.COLUMN));
        }
      }
    }

    findAllValidUdfNames(names, this, hintList);
  }

  private static void findAllValidUdfNames(
      List<String> names,
      SqlValidator validator,
      Collection<SqlMoniker> result) {
    final List<SqlMoniker> objNames = new ArrayList<>();
    SqlValidatorUtil.getSchemaObjectMonikers(
        validator.getCatalogReader(),
        names,
        objNames);
    for (SqlMoniker objName : objNames) {
      if (objName.getType() == SqlMonikerType.FUNCTION) {
        result.add(objName);
      }
    }
  }

  private static void findAllValidFunctionNames(
      List<String> names,
      SqlValidator validator,
      Collection<SqlMoniker> result,
      SqlParserPos pos) {
    // a function name can only be 1 part
    if (names.size() > 1) {
      return;
    }
    for (SqlOperator op : validator.getOperatorTable().getOperatorList()) {
      SqlIdentifier curOpId =
          new SqlIdentifier(
              op.getName(),
              pos);

      final SqlCall call = validator.makeNullaryCall(curOpId);
      if (call != null) {
        result.add(
            new SqlMonikerImpl(
                op.getName(),
                SqlMonikerType.FUNCTION));
      } else {
        if ((op.getSyntax() == SqlSyntax.FUNCTION)
            || (op.getSyntax() == SqlSyntax.PREFIX)) {
          if (op.getOperandTypeChecker() != null) {
            String sig = op.getAllowedSignatures();
            sig = sig.replace("'", "");
            result.add(
                new SqlMonikerImpl(
                    sig,
                    SqlMonikerType.FUNCTION));
            continue;
          }
          result.add(
              new SqlMonikerImpl(
                  op.getName(),
                  SqlMonikerType.FUNCTION));
        }
      }
    }
  }

  public SqlNode validateParameterizedExpression(
      SqlNode topNode,
      final Map<String, RelDataType> nameToTypeMap) {
    SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
    return validateScopedExpression(topNode, scope);
  }

  protected SqlNode validateScopedExpression(
      SqlNode topNode,
      SqlValidatorScope scope) {
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
    cursorSet.add(outermostNode);
    top = outermostNode;
    TRACER.trace("After unconditional rewrite: {}", outermostNode);
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(scope, null, scope, outermostNode, outermostNode, null, false);
    }
    outermostNode.validate(this, scope);
    if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
      // force type derivation so that we can provide it to the
      // caller later without needing the scope
      deriveType(scope, outermostNode);
    }
    TRACER.trace("After validation: {}", outermostNode);
    return outermostNode;
  }

  public void validateQuery(SqlNode node, SqlValidatorScope scope,
      RelDataType targetRowType) {
    final SqlValidatorNamespace ns = getNamespace(node, scope);
    if (node.getKind() == SqlKind.TABLESAMPLE) {
      List<SqlNode> operands = ((SqlCall) node).getOperandList();
      SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands.get(1));
      if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
        validateFeature(RESOURCE.sQLFeature_T613(), node.getParserPosition());
      } else if (sampleSpec
          instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
        validateFeature(RESOURCE.sQLFeatureExt_T613_Substitution(),
            node.getParserPosition());
      }
    }

    validateNamespace(ns, targetRowType);
    switch (node.getKind()) {
    case EXTEND:
      // Until we have a dedicated namespace for EXTEND
      deriveType(scope, node);
    }
    if (node == top) {
      validateModality(node);
    }
    validateAccess(
        node,
        ns.getTable(),
        SqlAccessEnum.SELECT);

    validateSnapshot(node, scope, ns);
  }

  /**
   * Validates a namespace.
   *
   * @param namespace Namespace
   * @param targetRowType Desired row type, must not be null, may be the data
   *                      type 'unknown'.
   */
  protected void validateNamespace(final SqlValidatorNamespace namespace,
      RelDataType targetRowType) {
    namespace.validate(targetRowType);
    if (namespace.getNode() != null) {
      setValidatedNodeType(namespace.getNode(), namespace.getType());
    }
  }

  @VisibleForTesting
  public SqlValidatorScope getEmptyScope() {
    return new EmptyScope(this);
  }

  public SqlValidatorScope getCursorScope(SqlSelect select) {
    return cursorScopes.get(select);
  }

  public SqlValidatorScope getWhereScope(SqlSelect select) {
    return whereScopes.get(select);
  }

  public SqlValidatorScope getSelectScope(SqlSelect select) {
    return selectScopes.get(select);
  }

  /**
   * Namespace for the given node.
   *
   * @param node node to compute the namespace for
   * @return namespace for the given node, never null
   * @see #getNamespace(SqlNode)
   */
  SqlValidatorNamespace getNamespaceOrThrow(SqlNode node) {
    return requireNonNull(
        getNamespace(node),
        () -> "namespace for " + node);
  }

  public SelectScope getRawSelectScope(SqlSelect select) {
    SqlValidatorScope scope = getSelectScope(select);
    if (scope instanceof AggregatingSelectScope) {
      scope = ((AggregatingSelectScope) scope).getParent();
    }
    return (SelectScope) scope;
  }

  public SqlValidatorScope getHavingScope(SqlSelect select) {
    // Yes, it's the same as getSelectScope
    return selectScopes.get(select);
  }

  public SqlValidatorScope getGroupScope(SqlSelect select) {
    // Yes, it's the same as getWhereScope
    return groupByScopes.get(select);
  }

  public SqlValidatorScope getFromScope(SqlSelect select) {
    return scopes.get(select);
  }

  public SqlValidatorScope getOrderScope(SqlSelect select) {
    return orderScopes.get(select);
  }

  public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
    return scopes.get(node);
  }

  public SqlValidatorScope getJoinScope(SqlNode node) {
    return scopes.get(stripAs(node));
  }

  @Override public SqlValidatorScope getOverScope(SqlNode node) {
    return getScopeOrThrow(node);
  }

  @Override public SqlValidatorScope getLateralScope(SqlJoin node) {
    return lateralScopes.get(node);
  }

  private SqlValidatorScope getScopeOrThrow(SqlNode node) {
    return scopes.get(node);
  }

  private SqlValidatorNamespace getNamespace(SqlNode node,
      SqlValidatorScope scope) {
    if (node instanceof SqlIdentifier && scope instanceof DelegatingScope) {
      final SqlIdentifier id = (SqlIdentifier) node;
      final DelegatingScope idScope = (DelegatingScope) ((DelegatingScope) scope).getParent();
      return getNamespace(id, idScope);
    } else if (node instanceof SqlCall) {
      // Handle extended identifiers.
      final SqlCall call = (SqlCall) node;
      switch (call.getOperator().getKind()) {
      case TABLE_REF:
        return getNamespace(call.operand(0), scope);
      case EXTEND:
        final SqlNode operand0 = call.getOperandList().get(0);
        final SqlIdentifier identifier = operand0.getKind() == SqlKind.TABLE_REF
            ? ((SqlCall) operand0).operand(0)
            : (SqlIdentifier) operand0;
        final DelegatingScope idScope = (DelegatingScope) scope;
        return getNamespace(identifier, idScope);
      case AS:
        final SqlNode nested = call.getOperandList().get(0);
        switch (nested.getKind()) {
        case TABLE_REF:
        case EXTEND:
          return getNamespace(nested, scope);
        }
        break;
      }
    }
    return getNamespace(node);
  }

  private SqlValidatorNamespace getNamespace(SqlIdentifier id, DelegatingScope scope) {
    if (id.isSimple()) {
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      scope.resolve(id.names, nameMatcher, false, resolved);
      if (resolved.count() == 1) {
        return resolved.only().namespace;
      }
    }
    return getNamespace(id);
  }

  public SqlValidatorNamespace getNamespace(SqlNode node) {
    switch (node.getKind()) {
    case AS:

      // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
      final SqlValidatorNamespace ns = namespaces.get(node);
      if (ns != null) {
        return ns;
      }
      // fall through
>>>>>>> aed92dce0 (DX-58483: [CALCITE-482] Implement sql and planner hints)
    case TABLE_REF:
    case OVER:
    case COLLECTION_TABLE:
    case ORDER_BY:
    case TABLESAMPLE:
      return getNamespace(((SqlCall) node).operand(0));
    default:
      return namespaces.get(node);
    }
  }

  private void handleOffsetFetch(SqlNode offset, SqlNode fetch) {
    if (offset instanceof SqlDynamicParam) {
      setValidatedNodeType(offset,
          typeFactory.createSqlType(SqlTypeName.INTEGER));
    }
    if (fetch instanceof SqlDynamicParam) {
      setValidatedNodeType(fetch,
          typeFactory.createSqlType(SqlTypeName.INTEGER));
    }
  }

  /**
   * Performs expression rewrites which are always used unconditionally. These
   * rewrites massage the expression tree into a standard form so that the
   * rest of the validation logic can be simpler.
   *
   * @param node      expression to be rewritten
   * @param underFrom whether node appears directly under a FROM clause
   * @return rewritten expression
   */
  protected SqlNode performUnconditionalRewrites(
      SqlNode node,
      boolean underFrom) {
    if (node == null) {
      return node;
    }

    SqlNode newOperand;

    // first transform operands and invoke generic call rewrite
    if (node instanceof SqlCall) {
      if (node instanceof SqlMerge) {
        validatingSqlMerge = true;
      }
      SqlCall call = (SqlCall) node;
      final SqlKind kind = call.getKind();
      final List<SqlNode> operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        SqlNode operand = operands.get(i);
        boolean childUnderFrom;
        if (kind == SqlKind.SELECT) {
          childUnderFrom = i == SqlSelect.FROM_OPERAND;
        } else if (kind == SqlKind.AS && (i == 0)) {
          // for an aliased expression, it is under FROM if
          // the AS expression is under FROM
          childUnderFrom = underFrom;
        } else {
          childUnderFrom = false;
        }
        newOperand =
            performUnconditionalRewrites(operand, childUnderFrom);
        if (newOperand != null && newOperand != operand) {
          call.setOperand(i, newOperand);
        }
      }

      if (call.getOperator() instanceof SqlUnresolvedFunction) {
        assert call instanceof SqlBasicCall;
        final SqlUnresolvedFunction function =
            (SqlUnresolvedFunction) call.getOperator();
        // This function hasn't been resolved yet.  Perform
        // a half-hearted resolution now in case it's a
        // builtin function requiring special casing.  If it's
        // not, we'll handle it later during overload resolution.
        final List<SqlOperator> overloads = new ArrayList<>();
        opTab.lookupOperatorOverloads(function.getNameAsId(),
            function.getFunctionType(), SqlSyntax.FUNCTION, overloads,
            catalogReader.nameMatcher());
        if (overloads.size() == 1) {
          ((SqlBasicCall) call).setOperator(overloads.get(0));
        }
      }
      if (config.callRewrite()) {
        node = call.getOperator().rewriteCall(this, call);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (int i = 0, count = list.size(); i < count; i++) {
        SqlNode operand = list.get(i);
        newOperand =
            performUnconditionalRewrites(
                operand,
                false);
        if (newOperand != null) {
          list.getList().set(i, newOperand);
        }
      }
    }

    // now transform node itself
    final SqlKind kind = node.getKind();
    switch (kind) {
    case VALUES:
      // CHECKSTYLE: IGNORE 1
      if (underFrom || true) {
        // leave FROM (VALUES(...)) [ AS alias ] clauses alone,
        // otherwise they grow cancerously if this rewrite is invoked
        // over and over
        return node;
      } else {
        final SqlNodeList selectList =
            new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
        return new SqlSelect(node.getParserPosition(), null, selectList, node,
            null, null, null, null, null, null, null, null, null);
      }

    case ORDER_BY: {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      handleOffsetFetch(orderBy.offset, orderBy.fetch);
      if (orderBy.query instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) orderBy.query;

        // Don't clobber existing ORDER BY.  It may be needed for
        // an order-sensitive function like RANK.
        if (select.getOrderList() == null) {
          // push ORDER BY into existing select
          select.setOrderBy(orderBy.orderList);
          select.setOffset(orderBy.offset);
          select.setFetch(orderBy.fetch);
          return select;
        }
      }
      if (orderBy.query instanceof SqlWith
          && ((SqlWith) orderBy.query).body instanceof SqlSelect) {
        SqlWith with = (SqlWith) orderBy.query;
        SqlSelect select = (SqlSelect) with.body;

        // Don't clobber existing ORDER BY.  It may be needed for
        // an order-sensitive function like RANK.
        if (select.getOrderList() == null) {
          // push ORDER BY into existing select
          select.setOrderBy(orderBy.orderList);
          select.setOffset(orderBy.offset);
          select.setFetch(orderBy.fetch);
          return with;
        }
      }
      final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
      final SqlNodeList orderList;
      if (getInnerSelect(node) != null && isAggregate(getInnerSelect(node))) {
        orderList = SqlNode.clone(orderBy.orderList);
        // We assume that ORDER BY item does not have ASC etc.
        // We assume that ORDER BY item is present in SELECT list.
        for (int i = 0; i < orderList.size(); i++) {
          SqlNode sqlNode = orderList.get(i);
          SqlNodeList selectList2 = getInnerSelect(node).getSelectList();
          for (Ord<SqlNode> sel : Ord.zip(selectList2)) {
            if (stripAs(sel.e).equalsDeep(sqlNode, Litmus.IGNORE)) {
              orderList.set(i,
                  SqlLiteral.createExactNumeric(Integer.toString(sel.i + 1),
                      SqlParserPos.ZERO));
            }
          }
        }
      } else {
        orderList = orderBy.orderList;
      }
      return new SqlSelect(SqlParserPos.ZERO, null, selectList, orderBy.query,
          null, null, null, null, null, orderList, orderBy.offset,
          orderBy.fetch, null);
    }

    case EXPLICIT_TABLE: {
      // (TABLE t) is equivalent to (SELECT * FROM t)
      SqlCall call = (SqlCall) node;
      final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
      return new SqlSelect(SqlParserPos.ZERO, null, selectList, call.operand(0),
          null, null, null, null, null, null, null, null, null);
    }

    case DELETE: {
      SqlDelete call = (SqlDelete) node;
      SqlSelect select = createSourceSelectForDelete(call);
      call.setSourceSelect(select);
      break;
    }

    case UPDATE: {
      SqlUpdate call = (SqlUpdate) node;
      SqlSelect select = createSourceSelectForUpdate(call);
      call.setSourceSelect(select);

      // See if we're supposed to rewrite UPDATE to MERGE
      // (unless this is the UPDATE clause of a MERGE,
      // in which case leave it alone).
      if (!validatingSqlMerge) {
        SqlNode selfJoinSrcExpr =
            getSelfJoinExprForUpdate(
                call.getTargetTable(),
                UPDATE_SRC_ALIAS);
        if (selfJoinSrcExpr != null) {
          node = rewriteUpdateToMerge(call, selfJoinSrcExpr);
        }
      }
      break;
    }

    case MERGE: {
      SqlMerge call = (SqlMerge) node;
      rewriteMerge(call);
      break;
    }
    }
    return node;
  }

  private SqlSelect getInnerSelect(SqlNode node) {
    for (;;) {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      } else if (node instanceof SqlOrderBy) {
        node = ((SqlOrderBy) node).query;
      } else if (node instanceof SqlWith) {
        node = ((SqlWith) node).body;
      } else {
        return null;
      }
    }
  }

  private void rewriteMerge(SqlMerge call) {
    SqlNodeList selectList;
    SqlUpdate updateStmt = call.getUpdateCall();
    if (updateStmt != null) {
      // if we have an update statement, just clone the select list
      // from the update statement's source since it's the same as
      // what we want for the select list of the merge source -- '*'
      // followed by the update set expressions
      selectList = SqlNode.clone(updateStmt.getSourceSelect().getSelectList());
    } else {
      // otherwise, just use select *
      selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    }
    SqlNode targetTable = call.getTargetTable();
    if (call.getAlias() != null) {
      targetTable =
          SqlValidatorUtil.addAlias(
              targetTable,
              call.getAlias().getSimple());
    }

    // Provided there is an insert substatement, the source select for
    // the merge is a left outer join between the source in the USING
    // clause and the target table; otherwise, the join is just an
    // inner join.  Need to clone the source table reference in order
    // for validation to work
    SqlNode sourceTableRef = call.getSourceTableRef();
    SqlInsert insertCall = call.getInsertCall();
    JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;
    final SqlNode leftJoinTerm = SqlNode.clone(sourceTableRef);
    SqlNode outerJoin =
        new SqlJoin(SqlParserPos.ZERO,
            leftJoinTerm,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            joinType.symbol(SqlParserPos.ZERO),
            targetTable,
            JoinConditionType.ON.symbol(SqlParserPos.ZERO),
            call.getCondition());
    SqlSelect select =
        new SqlSelect(SqlParserPos.ZERO, null, selectList, outerJoin, null,
            null, null, null, null, null, null, null, null);
    call.setSourceSelect(select);

    // Source for the insert call is a select of the source table
    // reference with the select list being the value expressions;
    // note that the values clause has already been converted to a
    // select on the values row constructor; so we need to extract
    // that via the from clause on the select
    if (insertCall != null) {
      SqlCall valuesCall = (SqlCall) insertCall.getSource();
      SqlCall rowCall = valuesCall.operand(0);
      selectList =
          new SqlNodeList(
              rowCall.getOperandList(),
              SqlParserPos.ZERO);
      final SqlNode insertSource = SqlNode.clone(sourceTableRef);
      select =
          new SqlSelect(SqlParserPos.ZERO, null, selectList, insertSource, null,
              null, null, null, null, null, null, null, null);
      insertCall.setSource(select);
    }
  }

  private SqlNode rewriteUpdateToMerge(
      SqlUpdate updateCall,
      SqlNode selfJoinSrcExpr) {
    // Make sure target has an alias.
    if (updateCall.getAlias() == null) {
      updateCall.setAlias(
          new SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO));
    }
    SqlNode selfJoinTgtExpr =
        getSelfJoinExprForUpdate(
            updateCall.getTargetTable(),
            updateCall.getAlias().getSimple());
    assert selfJoinTgtExpr != null;

    // Create join condition between source and target exprs,
    // creating a conjunction with the user-level WHERE
    // clause if one was supplied
    SqlNode condition = updateCall.getCondition();
    SqlNode selfJoinCond =
        SqlStdOperatorTable.EQUALS.createCall(
            SqlParserPos.ZERO,
            selfJoinSrcExpr,
            selfJoinTgtExpr);
    if (condition == null) {
      condition = selfJoinCond;
    } else {
      condition =
          SqlStdOperatorTable.AND.createCall(
              SqlParserPos.ZERO,
              selfJoinCond,
              condition);
    }
    SqlNode target =
        updateCall.getTargetTable().clone(SqlParserPos.ZERO);

    // For the source, we need to anonymize the fields, so
    // that for a statement like UPDATE T SET I = I + 1,
    // there's no ambiguity for the "I" in "I + 1";
    // this is OK because the source and target have
    // identical values due to the self-join.
    // Note that we anonymize the source rather than the
    // target because downstream, the optimizer rules
    // don't want to see any projection on top of the target.
    IdentifierNamespace ns =
        new IdentifierNamespace(this, target, null, null);
    RelDataType rowType = ns.getRowType();
    SqlNode source = updateCall.getTargetTable().clone(SqlParserPos.ZERO);
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    int i = 1;
    for (RelDataTypeField field : rowType.getFieldList()) {
      SqlIdentifier col =
          new SqlIdentifier(
              field.getName(),
              SqlParserPos.ZERO);
      selectList.add(
          SqlValidatorUtil.addAlias(col, UPDATE_ANON_PREFIX + i));
      ++i;
    }
    source =
        new SqlSelect(SqlParserPos.ZERO, null, selectList, source, null, null,
            null, null, null, null, null, null, null);
    source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS);
    SqlMerge mergeCall =
        new SqlMerge(updateCall.getParserPosition(), target, condition, source,
            updateCall, null, null, updateCall.getAlias());
    rewriteMerge(mergeCall);
    return mergeCall;
  }

  /**
   * Allows a subclass to provide information about how to convert an UPDATE
   * into a MERGE via self-join. If this method returns null, then no such
   * conversion takes place. Otherwise, this method should return a suitable
   * unique identifier expression for the given table.
   *
   * @param table identifier for table being updated
   * @param alias alias to use for qualifying columns in expression, or null
   *              for unqualified references; if this is equal to
   *              {@value #UPDATE_SRC_ALIAS}, then column references have been
   *              anonymized to "SYS$ANONx", where x is the 1-based column
   *              number.
   * @return expression for unique identifier, or null to prevent conversion
   */
  protected SqlNode getSelfJoinExprForUpdate(
      SqlNode table,
      String alias) {
    return null;
  }

  /**
   * Creates the SELECT statement that putatively feeds rows into an UPDATE
   * statement to be updated.
   *
   * @param call Call to the UPDATE operator
   * @return select statement
   */
  protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    int ordinal = 0;
    for (SqlNode exp : call.getSourceExpressionList()) {
      // Force unique aliases to avoid a duplicate for Y with
      // SET X=Y
      String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
      selectList.add(SqlValidatorUtil.addAlias(exp, alias));
      ++ordinal;
    }
    SqlNode sourceTable = call.getTargetTable();
    if (call.getAlias() != null) {
      sourceTable =
          SqlValidatorUtil.addAlias(
              sourceTable,
              call.getAlias().getSimple());
    }
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
        call.getCondition(), null, null, null, null, null, null, null, null);
  }

  /**
   * Creates the SELECT statement that putatively feeds rows into a DELETE
   * statement to be deleted.
   *
   * @param call Call to the DELETE operator
   * @return select statement
   */
  protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    SqlNode sourceTable = call.getTargetTable();
    if (call.getAlias() != null) {
      sourceTable =
          SqlValidatorUtil.addAlias(
              sourceTable,
              call.getAlias().getSimple());
    }
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
        call.getCondition(), null, null, null, null, null, null, null, null);
  }

  /**
   * Returns null if there is no common type. E.g. if the rows have a
   * different number of columns.
   */
  RelDataType getTableConstructorRowType(
      SqlCall values,
      SqlValidatorScope scope) {
    final List<SqlNode> rows = values.getOperandList();
    assert rows.size() >= 1;
    final List<RelDataType> rowTypes = new ArrayList<>();
    for (final SqlNode row : rows) {
      assert row.getKind() == SqlKind.ROW;
      SqlCall rowConstructor = (SqlCall) row;

      // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
      // rows, need to infer aliases from there.
      final List<String> aliasList = new ArrayList<>();
      final List<RelDataType> typeList = new ArrayList<>();
      for (Ord<SqlNode> column : Ord.zip(rowConstructor.getOperandList())) {
        final String alias = deriveAlias(column.e, column.i);
        aliasList.add(alias);
        final RelDataType type = deriveType(scope, column.e);
        typeList.add(type);
      }
      rowTypes.add(typeFactory.createStructType(typeList, aliasList));
    }
    if (rows.size() == 1) {
      // TODO jvs 10-Oct-2005:  get rid of this workaround once
      // leastRestrictive can handle all cases
      return rowTypes.get(0);
    }
    return typeFactory.leastRestrictive(rowTypes);
  }

  public RelDataType getValidatedNodeType(SqlNode node) {
    RelDataType type = getValidatedNodeTypeIfKnown(node);
    if (type == null) {
      if (node.getKind() == SqlKind.IDENTIFIER) {
        throw newValidationError(node, RESOURCE.unknownIdentifier(node.toString()));
      }
      throw Util.needToImplement(node);
    } else {
      return type;
    }
  }

  public RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
    final RelDataType type = nodeToTypeMap.get(node);
    if (type != null) {
      return type;
    }
    final SqlValidatorNamespace ns = getNamespace(node);
    if (ns != null) {
      return ns.getType();
    }
    final SqlNode original = originalExprs.get(node);
    if (original != null && original != node) {
      return getValidatedNodeType(original);
    }
    if (node instanceof SqlIdentifier) {
      return getCatalogReader().getNamedType((SqlIdentifier) node);
    }
    if (node instanceof SqlCall) {
      SqlCall sqlCall = (SqlCall) node;
      if (sqlCall.getOperator() instanceof SqlAggFunction) {
        // Aggregate functions don't implement deriveType correctly when it comes to nullability
        // Something like SUM($0) gets rewritten to
        // CASE(>(COUNT($0), 0), CAST($SUM0($0)):INTEGER, null:INTEGER)
        return null;
      }
      try {
        return sqlCall
          .getOperator()
          .deriveType(
            this,
            this.tableScope,
            sqlCall);
      } catch (Exception e) {
        // Not all functions implement .deriveType()
        // But this method is effort anyways.
        return null;
      }
    }
    return null;
  }

  /**
   * Saves the type of a {@link SqlNode}, now that it has been validated.
   *
   * <p>Unlike the base class method, this method is not deprecated.
   * It is available from within Calcite, but is not part of the public API.
   *
   * @param node A SQL parse tree node, never null
   * @param type Its type; must not be null
   */
  @SuppressWarnings("deprecation")
  public final void setValidatedNodeType(SqlNode node, RelDataType type) {
    try {
      Preconditions.checkNotNull(type);
    } catch (NullPointerException e) {
      throw new NullPointerException("Operand types mismatch for: " + node);
    }
    Preconditions.checkNotNull(node);
    if (type.equals(unknownType)) {
      // don't set anything until we know what it is, and don't overwrite
      // a known type with the unknown type
      return;
    }
    nodeToTypeMap.put(node, type);
  }

  public void removeValidatedNodeType(SqlNode node) {
    nodeToTypeMap.remove(node);
  }

  @Nullable public SqlCall makeNullaryCall(SqlIdentifier id) {
    if (id.names.size() == 1 && !id.isComponentQuoted(0)) {
      final List<SqlOperator> list = new ArrayList<>();
      opTab.lookupOperatorOverloads(id, null, SqlSyntax.FUNCTION, list,
          catalogReader.nameMatcher());
      for (SqlOperator operator : list) {
        if (operator.getSyntax() == SqlSyntax.FUNCTION_ID) {
          // Even though this looks like an identifier, it is a
          // actually a call to a function. Construct a fake
          // call to this function, so we can use the regular
          // operator validation.
          return new SqlBasicCall(operator, SqlNode.EMPTY_ARRAY,
              id.getParserPosition(), true, null);
        }
      }
    }
    return null;
  }

  public RelDataType deriveType(
      SqlValidatorScope scope,
      SqlNode expr) {
    Objects.requireNonNull(scope);
    Objects.requireNonNull(expr);

    // if we already know the type, no need to re-derive
    RelDataType type = nodeToTypeMap.get(expr);
    if (type != null) {
      return type;
    }
    final SqlValidatorNamespace ns = getNamespace(expr);
    if (ns != null) {
      return ns.getType();
    }
    type = deriveTypeImpl(scope, expr);
    Preconditions.checkArgument(
        type != null,
        "SqlValidator.deriveTypeInternal returned null");
    setValidatedNodeType(expr, type);
    return type;
  }

  /**
   * Derives the type of a node, never null.
   */
  RelDataType deriveTypeImpl(
      SqlValidatorScope scope,
      SqlNode operand) {
    DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
    final RelDataType type = operand.accept(v);

    // After Guava 17, use Verify.verifyNotNull for Preconditions.checkNotNull
    Bug.upgrade("guava-17");
    return Preconditions.checkNotNull(scope.nullifyType(operand, type));
  }

  public RelDataType deriveConstructorType(
      SqlValidatorScope scope,
      SqlCall call,
      SqlFunction unresolvedConstructor,
      SqlFunction resolvedConstructor,
      List<RelDataType> argTypes) {
    SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
    assert sqlIdentifier != null;
    RelDataType type = catalogReader.getNamedType(sqlIdentifier);
    if (type == null) {
      // TODO jvs 12-Feb-2005:  proper type name formatting
      throw newValidationError(sqlIdentifier,
          RESOURCE.unknownDatatypeName(sqlIdentifier.toString()));
    }

    if (resolvedConstructor == null) {
      if (call.operandCount() > 0) {
        // This is not a default constructor invocation, and
        // no user-defined constructor could be found
        throw handleUnresolvedFunction(call, unresolvedConstructor, argTypes,
            null);
      }
    } else {
      SqlCall testCall =
          resolvedConstructor.createCall(
              call.getParserPosition(),
              call.getOperandList());
      RelDataType returnType =
          resolvedConstructor.validateOperands(
              this,
              scope,
              testCall);
      assert type == returnType;
    }

    if (config.identifierExpansion()) {
      if (resolvedConstructor != null) {
        ((SqlBasicCall) call).setOperator(resolvedConstructor);
      } else {
        // fake a fully-qualified call to the default constructor
        ((SqlBasicCall) call).setOperator(
            new SqlFunction(
                type.getSqlIdentifier(),
                ReturnTypes.explicit(type),
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR));
      }
    }
    return type;
  }

  public CalciteException handleUnresolvedFunction(SqlCall call,
      SqlFunction unresolvedFunction, List<RelDataType> argTypes,
      List<String> argNames) {
    // For builtins, we can give a better error message
    final List<SqlOperator> overloads = new ArrayList<>();
    opTab.lookupOperatorOverloads(unresolvedFunction.getNameAsId(), null,
        SqlSyntax.FUNCTION, overloads, catalogReader.nameMatcher());
    if (overloads.size() == 1) {
      SqlFunction fun = (SqlFunction) overloads.get(0);
      if ((fun.getSqlIdentifier() == null)
          && (fun.getSyntax() != SqlSyntax.FUNCTION_ID)) {
        final int expectedArgCount =
            fun.getOperandCountRange().getMin();
        throw newValidationError(call,
            RESOURCE.invalidArgCount(call.getOperator().getName(),
                expectedArgCount));
      }
    }

    AssignableOperandTypeChecker typeChecking =
        new AssignableOperandTypeChecker(argTypes, argNames);
    String signature =
        typeChecking.getAllowedSignatures(
            unresolvedFunction,
            unresolvedFunction.getName());
    throw newValidationError(call,
        RESOURCE.validatorUnknownFunction(signature));
  }

  protected void inferUnknownTypes(
      RelDataType inferredType,
      SqlValidatorScope scope,
      SqlNode node) {
    final SqlValidatorScope newScope = scopes.get(node);
    if (newScope != null) {
      scope = newScope;
    }
    boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
    if ((node instanceof SqlDynamicParam) || isNullLiteral) {
      if (inferredType.equals(unknownType)) {
        if (isNullLiteral) {
          if (config.typeCoercionEnabled()) {
            // derive type of null literal
            deriveType(scope, node);
            return;
          } else {
            throw newValidationError(node, RESOURCE.nullIllegal());
          }
        } else {
          throw newValidationError(node, RESOURCE.dynamicParamIllegal());
        }
      }

      // REVIEW:  should dynamic parameter types always be nullable?
      RelDataType newInferredType =
          typeFactory.createTypeWithNullability(inferredType, true);
      if (SqlTypeUtil.inCharFamily(inferredType)) {
        newInferredType =
            typeFactory.createTypeWithCharsetAndCollation(
                newInferredType,
                inferredType.getCharset(),
                inferredType.getCollation());
      }
      setValidatedNodeType(node, newInferredType);
    } else if (node instanceof SqlNodeList) {
      SqlNodeList nodeList = (SqlNodeList) node;
      if (inferredType.isStruct()) {
        if (inferredType.getFieldCount() != nodeList.size()) {
          // this can happen when we're validating an INSERT
          // where the source and target degrees are different;
          // bust out, and the error will be detected higher up
          return;
        }
      }
      int i = 0;
      for (SqlNode child : nodeList) {
        RelDataType type;
        if (inferredType.isStruct()) {
          type = inferredType.getFieldList().get(i).getType();
          ++i;
        } else {
          type = inferredType;
        }
        inferUnknownTypes(type, scope, child);
      }
    } else if (node instanceof SqlCase) {
      final SqlCase caseCall = (SqlCase) node;

      final RelDataType whenType =
          caseCall.getValueOperand() == null ? booleanType : unknownType;
      for (SqlNode sqlNode : caseCall.getWhenOperands().getList()) {
        inferUnknownTypes(whenType, scope, sqlNode);
      }
      RelDataType returnType = deriveType(scope, node);
      for (SqlNode sqlNode : caseCall.getThenOperands().getList()) {
        inferUnknownTypes(returnType, scope, sqlNode);
      }

      if (!SqlUtil.isNullLiteral(caseCall.getElseOperand(), false)) {
        inferUnknownTypes(
            returnType,
            scope,
            caseCall.getElseOperand());
      } else {
        setValidatedNodeType(caseCall.getElseOperand(), returnType);
      }
    } else if (node.getKind()  == SqlKind.AS) {
      // For AS operator, only infer the operand not the alias
      inferUnknownTypes(inferredType, scope, ((SqlCall) node).operand(0));
    } else if (node instanceof SqlCall) {
      final SqlCall call = (SqlCall) node;
      final SqlOperandTypeInference operandTypeInference =
          call.getOperator().getOperandTypeInference();
      final SqlCallBinding callBinding = new SqlCallBinding(this, scope, call);
      final List<SqlNode> operands = callBinding.operands();
      final RelDataType[] operandTypes = new RelDataType[operands.size()];
      Arrays.fill(operandTypes, unknownType);
      // TODO:  eventually should assert(operandTypeInference != null)
      // instead; for now just eat it
      if (operandTypeInference != null) {
        operandTypeInference.inferOperandTypes(
            callBinding,
            inferredType,
            operandTypes);
      }
      for (int i = 0; i < operands.size(); ++i) {
        final SqlNode operand = operands.get(i);
        if (operand != null) {
          inferUnknownTypes(operandTypes[i], scope, operand);
        }
      }
    }
  }

  /**
   * Adds an expression to a select list, ensuring that its alias does not
   * clash with any existing expressions on the list.
   */
  protected void addToSelectList(
      List<SqlNode> list,
      Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fieldList,
      SqlNode exp,
      SelectScope scope,
      final boolean includeSystemVars) {
    String alias = SqlValidatorUtil.getAlias(exp, -1);
    String uniqueAlias =
        SqlValidatorUtil.uniquify(
            alias, aliases, SqlValidatorUtil.EXPR_SUGGESTER);
    if (!alias.equals(uniqueAlias)) {
      exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
    }
    fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
    list.add(exp);
  }

  public String deriveAlias(
      SqlNode node,
      int ordinal) {
    return SqlValidatorUtil.getAlias(node, ordinal);
  }

  // implement SqlValidator
  public void setIdentifierExpansion(boolean expandIdentifiers) {
    this.config = this.config.withIdentifierExpansion(expandIdentifiers);
  }

  // implement SqlValidator
  public void setColumnReferenceExpansion(
      boolean expandColumnReferences) {
    this.config = this.config.withColumnReferenceExpansion(expandColumnReferences);
  }

  // implement SqlValidator
  public boolean getColumnReferenceExpansion() {
    return this.config.columnReferenceExpansion();
  }

  public void setDefaultNullCollation(NullCollation nullCollation) {
    this.config = this.config.withDefaultNullCollation(Objects.requireNonNull(nullCollation));
  }

  public NullCollation getDefaultNullCollation() {
    return this.config.defaultNullCollation();
  }

  // implement SqlValidator
  public void setCallRewrite(boolean rewriteCalls) {
    this.config = this.config.withCallRewrite(rewriteCalls);
  }

  public boolean shouldExpandIdentifiers() {
    return this.config.identifierExpansion();
  }

  protected boolean shouldAllowIntermediateOrderBy() {
    return true;
  }

  private void registerMatchRecognize(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlMatchRecognize call,
      SqlNode enclosingNode,
      @Nullable String alias,
      boolean forceNullable,
      SqlValidatorScope lateralScope) {

    final MatchRecognizeNamespace matchRecognizeNamespace =
        createMatchRecognizeNameSpace(call, enclosingNode);
    registerNamespace(usingScope, alias, matchRecognizeNamespace, forceNullable);

    final MatchRecognizeScope matchRecognizeScope =
        new MatchRecognizeScope(parentScope, call);
    scopes.put(call, matchRecognizeScope);

    // parse input query
    SqlNode expr = call.getTableRef();
    SqlNode newExpr = registerFrom(usingScope, matchRecognizeScope, true, expr,
        expr, null, null, forceNullable, false);
    if (expr != newExpr) {
      call.setOperand(0, newExpr);
    }
  }

  protected MatchRecognizeNamespace createMatchRecognizeNameSpace(
      SqlMatchRecognize call,
      SqlNode enclosingNode) {
    return new MatchRecognizeNamespace(this, call, enclosingNode);
  }

  private void registerPivot(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlPivot pivot,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable) {
    final PivotNamespace namespace =
        createPivotNameSpace(pivot, enclosingNode);
    registerNamespace(usingScope, alias, namespace, forceNullable);

    final SqlValidatorScope scope =
        new PivotScope(parentScope, pivot);
    scopes.put(pivot, scope);

    // parse input query
    SqlNode expr = pivot.query;
    SqlNode newExpr = registerFrom(parentScope, scope, true, expr,
        expr, null, null, forceNullable, false);
    if (expr != newExpr) {
      pivot.setOperand(0, newExpr);
    }
  }

  protected PivotNamespace createPivotNameSpace(SqlPivot call,
      SqlNode enclosingNode) {
    return new PivotNamespace(this, call, enclosingNode);
  }

>>>>>>> 0f33d217a (DX-55026: Add skeleton for alias expansion)
    return expand(expr, scope);
  }

  public boolean isSystemField(RelDataTypeField field) {
    return false;
  }

  public List<List<String>> getFieldOrigins(SqlNode sqlQuery) {
    if (sqlQuery instanceof SqlExplain) {
      return Collections.emptyList();
    }
    final RelDataType rowType = getValidatedNodeType(sqlQuery);
    final int fieldCount = rowType.getFieldCount();
    if (!sqlQuery.isA(SqlKind.QUERY)) {
      return Collections.nCopies(fieldCount, null);
    }
    final List<List<String>> list = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      list.add(getFieldOrigin(sqlQuery, i));
    }
    return ImmutableNullableList.copyOf(list);
  }

  private List<String> getFieldOrigin(SqlNode sqlQuery, int i) {
    if (sqlQuery instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlQuery;
      final SelectScope scope = getRawSelectScope(sqlSelect);
      final List<SqlNode> selectList = scope.getExpandedSelectList();
      final SqlNode selectItem = stripAs(selectList.get(i));
      if (selectItem instanceof SqlIdentifier) {
        final SqlQualified qualified =
            scope.fullyQualify((SqlIdentifier) selectItem);
        SqlValidatorNamespace namespace = qualified.namespace;
        final SqlValidatorTable table = namespace.getTable();
        if (table == null) {
          return null;
        }
        final List<String> origin =
            new ArrayList<>(table.getQualifiedName());
        for (String name : qualified.suffix()) {
          namespace = namespace.lookupChild(name);
          if (namespace == null) {
            return null;
          }
          origin.add(name);
        }
        return origin;
      }
      return null;
    } else if (sqlQuery instanceof SqlOrderBy) {
      return getFieldOrigin(((SqlOrderBy) sqlQuery).query, i);
    } else {
      return null;
    }
  }

  public RelDataType getParameterRowType(SqlNode sqlQuery) {
    // NOTE: We assume that bind variables occur in depth-first tree
    // traversal in the same order that they occurred in the SQL text.
    final List<RelDataType> types = new ArrayList<>();
    // NOTE: but parameters on fetch/offset would be counted twice
    // as they are counted in the SqlOrderBy call and the inner SqlSelect call
    final Set<SqlNode> alreadyVisited = new HashSet<>();
    sqlQuery.accept(
        new SqlShuttle() {

          @Override public SqlNode visit(SqlDynamicParam param) {
            if (alreadyVisited.add(param)) {
              RelDataType type = getValidatedNodeType(param);
              types.add(type);
            }
            return param;
          }
        });
    return typeFactory.createStructType(
        types,
        new AbstractList<String>() {
          @Override public String get(int index) {
            return "?" + index;
          }

          @Override public int size() {
            return types.size();
          }
        });
  }

  public void validateColumnListParams(
      SqlFunction function,
      List<RelDataType> argTypes,
      List<SqlNode> operands) {
    throw new UnsupportedOperationException();
  }

  private static boolean isPhysicalNavigation(SqlKind kind) {
    return kind == SqlKind.PREV || kind == SqlKind.NEXT;
  }

  private static boolean isLogicalNavigation(SqlKind kind) {
    return kind == SqlKind.FIRST || kind == SqlKind.LAST;
  }

  protected static boolean isAggregation(SqlKind kind) {
    return kind == SqlKind.SUM || kind == SqlKind.SUM0
        || kind == SqlKind.AVG || kind == SqlKind.COUNT
        || kind == SqlKind.MAX || kind == SqlKind.MIN;
  }

  private static boolean isRunningOrFinal(SqlKind kind) {
    return kind == SqlKind.RUNNING || kind == SqlKind.FINAL;
  }

  private static boolean isSingleVarRequired(SqlKind kind) {
    return isPhysicalNavigation(kind)
        || isLogicalNavigation(kind)
        || isAggregation(kind);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Common base class for DML statement namespaces.
   */
  public static class DmlNamespace extends IdentifierNamespace {
    protected DmlNamespace(SqlValidatorImpl validator, SqlNode id,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, id, enclosingNode, parentScope);
    }
  }

  /**
   * Namespace for an INSERT statement.
   */
  private static class InsertNamespace extends DmlNamespace {
    private final SqlInsert node;

    InsertNamespace(SqlValidatorImpl validator, SqlInsert node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Objects.requireNonNull(node);
    }

    public SqlInsert getNode() {
      return node;
    }
  }

  /**
   * Namespace for an UPDATE statement.
   */
  private static class UpdateNamespace extends DmlNamespace {
    private final SqlUpdate node;

    UpdateNamespace(SqlValidatorImpl validator, SqlUpdate node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Objects.requireNonNull(node);
    }

    public SqlUpdate getNode() {
      return node;
    }
  }

  /**
   * Namespace for a DELETE statement.
   */
  private static class DeleteNamespace extends DmlNamespace {
    private final SqlDelete node;

    DeleteNamespace(SqlValidatorImpl validator, SqlDelete node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Objects.requireNonNull(node);
    }

    public SqlDelete getNode() {
      return node;
    }
  }

  /**
   * Namespace for a MERGE statement.
   */
  private static class MergeNamespace extends DmlNamespace {
    private final SqlMerge node;

    MergeNamespace(SqlValidatorImpl validator, SqlMerge node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Objects.requireNonNull(node);
    }

    public SqlMerge getNode() {
      return node;
    }
  }

  /**
   * retrieve pattern variables defined
   */
  private class PatternVarVisitor implements SqlVisitor<Void> {
    private MatchRecognizeScope scope;
    PatternVarVisitor(MatchRecognizeScope scope) {
      this.scope = scope;
    }

    @Override public Void visit(SqlLiteral literal) {
      return null;
    }

    @Override public Void visit(SqlCall call) {
      for (int i = 0; i < call.getOperandList().size(); i++) {
        call.getOperandList().get(i).accept(this);
      }
      return null;
    }

    @Override public Void visit(SqlNodeList nodeList) {
      throw Util.needToImplement(nodeList);
    }

    @Override public Void visit(SqlIdentifier id) {
      Preconditions.checkArgument(id.isSimple());
      scope.addPatternVar(id.getSimple());
      return null;
    }

    @Override public Void visit(SqlDataTypeSpec type) {
      throw Util.needToImplement(type);
    }

    @Override public Void visit(SqlDynamicParam param) {
      throw Util.needToImplement(param);
    }

    @Override public Void visit(SqlIntervalQualifier intervalQualifier) {
      throw Util.needToImplement(intervalQualifier);
    }
  }

  /**
   * Visitor which derives the type of a given {@link SqlNode}.
   *
   * <p>Each method must return the derived type. This visitor is basically a
   * single-use dispatcher; the visit is never recursive.
   */
  private class DeriveTypeVisitor implements SqlVisitor<RelDataType> {
    private final SqlValidatorScope scope;

    DeriveTypeVisitor(SqlValidatorScope scope) {
      this.scope = scope;
    }

    public RelDataType visit(SqlLiteral literal) {
      return literal.createSqlType(typeFactory);
    }

    public RelDataType visit(SqlCall call) {
      final SqlOperator operator = call.getOperator();
      return operator.deriveType(SqlValidatorImpl.this, scope, call);
    }

    public RelDataType visit(SqlNodeList nodeList) {
      // Operand is of a type that we can't derive a type for. If the
      // operand is of a peculiar type, such as a SqlNodeList, then you
      // should override the operator's validateCall() method so that it
      // doesn't try to validate that operand as an expression.
      throw Util.needToImplement(nodeList);
    }

    public RelDataType visit(SqlIdentifier id) {
      // First check for builtin functions which don't have parentheses,
      // like "LOCALTIME".
      final SqlCall call = makeNullaryCall(id);
      if (call != null) {
        return call.getOperator().validateOperands(
            SqlValidatorImpl.this,
            scope,
            call);
      }

      RelDataType type = null;
      if (!(scope instanceof EmptyScope)) {
        id = scope.fullyQualify(id).identifier;
      }

      // Resolve the longest prefix of id that we can
      int i;
      for (i = id.names.size() - 1; i > 0; i--) {
        // REVIEW jvs 9-June-2005: The name resolution rules used
        // here are supposed to match SQL:2003 Part 2 Section 6.6
        // (identifier chain), but we don't currently have enough
        // information to get everything right.  In particular,
        // routine parameters are currently looked up via resolve;
        // we could do a better job if they were looked up via
        // resolveColumn.

        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        final SqlValidatorScope.ResolvedImpl resolved =
            new SqlValidatorScope.ResolvedImpl();
        scope.resolve(id.names.subList(0, i), nameMatcher, false, resolved);
        if (resolved.count() == 1) {
          // There's a namespace with the name we seek.
          final SqlValidatorScope.Resolve resolve = resolved.only();
          type = resolve.rowType();
          for (SqlValidatorScope.Step p : Util.skip(resolve.path.steps())) {
            type = type.getFieldList().get(p.i).getType();
          }
          break;
        }
      }

      // Give precedence to namespace found, unless there
      // are no more identifier components.
      if (type == null || id.names.size() == 1) {
        // See if there's a column with the name we seek in
        // precisely one of the namespaces in this scope.
        RelDataType colType = scope.resolveColumn(id.names.get(0), id);
        if (colType != null) {
          type = colType;
        }
        ++i;
      }

      if (type == null) {
        final SqlIdentifier last = id.getComponent(i - 1, i);
        throw newValidationError(last,
            RESOURCE.unknownIdentifier(last.toString()));
      }

      // Resolve rest of identifier
      for (; i < id.names.size(); i++) {
        String name = id.names.get(i);
        final RelDataTypeField field;
        if (name.equals("")) {
          // The wildcard "*" is represented as an empty name. It never
          // resolves to a field.
          name = "*";
          field = null;
        } else {
          final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
          field = nameMatcher.field(type, name);
        }
        if (field == null) {
          throw newValidationError(id.getComponent(i),
              RESOURCE.unknownField(name));
        }
        type = field.getType();
      }
      type =
          SqlTypeUtil.addCharsetAndCollation(
              type,
              getTypeFactory());
      return type;
    }

    public RelDataType visit(SqlDataTypeSpec dataType) {
      // Q. How can a data type have a type?
      // A. When it appears in an expression. (Say as the 2nd arg to the
      //    CAST operator.)
      validateDataType(dataType);
      return dataType.deriveType(SqlValidatorImpl.this);
    }

    public RelDataType visit(SqlDynamicParam param) {
      return unknownType;
    }

    public RelDataType visit(SqlIntervalQualifier intervalQualifier) {
      return typeFactory.createSqlIntervalType(intervalQualifier);
    }
  }

  /**
   * Converts an expression into canonical form by fully-qualifying any
   * identifiers.
   */
  private static class Expander extends SqlScopedShuttle {
    protected final SqlValidatorImpl validator;

    Expander(SqlValidatorImpl validator, SqlValidatorScope scope) {
      super(scope);
      this.validator = validator;
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      // First check for builtin functions which don't have
      // parentheses, like "LOCALTIME".
      final SqlCall call = validator.makeNullaryCall(id);
      if (call != null) {
        return call.accept(this);
      }
      final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;
      SqlNode expandedExpr = expandDynamicStar(id, fqId);
      validator.setOriginal(expandedExpr, id);
      return expandedExpr;
    }

    @Override protected SqlNode visitScoped(SqlCall call) {
      switch (call.getKind()) {
      case SCALAR_QUERY:
      case CURRENT_VALUE:
      case NEXT_VALUE:
      case WITH:
        return call;
      }
      // Only visits arguments which are expressions. We don't want to
      // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
      ArgHandler<SqlNode> argHandler =
          new CallCopyingArgHandler(call, false);
      call.getOperator().acceptCall(this, call, true, argHandler);
      final SqlNode result = argHandler.result();
      validator.setOriginal(result, call);
      return result;
    }

    protected SqlNode expandDynamicStar(SqlIdentifier id, SqlIdentifier fqId) {
      if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
          && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
        // Convert a column ref into ITEM(*, 'col_name')
        // for a dynamic star field in dynTable's rowType.
        SqlNode[] inputs = new SqlNode[2];
        inputs[0] = fqId;
        inputs[1] = SqlLiteral.createCharString(
            Util.last(id.names),
            id.getParserPosition());
        return new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            inputs,
            id.getParserPosition());
      }
      return fqId;
    }
  }

  /**
   * Shuttle which walks over an expression in the ORDER BY clause, replacing
   * usages of aliases with the underlying expression.
   */
  class OrderExpressionExpander extends SqlScopedShuttle {
    private final List<String> aliasList;
    private final SqlSelect select;
    private final SqlNode root;

    OrderExpressionExpander(SqlSelect select, SqlNode root) {
      super(getOrderScope(select));
      this.select = select;
      this.root = root;
      this.aliasList = getNamespace(select).getRowType().getFieldNames();
    }

    public SqlNode go() {
      return root.accept(this);
    }

    public SqlNode visit(SqlLiteral literal) {
      // Ordinal markers, e.g. 'select a, b from t order by 2'.
      // Only recognize them if they are the whole expression,
      // and if the dialect permits.
      if (literal == root && config.sqlConformance().isSortByOrdinal()) {
        switch (literal.getTypeName()) {
        case DECIMAL:
        case DOUBLE:
          final int intValue = literal.intValue(false);
          if (intValue >= 0) {
            if (intValue < 1 || intValue > aliasList.size()) {
              throw newValidationError(
                  literal, RESOURCE.orderByOrdinalOutOfRange());
            }

            // SQL ordinals are 1-based, but Sort's are 0-based
            int ordinal = intValue - 1;
            return nthSelectItem(ordinal, literal.getParserPosition());
          }
          break;
        }
      }

      return super.visit(literal);
    }

    /**
     * Returns the <code>ordinal</code>th item in the select list.
     */
    private SqlNode nthSelectItem(int ordinal, final SqlParserPos pos) {
      // TODO: Don't expand the list every time. Maybe keep an expanded
      // version of each expression -- select lists and identifiers -- in
      // the validator.

      SqlNodeList expandedSelectList =
          expandStar(
              select.getSelectList(),
              select,
              false);
      SqlNode expr = expandedSelectList.get(ordinal);
      expr = stripAs(expr);
      if (expr instanceof SqlIdentifier) {
        expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
      }

      // Create a copy of the expression with the position of the order
      // item.
      return expr.clone(pos);
    }

    public SqlNode visit(SqlIdentifier id) {
      // Aliases, e.g. 'select a as x, b from t order by x'.
      if (id.isSimple()
          && config.sqlConformance().isSortByAlias()) {
        String alias = id.getSimple();
        final SqlValidatorNamespace selectNs = getNamespace(select);
        final RelDataType rowType =
            selectNs.getRowTypeSansSystemColumns();
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        RelDataTypeField field = nameMatcher.field(rowType, alias);
        if (field != null) {
          return nthSelectItem(
              field.getIndex(),
              id.getParserPosition());
        }
      }

      // No match. Return identifier unchanged.
      return getScope().fullyQualify(id).identifier;
    }

    protected SqlNode visitScoped(SqlCall call) {
      // Don't attempt to expand sub-queries. We haven't implemented
      // these yet.
      if (call instanceof SqlSelect) {
        return call;
      }
      if (call.getOperator() instanceof SqlDotOperator) {
        ArgHandler<SqlNode> argHandler =
            new CallCopyingArgHandler(call, false);
        call.getOperator().acceptCall(this, call, true, argHandler);
        return argHandler.result();
      }
      return super.visitScoped(call);
    }
  }


<<<<<<< HEAD
  /**
   * Converts an expression into canonical form by fully-qualifying any
   * identifiers. For common columns in USING, it will be converted to
   * COALESCE(A.col, B.col) AS col.
   */
  static class SelectExpander extends Expander {
    final SqlSelect select;

    SelectExpander(SqlValidatorImpl validator, SelectScope scope,
        SqlSelect select) {
      super(validator, scope);
      this.select = select;
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      final SqlNode node = expandCommonColumn(select, id, (SelectScope) getScope(), validator);
      if (node != id) {
        return node;
      } else {
        return super.visit(id);
      }
    }
  }

=======
  /**
   * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing
   * usages of aliases or ordinals with the underlying expression.
   */
  static class ExtendedExpander extends Expander {
    final SqlSelect select;
    final SqlNode root;
    final boolean havingExpr;
    final Set<SqlNode> aliasOrdinalExpandSet = Sets.newIdentityHashSet();

    ExtendedExpander(SqlValidatorImpl validator, SqlValidatorScope scope,
        SqlSelect select, SqlNode root, boolean havingExpr) {
      super(validator, scope);
      this.select = select;
      this.root = root;
      this.havingExpr = havingExpr;
      if (!havingExpr) {
        /**
         * Add all possible expandable group by expression to set, which is
         * used to check whether expr could be expanded as alias or ordinal.
         */
        switch (root.getKind()) {
        case IDENTIFIER:
        case LITERAL:
          aliasOrdinalExpandSet.add(root);
          break;
        case GROUPING_SETS:
        case ROLLUP:
        case CUBE:
          if (root instanceof SqlBasicCall) {
            List<SqlNode> operandList = ((SqlBasicCall) root).getOperandList();
            for (SqlNode node : operandList) {
              if (node.getKind() == SqlKind.ROW) {
                List<SqlNode> rowOperandList = ((SqlCall) node).getOperandList();
                rowOperandList = Optional.ofNullable(rowOperandList).orElse(ImmutableList.of());
                aliasOrdinalExpandSet.addAll(rowOperandList);
              } else {
                aliasOrdinalExpandSet.add(node);
              }
            }
          }
          break;
        default:
          break;
        }
      }
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      if (id.isSimple()
          && (havingExpr
              ? validator.config().sqlConformance().isHavingAlias()
<<<<<<< HEAD
              : (validator.config().sqlConformance().isGroupByAlias()
                  && aliasOrdinalExpandSet.contains(id)))) {
=======
              : validator.config().sqlConformance().isGroupByAlias())) {
        String name = id.getSimple();
        SqlNode expr = null;
        final SqlNameMatcher nameMatcher =
            validator.catalogReader.nameMatcher();
        int n = 0;
        for (SqlNode s : select.getSelectList()) {
          final String alias = SqlValidatorUtil.getAlias(s, -1);
          if (alias != null && nameMatcher.matches(alias, name)) {
            expr = s;
            n++;
          }
        }
        if (n == 0) {
          return super.visit(id);
        } else if (n > 1) {
          // More than one column has this alias.
          throw validator.newValidationError(id,
              RESOURCE.columnAmbiguous(name));
        }
        if (havingExpr && validator.isAggregate(root)) {
          return super.visit(id);
        }
        expr = stripAs(expr);
        if (expr instanceof SqlIdentifier) {
<<<<<<< HEAD
          SqlIdentifier sid = (SqlIdentifier) expr;
          final SqlIdentifier fqId = getScope().fullyQualify(sid).identifier;
          expr = expandDynamicStar(sid, fqId);
=======
          expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
        }
        return expr;
      }
      if (id.isSimple()) {
        final SelectScope scope = validator.getRawSelectScope(select);
        SqlNode node = expandCommonColumn(select, id, scope, validator);
        if (node != id) {
          return node;
        }
      }
      return super.visit(id);
    }

    public SqlNode visit(SqlLiteral literal) {
      if (havingExpr || !validator.config().sqlConformance().isGroupByOrdinal()) {
        return super.visit(literal);
      }
      boolean isOrdinalLiteral = aliasOrdinalExpandSet.contains(literal);
      if (isOrdinalLiteral) {
        switch (literal.getTypeName()) {
        case DECIMAL:
        case DOUBLE:
          final int intValue = literal.intValue(false);
          if (intValue >= 0) {
            if (intValue < 1 || intValue > select.getSelectList().size()) {
              throw validator.newValidationError(literal,
                  RESOURCE.orderByOrdinalOutOfRange());
            }

            // SQL ordinals are 1-based, but Sort's are 0-based
            int ordinal = intValue - 1;
            return SqlUtil.stripAs(select.getSelectList().get(ordinal));
          }
          break;
        }
      }

      return super.visit(literal);
    }
  }

  /** Information about an identifier in a particular scope. */
  protected static class IdInfo {
    public final SqlValidatorScope scope;
    public final SqlIdentifier id;

    public IdInfo(SqlValidatorScope scope, SqlIdentifier id) {
      this.scope = scope;
      this.id = id;
    }
  }

  /**
   * Utility object used to maintain information about the parameters in a
   * function call.
   */
  protected static class FunctionParamInfo {
    /**
     * Maps a cursor (based on its position relative to other cursor
     * parameters within a function call) to the SELECT associated with the
     * cursor.
     */
    public final Map<Integer, SqlSelect> cursorPosToSelectMap;

    /**
     * Maps a column list parameter to the parent cursor parameter it
     * references. The parameters are id'd by their names.
     */
    public final Map<String, String> columnListParamToParentCursorMap;

    public FunctionParamInfo() {
      cursorPosToSelectMap = new HashMap<>();
      columnListParamToParentCursorMap = new HashMap<>();
    }
  }

  /**
   * Modify the nodes in navigation function
   * such as FIRST, LAST, PREV AND NEXT.
   */
  private static class NavigationModifier extends SqlShuttle {
    public SqlNode go(SqlNode node) {
      return node.accept(this);
    }
  }

  /**
   * Shuttle that expands navigation expressions in a MATCH_RECOGNIZE clause.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code PREV(A.price + A.amount)} &rarr;
   *   {@code PREV(A.price) + PREV(A.amount)}
   *
   *   <li>{@code FIRST(A.price * 2)} &rarr; {@code FIRST(A.PRICE) * 2}
   * </ul>
   */
  private static class NavigationExpander extends NavigationModifier {
    final SqlOperator op;
    final SqlNode offset;

    NavigationExpander() {
      this(null, null);
    }

    NavigationExpander(SqlOperator operator, SqlNode offset) {
      this.offset = offset;
      this.op = operator;
    }

    @Override public SqlNode visit(SqlCall call) {
      SqlKind kind = call.getKind();
      List<SqlNode> operands = call.getOperandList();
      List<SqlNode> newOperands = new ArrayList<>();

      if (call.getFunctionQuantifier() != null
          && call.getFunctionQuantifier().getValue() == SqlSelectKeyword.DISTINCT) {
        final SqlParserPos pos = call.getParserPosition();
        throw SqlUtil.newContextException(pos,
            Static.RESOURCE.functionQuantifierNotAllowed(call.toString()));
      }

      if (isLogicalNavigation(kind) || isPhysicalNavigation(kind)) {
        SqlNode inner = operands.get(0);
        SqlNode offset = operands.get(1);

        // merge two straight prev/next, update offset
        if (isPhysicalNavigation(kind)) {
          SqlKind innerKind = inner.getKind();
          if (isPhysicalNavigation(innerKind)) {
            List<SqlNode> innerOperands = ((SqlCall) inner).getOperandList();
            SqlNode innerOffset = innerOperands.get(1);
            SqlOperator newOperator = innerKind == kind
                ? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS;
            offset = newOperator.createCall(SqlParserPos.ZERO,
              offset, innerOffset);
            inner = call.getOperator().createCall(SqlParserPos.ZERO,
              innerOperands.get(0), offset);
          }
        }
        SqlNode newInnerNode =
            inner.accept(new NavigationExpander(call.getOperator(), offset));
        if (op != null) {
          newInnerNode = op.createCall(SqlParserPos.ZERO, newInnerNode,
              this.offset);
        }
        return newInnerNode;
      }

      if (operands.size() > 0) {
        for (SqlNode node : operands) {
          if (node != null) {
            SqlNode newNode = node.accept(new NavigationExpander());
            if (op != null) {
              newNode = op.createCall(SqlParserPos.ZERO, newNode, offset);
            }
            newOperands.add(newNode);
          } else {
            newOperands.add(null);
          }
        }
        return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
      } else {
        if (op == null) {
          return call;
        } else {
          return op.createCall(SqlParserPos.ZERO, call, offset);
        }
      }
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      if (op == null) {
        return id;
      } else {
        return op.createCall(SqlParserPos.ZERO, id, offset);
      }
    }
  }

  /**
   * Shuttle that replaces {@code A as A.price > PREV(B.price)} with
   * {@code PREV(A.price, 0) > LAST(B.price, 0)}.
   *
   * <p>Replacing {@code A.price} with {@code PREV(A.price, 0)} makes the
   * implementation of
   * {@link RexVisitor#visitPatternFieldRef(RexPatternFieldRef)} more unified.
   * Otherwise, it's difficult to implement this method. If it returns the
   * specified field, then the navigation such as {@code PREV(A.price, 1)}
   * becomes impossible; if not, then comparisons such as
   * {@code A.price > PREV(A.price, 1)} become meaningless.
   */
  private static class NavigationReplacer extends NavigationModifier {
    private final String alpha;

    NavigationReplacer(String alpha) {
      this.alpha = alpha;
    }

    @Override public SqlNode visit(SqlCall call) {
      SqlKind kind = call.getKind();
      if (isLogicalNavigation(kind)
          || isAggregation(kind)
          || isRunningOrFinal(kind)) {
        return call;
      }

      switch (kind) {
      case PREV:
        final List<SqlNode> operands = call.getOperandList();
        if (operands.get(0) instanceof SqlIdentifier) {
          String name = ((SqlIdentifier) operands.get(0)).names.get(0);
          return name.equals(alpha) ? call
              : SqlStdOperatorTable.LAST.createCall(SqlParserPos.ZERO, operands);
        }
      }
      return super.visit(call);
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      if (id.isSimple()) {
        return id;
      }
      SqlOperator operator = id.names.get(0).equals(alpha)
          ? SqlStdOperatorTable.PREV : SqlStdOperatorTable.LAST;

      return operator.createCall(SqlParserPos.ZERO, id,
        SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
    }
  }

  /**
   * Within one navigation function, the pattern var should be same
   */
  private class PatternValidator extends SqlBasicVisitor<Set<String>> {
    private final boolean isMeasure;
    int firstLastCount;
    int prevNextCount;
    int aggregateCount;

    PatternValidator(boolean isMeasure) {
      this(isMeasure, 0, 0, 0);
    }

    PatternValidator(boolean isMeasure, int firstLastCount, int prevNextCount,
        int aggregateCount) {
      this.isMeasure = isMeasure;
      this.firstLastCount = firstLastCount;
      this.prevNextCount = prevNextCount;
      this.aggregateCount = aggregateCount;
    }

    @Override public Set<String> visit(SqlCall call) {
      boolean isSingle = false;
      Set<String> vars = new HashSet<>();
      SqlKind kind = call.getKind();
      List<SqlNode> operands = call.getOperandList();

      if (isSingleVarRequired(kind)) {
        isSingle = true;
        if (isPhysicalNavigation(kind)) {
          if (isMeasure) {
            throw newValidationError(call,
                Static.RESOURCE.patternPrevFunctionInMeasure(call.toString()));
          }
          if (firstLastCount != 0) {
            throw newValidationError(call,
                Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
          }
          prevNextCount++;
        } else if (isLogicalNavigation(kind)) {
          if (firstLastCount != 0) {
            throw newValidationError(call,
                Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
          }
          firstLastCount++;
        } else if (isAggregation(kind)) {
          // cannot apply aggregation in PREV/NEXT, FIRST/LAST
          if (firstLastCount != 0 || prevNextCount != 0) {
            throw newValidationError(call,
                Static.RESOURCE.patternAggregationInNavigation(call.toString()));
          }
          if (kind == SqlKind.COUNT && call.getOperandList().size() > 1) {
            throw newValidationError(call,
                Static.RESOURCE.patternCountFunctionArg());
          }
          aggregateCount++;
        }
      }

      if (isRunningOrFinal(kind) && !isMeasure) {
        throw newValidationError(call,
            Static.RESOURCE.patternRunningFunctionInDefine(call.toString()));
      }

      for (SqlNode node : operands) {
        if (node != null) {
          vars.addAll(
              node.accept(
                  new PatternValidator(isMeasure, firstLastCount, prevNextCount,
                      aggregateCount)));
        }
      }

      if (isSingle) {
        switch (kind) {
        case COUNT:
          if (vars.size() > 1) {
            throw newValidationError(call,
                Static.RESOURCE.patternCountFunctionArg());
          }
          break;
        default:
          if (operands.size() == 0
              || !(operands.get(0) instanceof SqlCall)
              || ((SqlCall) operands.get(0)).getOperator() != SqlStdOperatorTable.CLASSIFIER) {
            if (vars.isEmpty()) {
              throw newValidationError(call,
                  Static.RESOURCE.patternFunctionNullCheck(call.toString()));
            }
            if (vars.size() != 1) {
              throw newValidationError(call,
                  Static.RESOURCE.patternFunctionVariableCheck(call.toString()));
            }
          }
          break;
        }
      }
      return vars;
    }

    @Override public Set<String> visit(SqlIdentifier identifier) {
      boolean check = prevNextCount > 0 || firstLastCount > 0 || aggregateCount > 0;
      Set<String> vars = new HashSet<>();
      if (identifier.names.size() > 1 && check) {
        vars.add(identifier.names.get(0));
      }
      return vars;
    }

    @Override public Set<String> visit(SqlLiteral literal) {
      return ImmutableSet.of();
    }

    @Override public Set<String> visit(SqlIntervalQualifier qualifier) {
      return ImmutableSet.of();
    }

    @Override public Set<String> visit(SqlDataTypeSpec type) {
      return ImmutableSet.of();
    }

    @Override public Set<String> visit(SqlDynamicParam param) {
      return ImmutableSet.of();
    }
  }

  /** Permutation of fields in NATURAL JOIN or USING. */
  private class Permute {
    final List<ImmutableIntList> sources;
    final RelDataType rowType;
    final boolean trivial;
    final int offset;

    Permute(SqlNode from, int offset) {
      this.offset = offset;
      switch (from.getKind()) {
      case JOIN:
        final SqlJoin join = (SqlJoin) from;
        final Permute left = new Permute(join.getLeft(), offset);
        final int fieldCount =
            getValidatedNodeType(join.getLeft()).getFieldList().size();
        final Permute right =
            new Permute(join.getRight(), offset + fieldCount);
        final List<String> names = usingNames(join);
        final List<ImmutableIntList> sources = new ArrayList<>();
        final Set<ImmutableIntList> sourceSet = new HashSet<>();
        final RelDataTypeFactory.Builder b = typeFactory.builder();
        if (names != null) {
          for (String name : names) {
            final RelDataTypeField f = left.field(name);
            final ImmutableIntList source = left.sources.get(f.getIndex());
            sourceSet.add(source);
            final RelDataTypeField f2 = right.field(name);
            final ImmutableIntList source2 = right.sources.get(f2.getIndex());
            sourceSet.add(source2);
            sources.add(source.appendAll(source2));
            final boolean nullable =
                (f.getType().isNullable()
                    || join.getJoinType().generatesNullsOnLeft())
                && (f2.getType().isNullable()
                    || join.getJoinType().generatesNullsOnRight());
            b.add(f).nullable(nullable);
          }
        }
        for (RelDataTypeField f : left.rowType.getFieldList()) {
          final ImmutableIntList source = left.sources.get(f.getIndex());
          if (sourceSet.add(source)) {
            sources.add(source);
            b.add(f);
          }
        }
        for (RelDataTypeField f : right.rowType.getFieldList()) {
          final ImmutableIntList source = right.sources.get(f.getIndex());
          if (sourceSet.add(source)) {
            sources.add(source);
            b.add(f);
          }
        }
        rowType = b.build();
        this.sources = ImmutableList.copyOf(sources);
        this.trivial = left.trivial
            && right.trivial
            && (names == null || names.isEmpty());
        break;

      default:
        rowType = getValidatedNodeType(from);
        this.sources = Functions.generate(rowType.getFieldCount(),
            i -> ImmutableIntList.of(offset + i));
        this.trivial = true;
      }
    }

    private RelDataTypeField field(String name) {
      return catalogReader.nameMatcher().field(rowType, name);
    }

    /** Moves fields according to the permutation. */
    public void permute(List<SqlNode> selectItems,
        List<Map.Entry<String, RelDataType>> fields) {
      if (trivial) {
        return;
      }

      final List<SqlNode> oldSelectItems = ImmutableList.copyOf(selectItems);
      selectItems.clear();
      selectItems.addAll(oldSelectItems.subList(0, offset));
      final List<Map.Entry<String, RelDataType>> oldFields =
          ImmutableList.copyOf(fields);
      fields.clear();
      fields.addAll(oldFields.subList(0, offset));
      for (ImmutableIntList source : sources) {
        final int p0 = source.get(0);
        Map.Entry<String, RelDataType> field = oldFields.get(p0);
        final String name = field.getKey();
        RelDataType type = field.getValue();
        SqlNode selectItem = oldSelectItems.get(p0);
        for (int p1 : Util.skip(source)) {
          final Map.Entry<String, RelDataType> field1 = oldFields.get(p1);
          final SqlNode selectItem1 = oldSelectItems.get(p1);
          final RelDataType type1 = field1.getValue();
          // output is nullable only if both inputs are
          final boolean nullable = type.isNullable() && type1.isNullable();
          final RelDataType type2 =
              SqlTypeUtil.leastRestrictiveForComparison(typeFactory, type,
                  type1);
          selectItem =
              SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                  SqlStdOperatorTable.COALESCE.createCall(SqlParserPos.ZERO,
                      maybeCast(selectItem, type, type2),
                      maybeCast(selectItem1, type1, type2)),
                  new SqlIdentifier(name, SqlParserPos.ZERO));
          type = typeFactory.createTypeWithNullability(type2, nullable);
        }
        fields.add(Pair.of(name, type));
        selectItems.add(selectItem);
      }
    }
  }

  //~ Enums ------------------------------------------------------------------

  /**
   * Validation status.
   */
  public enum Status {
    /**
     * Validation has not started for this scope.
     */
    UNVALIDATED,

    /**
     * Validation is in progress for this scope.
     */
    IN_PROGRESS,

    /**
     * Validation has completed (perhaps unsuccessfully).
     */
    VALID
  }

  /**
   * Used for implementing the dot operator.
   */
  public SqlQualified ensureQualified(
      DelegatingScope scope, SqlIdentifier identifier) {
    return null;
  }
}
