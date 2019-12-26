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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.enumerable.EnumerableBindable;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * <code>RelOptUtil</code> defines static utility methods for use in optimizing
 * {@link RelNode}s.
 */
public abstract class RelOptUtil {
  //~ Static fields/initializers ---------------------------------------------

  static final boolean B = false;
  public static final double EPSILON = 1.0e-5;

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static final com.google.common.base.Predicate<Filter>
      FILTER_PREDICATE =
      RelOptUtil::containsMultisetOrWindowedAgg;

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static final com.google.common.base.Predicate<Project>
      PROJECT_PREDICATE =
      RelOptUtil::containsMultisetOrWindowedAgg;

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static final com.google.common.base.Predicate<Calc> CALC_PREDICATE =
      RelOptUtil::containsMultisetOrWindowedAgg;

  //~ Methods ----------------------------------------------------------------

  /**
   * Whether this node is a limit without sort specification.
   */
  public static boolean isPureLimit(RelNode rel) {
    return isLimit(rel) && !isOrder(rel);
  }

  /**
   * Whether this node is a sort without limit specification.
   */
  public static boolean isPureOrder(RelNode rel) {
    return !isLimit(rel) && isOrder(rel);
  }

  /**
   * Whether this node contains a limit specification.
   */
  public static boolean isLimit(RelNode rel) {
    if ((rel instanceof Sort) && ((Sort) rel).fetch != null) {
      return true;
    }
    return false;
  }

  /**
   * Whether this node contains a sort specification.
   */
  public static boolean isOrder(RelNode rel) {
    if ((rel instanceof Sort) && !((Sort) rel).getCollation().getFieldCollations().isEmpty()) {
      return true;
    }
    return false;
  }

  /**
   * Returns a set of tables used by this expression or its children
   */
  public static Set<RelOptTable> findTables(RelNode rel) {
    return new LinkedHashSet<>(findAllTables(rel));
  }

  /**
   * Returns a list of all tables used by this expression or its children
   */
  public static List<RelOptTable> findAllTables(RelNode rel) {
    final Multimap<Class<? extends RelNode>, RelNode> nodes =
        rel.getCluster().getMetadataQuery().getNodeTypes(rel);
    final List<RelOptTable> usedTables = new ArrayList<>();
    for (Entry<Class<? extends RelNode>, Collection<RelNode>> e : nodes.asMap().entrySet()) {
      if (TableScan.class.isAssignableFrom(e.getKey())) {
        for (RelNode node : e.getValue()) {
          usedTables.add(node.getTable());
        }
      }
    }
    return usedTables;
  }

  /**
   * Returns a list of all table qualified names used by this expression
   * or its children.
   */
  public static List<String> findAllTableQualifiedNames(RelNode rel) {
    return Lists.transform(findAllTables(rel),
        table -> table.getQualifiedName().toString());

  }

  /**
   * Returns a list of variables set by a relational expression or its
   * descendants.
   */
  public static Set<CorrelationId> getVariablesSet(RelNode rel) {
    VariableSetVisitor visitor = new VariableSetVisitor();
    go(visitor, rel);
    return visitor.variables;
  }

  @Deprecated // to be removed before 2.0
  public static List<CorrelationId> getVariablesSetAndUsed(RelNode rel0,
      RelNode rel1) {
    Set<CorrelationId> set = getVariablesSet(rel0);
    if (set.size() == 0) {
      return ImmutableList.of();
    }
    Set<CorrelationId> used = getVariablesUsed(rel1);
    if (used.size() == 0) {
      return ImmutableList.of();
    }
    final List<CorrelationId> result = new ArrayList<>();
    for (CorrelationId s : set) {
      if (used.contains(s) && !result.contains(s)) {
        result.add(s);
      }
    }
    return result;
  }

  /**
   * Returns a set of variables used by a relational expression or its
   * descendants.
   *
   * <p>The set may contain "duplicates" (variables with different ids that,
   * when resolved, will reference the same source relational expression).
   *
   * <p>The item type is the same as
   * {@link org.apache.calcite.rex.RexCorrelVariable#id}.
   */
  public static Set<CorrelationId> getVariablesUsed(RelNode rel) {
    CorrelationCollector visitor = new CorrelationCollector();
    rel.accept(visitor);
    return visitor.vuv.variables;
  }

  /** Finds which columns of a correlation variable are used within a
   * relational expression. */
  public static ImmutableBitSet correlationColumns(CorrelationId id,
      RelNode rel) {
    final CorrelationCollector collector = new CorrelationCollector();
    rel.accept(collector);
    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int field : collector.vuv.variableFields.get(id)) {
      if (field >= 0) {
        builder.set(field);
      }
    }
    return builder.build();
  }

  /** Returns true, and calls {@link Litmus#succeed()} if a given relational
   * expression does not contain a given correlation. */
  public static boolean notContainsCorrelation(RelNode r,
      CorrelationId correlationId, Litmus litmus) {
    final Set<CorrelationId> set = getVariablesUsed(r);
    if (!set.contains(correlationId)) {
      return litmus.succeed();
    } else {
      return litmus.fail("contains {}", correlationId);
    }
  }

  /**
   * Sets a {@link RelVisitor} going on a given relational expression, and
   * returns the result.
   */
  public static void go(
      RelVisitor visitor,
      RelNode p) {
    try {
      visitor.go(p);
    } catch (Exception e) {
      throw new RuntimeException("while visiting tree", e);
    }
  }

  /**
   * Returns a list of the types of the fields in a given struct type. The
   * list is immutable.
   *
   * @param type Struct type
   * @return List of field types
   * @see org.apache.calcite.rel.type.RelDataType#getFieldNames()
   */
  public static List<RelDataType> getFieldTypeList(final RelDataType type) {
    return Lists.transform(type.getFieldList(), RelDataTypeField::getType);
  }

  public static boolean areRowTypesEqual(
      RelDataType rowType1,
      RelDataType rowType2,
      boolean compareNames) {
    if (rowType1 == rowType2) {
      return true;
    }
    if (compareNames) {
      // if types are not identity-equal, then either the names or
      // the types must be different
      return false;
    }
    if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
      return false;
    }
    final List<RelDataTypeField> f1 = rowType1.getFieldList();
    final List<RelDataTypeField> f2 = rowType2.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();
      // If one of the types is ANY comparison should succeed
      if (type1.getSqlTypeName() == SqlTypeName.ANY
          || type2.getSqlTypeName() == SqlTypeName.ANY) {
        continue;
      }
      if (!type1.equals(type2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Verifies that a row type being added to an equivalence class matches the
   * existing type, raising an assertion if this is not the case.
   *
   * @param originalRel      canonical rel for equivalence class
   * @param newRel           rel being added to equivalence class
   * @param equivalenceClass object representing equivalence class
   */
  public static void verifyTypeEquivalence(
      RelNode originalRel,
      RelNode newRel,
      Object equivalenceClass) {
    RelDataType expectedRowType = originalRel.getRowType();
    RelDataType actualRowType = newRel.getRowType();

    // Row types must be the same, except for field names.
    if (areRowTypesEqual(expectedRowType, actualRowType, false)) {
      return;
    }

    String s = "Cannot add expression of different type to set:\n"
        + "set type is " + expectedRowType.getFullTypeString()
        + "\nexpression type is " + actualRowType.getFullTypeString()
        + "\nset is " + equivalenceClass.toString()
        + "\nexpression is " + RelOptUtil.toString(newRel);
    throw new AssertionError(s);
  }

  /**
   * Copy the {@link org.apache.calcite.rel.hint.RelHint}s from {@code originalRel}
   * to {@code newRel} if both of them are {@link Hintable}.
   *
   * <p>The hints are filtered by the hint strategies when attaching to the {@code newRel}.
   *
   * @param originalRel Original relational expression
   * @param equiv       New equivalent relational expression
   * @return A copy of {@code newRel} with attached qualified hints from {@code originalRel},
   * or {@code newRel} directly if one of them are not {@link Hintable}
   */
  public static RelNode propagateRelHints(RelNode originalRel, RelNode equiv) {
    if (!(originalRel instanceof Hintable)
            || ((Hintable) originalRel).getHints().isEmpty()) {
      return equiv;
    }
    final RelShuttle shuttle =
            new SubTreeHintPropagateShuttle(
                    originalRel.getCluster().getHintStrategies(),
                    ((Hintable) originalRel).getHints());
    return equiv.accept(shuttle);
  }

  /**
   * Copy the {@link org.apache.calcite.rel.hint.RelHint}s from {@code originalRel}
   * to {@code newRel} if both of them are {@link Hintable}.
   *
   * <p>The hints would be attached directly(e.g. without any filtering).
   *
   * @param originalRel Original relational expression
   * @param newRel      New relational expression
   * @return A copy of {@code newRel} with attached hints from {@code originalRel},
   * or {@code newRel} directly if one of them are not {@link Hintable}
   */
  public static RelNode copyRelHints(RelNode originalRel, RelNode newRel) {
    return copyRelHints(originalRel, newRel, true);
  }

  /**
   * Copy the {@link org.apache.calcite.rel.hint.RelHint}s from {@code originalRel}
   * to {@code newRel} if both of them are {@link Hintable}.
   *
   * <p>The hints would be filtered by the specified hint strategies
   * if {@code filterHints} is true.
   *
   * @param originalRel Original relational expression
   * @param newRel      New relational expression
   * @param filterHints Flag saying if to filter out unqualified hints for {@code newRel}
   * @return A copy of {@code newRel} with attached hints from {@code originalRel},
   * or {@code newRel} directly if one of them are not {@link Hintable}
   */
  public static RelNode copyRelHints(RelNode originalRel, RelNode newRel, boolean filterHints) {
    if (originalRel instanceof Hintable
        && newRel instanceof Hintable
        && ((Hintable) originalRel).getHints().size() > 0) {
      HintStrategyTable hintStrategies = originalRel.getCluster().getHintStrategies();
      final List<RelHint> hints = ((Hintable) originalRel).getHints();
      // Keep all the hints of project node for 2 reasons:
      // 1. Keep sync with the hints propagation logic,
      // see RelHintPropagateShuttle for details.
      // 2. We may re-propagate these hints when decorrelating a query.
      if (originalRel instanceof Project
          && newRel instanceof Project) {
        return ((Hintable) newRel).attachHints(hints);
      } else {
        return ((Hintable) newRel)
            .attachHints(hintStrategies.apply(hints, newRel));
      }
    }
    return newRel;
  }

  /**
   * Propagates the relational expression hints from root node to leaf node.
   *
   * @param rel   The relational expression
   * @param reset Flag saying if to reset the existing hints before the propagation
   * @return New relational expression with hints propagated
   */
  public static RelNode propagateRelHints(RelNode rel, boolean reset) {
    if (reset) {
      rel = rel.accept(new ResetHintsShuttle());
    }
    final RelShuttle shuttle = new RelHintPropagateShuttle(rel.getCluster().getHintStrategies());
    return rel.accept(shuttle);
  }

  /**
   * Returns a permutation describing where output fields come from. In
   * the returned map, value of {@code map.getTargetOpt(i)} is {@code n} if
   * field {@code i} projects input field {@code n} or applies a cast on
   * {@code n}, -1 if it is another expression.
   */
  public static Mappings.TargetMapping permutationIgnoreCast(
      List<RexNode> nodes,
      RelDataType inputRowType) {
    final Mappings.TargetMapping mapping =
        Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            nodes.size(),
            inputRowType.getFieldCount());
    for (Ord<RexNode> node : Ord.zip(nodes)) {
      if (node.e instanceof RexInputRef) {
        mapping.set(
            node.i,
            ((RexInputRef) node.e).getIndex());
      } else if (node.e.isA(SqlKind.CAST)) {
        final RexNode operand = ((RexCall) node.e).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          mapping.set(node.i, ((RexInputRef) operand).getIndex());
        }
      }
    }
    return mapping;
  }

  /**
   * Returns a permutation describing where output fields come from. In
   * the returned map, value of {@code map.getTargetOpt(i)} is {@code n} if
   * field {@code i} projects input field {@code n}, -1 if it is an
   * expression.
   */
  public static Mappings.TargetMapping permutation(
      List<RexNode> nodes,
      RelDataType inputRowType) {
    final Mappings.TargetMapping mapping =
        Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            nodes.size(),
            inputRowType.getFieldCount());
    for (Ord<RexNode> node : Ord.zip(nodes)) {
      if (node.e instanceof RexInputRef) {
        mapping.set(
            node.i,
            ((RexInputRef) node.e).getIndex());
      }
    }
    return mapping;
  }

  /**
   * Returns a permutation describing where the Project's fields come from
   * after the Project is pushed down.
   */
  public static Mappings.TargetMapping permutationPushDownProject(
      List<RexNode> nodes,
      RelDataType inputRowType,
      int sourceOffset,
      int targetOffset) {
    final Mappings.TargetMapping mapping =
        Mappings.create(MappingType.PARTIAL_FUNCTION,
            inputRowType.getFieldCount() + sourceOffset,
            nodes.size() + targetOffset);
    for (Ord<RexNode> node : Ord.zip(nodes)) {
      if (node.e instanceof RexInputRef) {
        mapping.set(
            ((RexInputRef) node.e).getIndex() + sourceOffset,
            node.i + targetOffset);
      }
    }
    return mapping;
  }

  @Deprecated // to be removed before 2.0
  public static RelNode createExistsPlan(
      RelOptCluster cluster,
      RelNode seekRel,
      List<RexNode> conditions,
      RexLiteral extraExpr,
      String extraName) {
    assert extraExpr == null || extraName != null;
    RelNode ret = seekRel;

    if ((conditions != null) && (conditions.size() > 0)) {
      RexNode conditionExp =
          RexUtil.composeConjunction(
              cluster.getRexBuilder(), conditions, true);

      final RelFactories.FilterFactory factory =
          RelFactories.DEFAULT_FILTER_FACTORY;
      ret = factory.createFilter(ret, conditionExp);
    }

    if (extraExpr != null) {
      RexBuilder rexBuilder = cluster.getRexBuilder();

      assert extraExpr == rexBuilder.makeLiteral(true);

      // this should only be called for the exists case
      // first stick an Agg on top of the sub-query
      // agg does not like no agg functions so just pretend it is
      // doing a min(TRUE)

      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(cluster, null);
      ret = relBuilder.push(ret)
          .project(extraExpr)
          .aggregate(relBuilder.groupKey(),
              relBuilder.min(relBuilder.field(0)).as(extraName))
          .build();
    }

    return ret;
  }

  @Deprecated // to be removed before 2.0
  public static Exists createExistsPlan(
      RelNode seekRel,
      SubQueryType subQueryType,
      Logic logic,
      boolean notIn) {
    final RelBuilder relBuilder =
        RelFactories.LOGICAL_BUILDER.create(seekRel.getCluster(), null);
    return createExistsPlan(seekRel, subQueryType, logic, notIn, relBuilder);
  }

  /**
   * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code>
   * statements.
   *
   * @see org.apache.calcite.sql2rel.SqlToRelConverter
   * SqlToRelConverter#convertExists
   *
   * @param seekRel    A query rel, for example the resulting rel from 'select *
   *                   from emp' or 'values (1,2,3)' or '('Foo', 34)'.
   * @param subQueryType Sub-query type
   * @param logic  Whether to use 2- or 3-valued boolean logic
   * @param notIn Whether the operator is NOT IN
   * @param relBuilder Builder for relational expressions
   *
   * @return A pair of a relational expression which outer joins a boolean
   * condition column, and a numeric offset. The offset is 2 if column 0 is
   * the number of rows and column 1 is the number of rows with not-null keys;
   * 0 otherwise.
   */
  public static Exists createExistsPlan(
      RelNode seekRel,
      SubQueryType subQueryType,
      Logic logic,
      boolean notIn,
      RelBuilder relBuilder) {
    switch (subQueryType) {
    case SCALAR:
      return new Exists(seekRel, false, true);
    }

    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      if (notIn && !containsNullableFields(seekRel)) {
        logic = Logic.TRUE_FALSE;
      }
    }
    RelNode ret = seekRel;
    final RelOptCluster cluster = seekRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final int keyCount = ret.getRowType().getFieldCount();
    final boolean outerJoin = notIn
        || logic == RelOptUtil.Logic.TRUE_FALSE_UNKNOWN;
    if (!outerJoin) {
      final LogicalAggregate aggregate =
          LogicalAggregate.create(ret, ImmutableList.of(), ImmutableBitSet.range(keyCount), null,
              ImmutableList.of());
      return new Exists(aggregate, false, false);
    }

    // for IN/NOT IN, it needs to output the fields
    final List<RexNode> exprs = new ArrayList<>();
    if (subQueryType == SubQueryType.IN) {
      for (int i = 0; i < keyCount; i++) {
        exprs.add(rexBuilder.makeInputRef(ret, i));
      }
    }

    final int projectedKeyCount = exprs.size();
    exprs.add(rexBuilder.makeBigintLiteral(BigDecimal.ONE));

    ret = relBuilder.push(ret)
        .project(exprs)
        .aggregate(
            relBuilder.groupKey(ImmutableBitSet.range(projectedKeyCount)),
            relBuilder.min(relBuilder.field(projectedKeyCount)))
        .build();

    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      return new Exists(ret, true, true);
    default:
      return new Exists(ret, false, true);
    }
  }

  @Deprecated // to be removed before 2.0
  public static RelNode createRenameRel(
      RelDataType outputType,
      RelNode rel) {
    RelDataType inputType = rel.getRowType();
    List<RelDataTypeField> inputFields = inputType.getFieldList();
    int n = inputFields.size();

    List<RelDataTypeField> outputFields = outputType.getFieldList();
    assert outputFields.size() == n
        : "rename: field count mismatch: in=" + inputType
        + ", out" + outputType;

    final List<Pair<RexNode, String>> renames = new ArrayList<>();
    for (Pair<RelDataTypeField, RelDataTypeField> pair
        : Pair.zip(inputFields, outputFields)) {
      final RelDataTypeField inputField = pair.left;
      final RelDataTypeField outputField = pair.right;
      assert inputField.getType().equals(outputField.getType());
      final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
      renames.add(
          Pair.of(
              rexBuilder.makeInputRef(inputField.getType(),
                  inputField.getIndex()),
              outputField.getName()));
    }
    final RelBuilder relBuilder =
        RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
    return relBuilder.push(rel)
        .project(Pair.left(renames), Pair.right(renames), true)
        .build();
  }

  @Deprecated // to be removed before 2.0
  public static RelNode createFilter(RelNode child, RexNode condition) {
    final RelFactories.FilterFactory factory =
        RelFactories.DEFAULT_FILTER_FACTORY;
    return factory.createFilter(child, condition);
  }

  @Deprecated // to be removed before 2.0
  public static RelNode createFilter(RelNode child, RexNode condition,
      RelFactories.FilterFactory filterFactory) {
    return filterFactory.createFilter(child, condition);
  }

  /** Creates a filter, using the default filter factory,
   * or returns the original relational expression if the
   * condition is trivial. */
  public static RelNode createFilter(RelNode child,
      Iterable<? extends RexNode> conditions) {
    return createFilter(child, conditions, RelFactories.DEFAULT_FILTER_FACTORY);
  }

  /** Creates a filter using the default factory,
   * or returns the original relational expression if the
   * condition is trivial. */
  public static RelNode createFilter(RelNode child,
      Iterable<? extends RexNode> conditions,
      RelFactories.FilterFactory filterFactory) {
    final RelOptCluster cluster = child.getCluster();
    final RexNode condition =
        RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, true);
    if (condition == null) {
      return child;
    } else {
      return filterFactory.createFilter(child, condition);
    }
  }

  @Deprecated // to be removed before 2.0
  public static RelNode createNullFilter(
      RelNode rel,
      Integer[] fieldOrdinals) {
    RexNode condition = null;
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RelDataType rowType = rel.getRowType();
    int n;
    if (fieldOrdinals != null) {
      n = fieldOrdinals.length;
    } else {
      n = rowType.getFieldCount();
    }
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < n; ++i) {
      int iField;
      if (fieldOrdinals != null) {
        iField = fieldOrdinals[i];
      } else {
        iField = i;
      }
      RelDataType type = fields.get(iField).getType();
      if (!type.isNullable()) {
        continue;
      }
      RexNode newCondition =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_NOT_NULL,
              rexBuilder.makeInputRef(type, iField));
      if (condition == null) {
        condition = newCondition;
      } else {
        condition =
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                condition,
                newCondition);
      }
    }
    if (condition == null) {
      // no filtering required
      return rel;
    }

    final RelFactories.FilterFactory factory =
        RelFactories.DEFAULT_FILTER_FACTORY;
    return factory.createFilter(rel, condition);
  }

  /**
   * Creates a projection which casts a rel's output to a desired row type.
   *
   * <p>No need to create new projection if {@code rel} is already a project,
   * instead, create a projection with the input of {@code rel} and the new
   * cast expressions.
   *
   * @param rel         producer of rows to be converted
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   * @return conversion rel
   */
  public static RelNode createCastRel(
      final RelNode rel,
      RelDataType castRowType,
      boolean rename) {
    return createCastRel(
        rel, castRowType, rename, RelFactories.DEFAULT_PROJECT_FACTORY);
  }

  /**
   * Creates a projection which casts a rel's output to a desired row type.
   *
   * <p>No need to create new projection if {@code rel} is already a project,
   * instead, create a projection with the input of {@code rel} and the new
   * cast expressions.
   *
   * @param rel         producer of rows to be converted
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   * @param projectFactory Project Factory
   * @return conversion rel
   */
  public static RelNode createCastRel(
      final RelNode rel,
      RelDataType castRowType,
      boolean rename,
      RelFactories.ProjectFactory projectFactory) {
    assert projectFactory != null;
    RelDataType rowType = rel.getRowType();
    if (areRowTypesEqual(rowType, castRowType, rename)) {
      // nothing to do
      return rel;
    }
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    List<RexNode> castExps;
    RelNode input;
    List<RelHint> hints = ImmutableList.of();
    if (rel instanceof Project) {
      // No need to create another project node if the rel
      // is already a project.
      final Project project = (Project) rel;
      castExps = RexUtil.generateCastExpressions(
          rexBuilder,
          castRowType,
          ((Project) rel).getProjects());
      input = rel.getInput(0);
      hints = project.getHints();
    } else {
      castExps = RexUtil.generateCastExpressions(
          rexBuilder,
          castRowType,
          rowType);
      input = rel;
    }
    if (rename) {
      // Use names and types from castRowType.
      return projectFactory.createProject(input, hints, castExps,
          castRowType.getFieldNames());
    } else {
      // Use names from rowType, types from castRowType.
      return projectFactory.createProject(input, hints, castExps,
          rowType.getFieldNames());
    }
  }

  /** Gets all fields in an aggregate. */
  public static Set<Integer> getAllFields(Aggregate aggregate) {
    final Set<Integer> allFields = new TreeSet<>();
    allFields.addAll(aggregate.getGroupSet().asList());
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      allFields.addAll(aggregateCall.getArgList());
      if (aggregateCall.filterArg >= 0) {
        allFields.add(aggregateCall.filterArg);
      }
      allFields.addAll(RelCollations.ordinals(aggregateCall.collation));
    }
    return allFields;
  }

  /**
   * Creates a LogicalAggregate that removes all duplicates from the result of
   * an underlying relational expression.
   *
   * @param rel underlying rel
   * @return rel implementing SingleValueAgg
   */
  public static RelNode createSingleValueAggRel(
      RelOptCluster cluster,
      RelNode rel) {
    final int aggCallCnt = rel.getRowType().getFieldCount();
    final List<AggregateCall> aggCalls = new ArrayList<>();

    for (int i = 0; i < aggCallCnt; i++) {
      aggCalls.add(
          AggregateCall.create(SqlStdOperatorTable.SINGLE_VALUE, false, false,
              false, ImmutableList.of(i), -1, RelCollations.EMPTY, 0, rel, null,
              null));
    }

    return LogicalAggregate.create(rel, ImmutableList.of(), ImmutableBitSet.of(),
        null, aggCalls);
  }

  /** @deprecated Use {@link RelBuilder#distinct()}. */
  @Deprecated // to be removed before 2.0
  public static RelNode createDistinctRel(RelNode rel) {
    return LogicalAggregate.create(rel,
        ImmutableList.of(),
        ImmutableBitSet.range(rel.getRowType().getFieldCount()), null,
        ImmutableList.of());
  }

  @Deprecated // to be removed before 2.0
  public static boolean analyzeSimpleEquiJoin(
      LogicalJoin join,
      int[] joinFieldOrdinals) {
    RexNode joinExp = join.getCondition();
    if (joinExp.getKind() != SqlKind.EQUALS) {
      return false;
    }
    RexCall binaryExpression = (RexCall) joinExp;
    RexNode leftComparand = binaryExpression.operands.get(0);
    RexNode rightComparand = binaryExpression.operands.get(1);
    if (!(leftComparand instanceof RexInputRef)) {
      return false;
    }
    if (!(rightComparand instanceof RexInputRef)) {
      return false;
    }

    final int leftFieldCount =
        join.getLeft().getRowType().getFieldCount();
    RexInputRef leftFieldAccess = (RexInputRef) leftComparand;
    if (!(leftFieldAccess.getIndex() < leftFieldCount)) {
      // left field must access left side of join
      return false;
    }

    RexInputRef rightFieldAccess = (RexInputRef) rightComparand;
    if (!(rightFieldAccess.getIndex() >= leftFieldCount)) {
      // right field must access right side of join
      return false;
    }

    joinFieldOrdinals[0] = leftFieldAccess.getIndex();
    joinFieldOrdinals[1] = rightFieldAccess.getIndex() - leftFieldCount;
    return true;
  }

  /**
   * Splits out the equi-join components of a join condition, and returns
   * what's left. For example, given the condition
   *
   * <blockquote><code>L.A = R.X AND L.B = L.C AND (L.D = 5 OR L.E =
   * R.Y)</code></blockquote>
   *
   * <p>returns
   *
   * <ul>
   * <li>leftKeys = {A}
   * <li>rightKeys = {X}
   * <li>rest = L.B = L.C AND (L.D = 5 OR L.E = R.Y)</li>
   * </ul>
   *
   * @param left      left input to join
   * @param right     right input to join
   * @param condition join condition
   * @param leftKeys  The ordinals of the fields from the left input which are
   *                  equi-join keys
   * @param rightKeys The ordinals of the fields from the right input which
   *                  are equi-join keys
   * @param filterNulls List of boolean values for each join key position
   *                    indicating whether the operator filters out nulls or not.
   *                    Value is true if the operator is EQUALS and false if the
   *                    operator is IS NOT DISTINCT FROM (or an expanded version).
   *                    If <code>filterNulls</code> is null, only join conditions
   *                    with EQUALS operators are considered equi-join components.
   *                    Rest (including IS NOT DISTINCT FROM) are returned in
   *                    remaining join condition.
   *
   * @return remaining join filters that are not equijoins; may return a
   * {@link RexLiteral} true, but never null
   */
  public static RexNode splitJoinCondition(
      RelNode left,
      RelNode right,
      RexNode condition,
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      List<Boolean> filterNulls) {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitJoinCondition(left, right, condition, leftKeys, rightKeys,
        filterNulls, nonEquiList);

    return RexUtil.composeConjunction(
        left.getCluster().getRexBuilder(), nonEquiList);
  }

  /** As
   * {@link #splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)},
   * but writes non-equi conditions to a conjunctive list. */
  public static void splitJoinCondition(
      RelNode left,
      RelNode right,
      RexNode condition,
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      List<Boolean> filterNulls,
      List<RexNode> nonEquiList) {
    splitJoinCondition(
        left.getCluster().getRexBuilder(),
        left.getRowType().getFieldCount(),
        condition,
        leftKeys,
        rightKeys,
        filterNulls,
        nonEquiList);

  /** Check if it is the join whose condition is based on column equality,
   * this mostly depends on the physical implementation of the join. */
  public static boolean forceEquiJoin(Join join) {
    return join.isSemiJoin() || join instanceof EquiJoin;
  }
=======
=======

  /**
   * A {@code RelShuttle} which propagates the given hints to the sub-tree from the root node.
   * It stops the search of current path if the node already has hints or the whole propagation
   * if there is already a matched node.
   *
   * <p>Given a plan:
   *
   * <blockquote><pre>
   *            Filter
   *                |
   *               Join
   *              /    \
   *            Scan  Project (Hint2)
   *                     |
   *                    Scan2
   * </pre></blockquote>
   *
   * <p>The [Filter, Join, Scan] are the candidates(in sequence) to propagate,
   * the whole propagation ends if we append the given hints to a node successfully.
   */
  private static class SubTreeHintPropagateShuttle extends RelHomogeneousShuttle {
    /** Stack recording the appended inheritPath. */
    private final List<Integer> appendPath = new ArrayList<>();

    /**
     * The hint strategies to decide if a hint should be attached to
     * a relational expression.
     */
    private final HintStrategyTable hintStrategies;

    /** Hints to propagate. */
    private final List<RelHint> hints;

    SubTreeHintPropagateShuttle(HintStrategyTable hintStrategies, List<RelHint> hints) {
      this.hintStrategies = hintStrategies;
      this.hints = hints;
    }

    /**
     * Visits a particular child of a parent.
     */
    @Override protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      appendPath.add(i);
      try {
        RelNode child2 = child.accept(this);
        if (child2 != child) {
          final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
          newInputs.set(i, child2);
          return parent.copy(parent.getTraitSet(), newInputs);
        }
        return parent;
      } finally {
        // Remove the last element.
        appendPath.remove(appendPath.size() - 1);
      }
    }

    @Override public RelNode visit(RelNode other) {
      if (this.appendPath.size() > 3) {
        // Returns early if the visiting depth is bigger than 3
        return other;
      }
      if (other instanceof Hintable) {
        return visitHintable(other);
      } else {
        return visitChildren(other);
      }
    }

    /**
     * Handle the {@link Hintable}s.
     *
     * <p>Try to propagate the given hints to the node, the propagation finishes if:
     *
     * <ul>
     *   <li>This hintable already has hints, that means, the rel is definitely
     *   not created by a planner rule(or copied by the planner rule)</li>
     *   <li>This hintable appended the hints successfully</li>
     * </ul>
     *
     * @param node {@link Hintable} to handle
     * @return New copy of the {@code hintable} with propagated hints attached
     */
    private RelNode visitHintable(RelNode node) {
      final List<RelHint> topHints = ((Hintable) node).getHints();
      final boolean hasHints = !topHints.isEmpty();
      if (hasHints) {
        // This node is definitely not created by the planner, returns early.
        return node;
      }
      final RelNode node1 = attachHints(node);
      if (node1 != node) {
        return node1;
      }
      return visitChildren(node);
    }

    private RelNode attachHints(RelNode original) {
      assert original instanceof Hintable;
      final List<RelHint> hints = this.hints.stream()
              .map(hint -> copyWithAppendPath(hint, appendPath))
              .collect(Collectors.toList());
      final List<RelHint> filteredHints = hintStrategies.apply(hints, original);
      if (!filteredHints.isEmpty()) {
        return ((Hintable) original).attachHints(filteredHints);
      }
      return original;
    }

    private static RelHint copyWithAppendPath(RelHint hint,
                                              List<Integer> appendPaths) {
      if (appendPaths.isEmpty()) {
        return hint;
      } else {
        List<Integer> newPath = new ArrayList<>(hint.inheritPath);
        newPath.addAll(appendPaths);
        return hint.copy(newPath);
      }
    }
  }
}

  /**
   * A {@code RelShuttle} which propagates the given hints to the sub-tree from the root node.
   * It stops the search of current path if the node already has hints or the whole propagation
   * if there is already a matched node.
   *
   * <p>Given a plan:
   *
   * <blockquote><pre>
   *            Filter
   *                |
   *               Join
   *              /    \
   *            Scan  Project (Hint2)
   *                     |
   *                    Scan2
   * </pre></blockquote>
   *
   * <p>The [Filter, Join, Scan] are the candidates(in sequence) to propagate,
   * the whole propagation ends if we append the given hints to a node successfully.
   */
  private static class SubTreeHintPropagateShuttle extends RelHomogeneousShuttle {
    /** Stack recording the appended inheritPath. */
    private final List<Integer> appendPath = new ArrayList<>();

    /**
     * The hint strategies to decide if a hint should be attached to
     * a relational expression.
     */
    private final HintStrategyTable hintStrategies;

    /** Hints to propagate. */
    private final List<RelHint> hints;

    SubTreeHintPropagateShuttle(HintStrategyTable hintStrategies, List<RelHint> hints) {
      this.hintStrategies = hintStrategies;
      this.hints = hints;
    }

    /**
     * Visits a particular child of a parent.
     */
    @Override protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      appendPath.add(i);
      try {
        RelNode child2 = child.accept(this);
        if (child2 != child) {
          final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
          newInputs.set(i, child2);
          return parent.copy(parent.getTraitSet(), newInputs);
        }
        return parent;
      } finally {
        // Remove the last element.
        appendPath.remove(appendPath.size() - 1);
      }
    }

    @Override public RelNode visit(RelNode other) {
      if (this.appendPath.size() > 3) {
        // Returns early if the visiting depth is bigger than 3
        return other;
      }
      if (other instanceof Hintable) {
        return visitHintable(other);
      } else {
        return visitChildren(other);
      }
    }

    /**
     * Handle the {@link Hintable}s.
     *
     * <p>Try to propagate the given hints to the node, the propagation finishes if:
     *
     * <ul>
     *   <li>This hintable already has hints, that means, the rel is definitely
     *   not created by a planner rule(or copied by the planner rule)</li>
     *   <li>This hintable appended the hints successfully</li>
     * </ul>
     *
     * @param node {@link Hintable} to handle
     * @return New copy of the {@code hintable} with propagated hints attached
     */
    private RelNode visitHintable(RelNode node) {
      final List<RelHint> topHints = ((Hintable) node).getHints();
      final boolean hasHints = !topHints.isEmpty();
      if (hasHints) {
        // This node is definitely not created by the planner, returns early.
        return node;
      }
      final RelNode node1 = attachHints(node);
      if (node1 != node) {
        return node1;
      }
      return visitChildren(node);
    }

    private RelNode attachHints(RelNode original) {
      assert original instanceof Hintable;
      final List<RelHint> hints = this.hints.stream()
              .map(hint -> copyWithAppendPath(hint, appendPath))
              .collect(Collectors.toList());
      final List<RelHint> filteredHints = hintStrategies.apply(hints, original);
      if (!filteredHints.isEmpty()) {
        return ((Hintable) original).attachHints(filteredHints);
      }
      return original;
    }

    private static RelHint copyWithAppendPath(RelHint hint,
                                              List<Integer> appendPaths) {
      if (appendPaths.isEmpty()) {
        return hint;
      } else {
        List<Integer> newPath = new ArrayList<>(hint.inheritPath);
        newPath.addAll(appendPaths);
        return hint.copy(newPath);
      }
    }
  }
}
