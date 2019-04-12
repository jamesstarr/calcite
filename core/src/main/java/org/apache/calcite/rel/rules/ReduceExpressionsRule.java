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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.SubstitutionRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 *
 * @param <C> Configuration type
 */
public abstract class ReduceExpressionsRule<C extends ReduceExpressionsRule.Config>
        extends RelRule<C>
        implements SubstitutionRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Default reduction options used for project and calc.
   *
   * <ul>
   * <li>Match nullability of expressions: true
   * <li>Treat unknown values as false: false
   * <li>Treat dynamic calls as non constant: true
   * </ul>
   */
  public static final ReductionOptions DEFAULT_OPTIONS =
      new ReductionOptions(true, false, true);

  /**
   * Default reduction options used for filter and join.
   *
   * <ul>
   * <li>Match nullability of expressions: true
   * <li>Treat unknown values as false: true
   * <li>Treat dynamic calls as non constant: true
   * </ul>
   */
  public static final ReductionOptions DEFAULT_FILTER_OPTIONS =
      new ReductionOptions(true, true, true);

  /**
   * Regular expression that matches the description of all instances of this
   * rule and {@link ValuesReduceRule} also. Use
   * it to prevent the planner from invoking these rules.
   */
  public static final Pattern EXCLUSION_PATTERN =
      Pattern.compile("Reduce(Expressions|Values)Rule.*");

  /**
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(filterClass, DEFAULT_FILTER_OPTIONS.matchNullability(matchNullability),
          relBuilderFactory);
    }

    public FilterReduceExpressionsRule(Class<? extends Filter> filterClass,
        ReductionOptions options, RelBuilderFactory relBuilderFactory) {
      super(filterClass, options, relBuilderFactory,
          "ReduceExpressionsRule(Filter)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final List<RexNode> expList =
          Lists.newArrayList(filter.getCondition());
      RexNode newConditionExp;
      boolean reduced;
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(filter.getInput());
      if (reduceExpressions(filter, expList, predicates, options)) {
        assert expList.size() == 1;
        newConditionExp = expList.get(0);
        reduced = true;
      } else {
        // No reduction, but let's still test the original
        // predicate to see if it was already a constant,
        // in which case we don't need any runtime decision
        // about filtering.
        newConditionExp = filter.getCondition();
        reduced = false;
      }

      // Even if no reduction, let's still test the original
      // predicate to see if it was already a constant,
      // in which case we don't need any runtime decision
      // about filtering.
      if (newConditionExp.isAlwaysTrue()) {
        call.transformTo(
            filter.getInput());
      } else if (newConditionExp instanceof RexLiteral
          || RexUtil.isNullLiteral(newConditionExp, true)) {
        call.transformTo(createEmptyRelOrEquivalent(call, filter));
      } else if (reduced) {
        call.transformTo(call.builder()
            .push(filter.getInput())
            .filter(newConditionExp).build());
      } else {
        if (newConditionExp instanceof RexCall) {
          boolean reverse = newConditionExp.getKind() == SqlKind.NOT;
          if (reverse) {
            newConditionExp = ((RexCall) newConditionExp).getOperands().get(0);
          }
          reduceNotNullableFilter(call, filter, newConditionExp, reverse);
        }
        return;
      }

      // New plan is absolutely better than old plan.
      call.getPlanner().setImportance(filter, 0.0);
    }

    /**
     * For static schema systems, a filter that is always false or null can be
     * replaced by a values operator that produces no rows, as the schema
     * information can just be taken from the input Rel. In dynamic schema
     * environments, the filter might have an unknown input type, in these cases
     * they must define a system specific alternative to a Values operator, such
     * as inserting a limit 0 instead of a filter on top of the original input.
     *
     * <p>The default implementation of this method is to call
     * {@link RelBuilder#empty}, which for the static schema will be optimized
     * to an empty
     * {@link org.apache.calcite.rel.core.Values}.
     *
     * @param input rel to replace, assumes caller has already determined
     *              equivalence to Values operation for 0 records or a
     *              false filter.
     * @return equivalent but less expensive replacement rel
     */
    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Filter input) {
      return call.builder().push(input).empty().build();
    }

    private void reduceNotNullableFilter(
        RelOptRuleCall call,
        Filter filter,
        RexNode rexNode,
        boolean reverse) {
      // If the expression is a IS [NOT] NULL on a non-nullable
      // column, then we can either remove the filter or replace
      // it with an Empty.
      boolean alwaysTrue;
      switch (rexNode.getKind()) {
      case IS_NULL:
      case IS_UNKNOWN:
        alwaysTrue = false;
        break;
      case IS_NOT_NULL:
        alwaysTrue = true;
        break;
      default:
        return;
      }
      if (reverse) {
        alwaysTrue = !alwaysTrue;
      }
      RexNode operand = ((RexCall) rexNode).getOperands().get(0);
      if (operand instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) operand;
        if (!inputRef.getType().isNullable()) {
          if (alwaysTrue) {
            call.transformTo(filter.getInput());
          } else {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
          }
          // New plan is absolutely better than old plan.
          call.getPlanner().setImportance(filter, 0.0);
        }
      }
    }

    /** Rule configuration. */
    public interface Config extends ReduceExpressionsRule.Config {
      Config DEFAULT = EMPTY.as(Config.class)
              .withMatchNullability(true)
              .withOperandFor(LogicalFilter.class)
              .withDescription("ReduceExpressionsRule(Filter)")
              .as(Config.class);

      @Override default FilterReduceExpressionsRule toRule() {
        return new FilterReduceExpressionsRule(this);
      }
    }
  }

  /** Rule that reduces constants inside a
   * {@link org.apache.calcite.rel.core.Project}.
   *
   * @see CoreRules#PROJECT_REDUCE_EXPRESSIONS */
  public static class ProjectReduceExpressionsRule
          extends ReduceExpressionsRule<ProjectReduceExpressionsRule.Config> {
    /** Creates a ProjectReduceExpressionsRule. */
    protected ProjectReduceExpressionsRule(Config config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public ProjectReduceExpressionsRule(Class<? extends Project> projectClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(projectClass,
          DEFAULT_OPTIONS.matchNullability(matchNullability).unknownAsFalse(false),
          relBuilderFactory);
    }

    public ProjectReduceExpressionsRule(
        Class<? extends Project> projectClass,
        ReductionOptions options,
        RelBuilderFactory relBuilderFactory) {
      super(projectClass, options, relBuilderFactory,
          "ReduceExpressionsRule(Project)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(project.getInput());
      final List<RexNode> expList =
          Lists.newArrayList(project.getProjects());
      if (reduceExpressions(project, expList, predicates, options)) {
        call.transformTo(
            call.builder()
                .push(project.getInput())
                .project(expList, project.getRowType().getFieldNames())
                .build());

        // New plan is absolutely better than old plan.
        call.getPlanner().setImportance(project, 0.0);
      }
    }

    /** Rule configuration. */
    public interface Config extends ReduceExpressionsRule.Config {
      Config DEFAULT = EMPTY.as(Config.class)
              .withMatchNullability(true)
              .withOperandFor(LogicalProject.class)
              .withDescription("ReduceExpressionsRule(Project)")
              .as(Config.class);

      @Override default ProjectReduceExpressionsRule toRule() {
        return new ProjectReduceExpressionsRule(this);
      }
    }
  }

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Join}.
   */
  public static class JoinReduceExpressionsRule extends ReduceExpressionsRule {
    @Deprecated // to be removed before 2.0
    public JoinReduceExpressionsRule(Class<? extends Join> joinClass,
        RelBuilderFactory relBuilderFactory) {
      this(joinClass, DEFAULT_FILTER_OPTIONS, relBuilderFactory);
    }

    @Deprecated // to be removed before 2.0
    public JoinReduceExpressionsRule(Class<? extends Join> joinClass,
        boolean matchNullability, RelBuilderFactory relBuilderFactory) {
      this(joinClass,
          DEFAULT_FILTER_OPTIONS.matchNullability(matchNullability),
          relBuilderFactory);
    }

    @Deprecated // to be removed before 2.0
    public JoinReduceExpressionsRule(Class<? extends Join> joinClass,
        ReductionOptions options, RelBuilderFactory relBuilderFactory) {
      super(joinClass, options, relBuilderFactory,
          "ReduceExpressionsRule(Join)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final List<RexNode> expList = Lists.newArrayList(join.getCondition());
      final int fieldCount = join.getLeft().getRowType().getFieldCount();
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList leftPredicates =
          mq.getPulledUpPredicates(join.getLeft());
      final RelOptPredicateList rightPredicates =
          mq.getPulledUpPredicates(join.getRight());
      final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      final RelOptPredicateList predicates =
          leftPredicates.union(rexBuilder,
              rightPredicates.shift(rexBuilder, fieldCount));
      if (!reduceExpressions(join, expList, predicates, options)) {
        return;
      }

  /**
   * An immutable object capturing the set of options to be used for reducing
   * expressions.
   */
  public static final class ReductionOptions {
    private final boolean matchNullability;
    private final boolean unknownAsFalse;
    private final boolean treatDynamicCallsAsNonConstant;

    private ReductionOptions(
        boolean matchNullability,
        boolean unknownAsFalse,
        boolean treatDynamicCallsAsNonConstant) {
      this.matchNullability = matchNullability;
      this.unknownAsFalse = unknownAsFalse;
      this.treatDynamicCallsAsNonConstant = treatDynamicCallsAsNonConstant;
    }

    /**
     * Update the options to control dynamic treatment.
     *
     * @param treatDynamicCallsAsNonConstant Whether to treat dynamic functions as
     *                                       non-constants (defaults to true)
     * @return A new set of ReductionOptions
     */
    public ReductionOptions treatDynamicCallsAsNonConstant(
        boolean treatDynamicCallsAsNonConstant) {
      return new ReductionOptions(
          this.matchNullability,
          this.unknownAsFalse,
          treatDynamicCallsAsNonConstant);
    }

    /**
     * <p> Update the options to control nullability matching.
     *
     * <p>The {@code matchNullability} flag comes into play when reducing a
     * expression whose type is nullable. Suppose we are reducing an expression
     * {@code CASE WHEN 'a' = 'a' THEN 1 ELSE NULL END}. Before reduction the
     * type is {@code INTEGER} (nullable), but after reduction the literal 1 has
     * type {@code INTEGER NOT NULL}.
     *
     * <p>In some situations it is more important to preserve types; in this
     * case you should use {@code matchNullability = true} (which used to be
     * the default behavior of this method), and it will cast the literal to
     * {@code INTEGER} (nullable).
     *
     * <p>In other situations, you would rather propagate the new stronger type,
     * because it may allow further optimizations later; pass
     * {@code matchNullability = false} and no cast will be added, but you may
     * need to adjust types elsewhere in the expression tree.
     *
     * @param matchNullability Whether Calcite should add a CAST to a literal
     *                         resulting from simplification and expression if the
     *                         expression had nullable type and the literal is
     *                         NOT NULL
     * @return A new set of ReductionOptions
     */
    public ReductionOptions matchNullability(
        boolean matchNullability) {
      return new ReductionOptions(
          matchNullability,
          this.unknownAsFalse,
          this.treatDynamicCallsAsNonConstant);
    }

    /**
     * Update the options to control behavior around UNKNOWN handling.
     *
     * @param unknownAsFalse Whether UNKNOWN will be treated as FALSE
     * @return A new set of ReductionOptions
     */
    public ReductionOptions unknownAsFalse(boolean unknownAsFalse) {
      return new ReductionOptions(
          this.matchNullability,
          unknownAsFalse,
          this.treatDynamicCallsAsNonConstant
          );
    }

    /**
     * Whether Calcite should add a CAST to a literal resulting from simplification and
     * expression if the expression had nullable type and the literal is NOT NULL
     *
     * @return True if reduction should cast for nullabilities.
     */
    public boolean isMatchNullability() {
      return matchNullability;
    }

    /**
     * Whether UNKNOWN will be treated as false. Typically used in the context of a filter
     * reduction
     *
     * @return If Unknown (null) values are treated as false.
     */
    public boolean isUnknownAsFalse() {
      return unknownAsFalse;
    }

    /**
     * Whether to treat dynamic calls as non-constant for purposes of reduction.
     *
     * @return True if dynamic calls should be treated as non-constant.
     */
    public boolean isTreatDynamicCallsAsNonConstant() {
      return treatDynamicCallsAsNonConstant;
    }

  }
}

  /**
   * An immutable object capturing the set of options to be used for reducing
   * expressions.
   */

  /**
   * <p> Update the options to control nullability matching.
   *
   * <p>The {@code matchNullability} flag comes into play when reducing a
   * expression whose type is nullable. Suppose we are reducing an expression
   * {@code CASE WHEN 'a' = 'a' THEN 1 ELSE NULL END}. Before reduction the
   * type is {@code INTEGER} (nullable), but after reduction the literal 1 has
   * type {@code INTEGER NOT NULL}.
   *
   * <p>In some situations it is more important to preserve types; in this
   * case you should use {@code matchNullability = true} (which used to be
   * the default behavior of this method), and it will cast the literal to
   * {@code INTEGER} (nullable).
   *
   * <p>In other situations, you would rather propagate the new stronger type,
   * because it may allow further optimizations later; pass
   * {@code matchNullability = false} and no cast will be added, but you may
   * need to adjust types elsewhere in the expression tree.
   *
   * @param matchNullability Whether Calcite should add a CAST to a literal
   *                         resulting from simplification and expression if the
   *                         expression had nullable type and the literal is
   *                         NOT NULL
   * @return A new set of ReductionOptions
   */
  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    @Override ReduceExpressionsRule<?> toRule();

    /** Whether to add a CAST when a nullable expression
     * reduces to a NOT NULL literal. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean matchNullability();

    /** Sets {@link #matchNullability()}. */
    Config withMatchNullability(boolean matchNullability);

    /** Whether to treat
     * {@link SqlOperator#isDynamicFunction() dynamic functions} as constants.
     *
     * <p>When false (the default), calls to dynamic functions (e.g.
     * {@code USER}) are not reduced. When true, calls to dynamic functions
     * are treated as a constant, and reduced. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean treatDynamicCallsAsConstant();

    /** Sets {@link #treatDynamicCallsAsConstant()}. */
    Config withTreatDynamicCallsAsConstant(boolean treatDynamicCallsAsConstant);

    /**
     * Whether UNKNOWN will be treated as false. Typically used in the context of a filter
     * reduction
     *
     * @return If Unknown (null) values are treated as false.
     */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isUnknownAsFalse();

    /** Sets {@link #isUnknownAsFalse()}. */
    Config withUnknownAsFalse(boolean unknownAsFalse);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends RelNode> relClass) {
      return withOperandSupplier(b -> b.operand(relClass).anyInputs())
              .as(Config.class);
    }
  }
}
