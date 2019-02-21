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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.Util;

import java.util.function.Predicate;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Project}.
 *
 * @see CoreRules#FILTER_PROJECT_TRANSPOSE
 */
public class FilterProjectTransposeRule
        extends RelRule<FilterProjectTransposeRule.Config>
        implements TransformationRule {

  /** Creates a FilterProjectTransposeRule. */
  protected FilterProjectTransposeRule(Config config) {
    super(config);
  }

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>Equivalent to the rule created by
   * {@link #FilterProjectTransposeRule(Class, Predicate, Class, Predicate, boolean, boolean, RelBuilderFactory)}
   * with some default predicates that do not allow a filter to be pushed
   * past the project if there is a correlation condition anywhere in the
   * filter (since in some cases it can prevent a
   * {@link org.apache.calcite.rel.core.Correlate} from being de-correlated).
   */
  @Deprecated // to be removed before 2.0
  public FilterProjectTransposeRule(
          Class<? extends Filter> filterClass,
          Class<? extends Project> projectClass,
          boolean copyFilter, boolean copyProject,
          RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .as(Config.class)
            .withOperandFor(filterClass,
              f -> !RexUtil.containsCorrelation(f.getCondition()),
                 projectClass, project -> true)
            .withCopyFilter(copyFilter)
            .withCopyProject(copyProject));
  }

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>If {@code copyFilter} is true, creates the same kind of Filter as
   * matched in the rule, otherwise it creates a Filter using the RelBuilder
   * obtained by the {@code relBuilderFactory}.
   * Similarly for {@code copyProject}.
   *
   * <p>Defining predicates for the Filter (using {@code filterPredicate})
   * and/or the Project (using {@code projectPredicate} allows making the rule
   * more restrictive.
   */
  @Deprecated // to be removed before 2.0
  public <F extends Filter, P extends Project> FilterProjectTransposeRule(
          Class<F> filterClass,
          Predicate<? super F> filterPredicate,
          Class<P> projectClass,
          Predicate<? super P> projectPredicate,
          boolean copyFilter, boolean copyProject,
          RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .withOperandSupplier(b0 ->
                    b0.operand(filterClass).predicate(filterPredicate)
                            .oneInput(b1 ->
                                    b1.operand(projectClass).predicate(projectPredicate)
                                            .anyInputs()))
            .as(Config.class)
            .withCopyFilter(copyFilter)
            .withCopyProject(copyProject));
  }

  @Deprecated // to be removed before 2.0
  public FilterProjectTransposeRule(
          Class<? extends Filter> filterClass,
          RelFactories.FilterFactory filterFactory,
          Class<? extends Project> projectClass,
          RelFactories.ProjectFactory projectFactory) {
    this(Config.DEFAULT
            .withRelBuilderFactory(RelBuilder.proto(filterFactory, projectFactory))
            .withOperandSupplier(b0 ->
                    b0.operand(filterClass)
                            .predicate(filter ->
                                    !RexUtil.containsCorrelation(filter.getCondition()))
                            .oneInput(b2 ->
                                    b2.operand(projectClass)
                                            .predicate(project -> true)
                                            .anyInputs()))
            .as(Config.class)
            .withCopyFilter(filterFactory == null)
            .withCopyProject(projectFactory == null));
  }

  @Deprecated // to be removed before 2.0
  protected FilterProjectTransposeRule(
          RelOptRuleOperand operand,
          boolean copyFilter,
          boolean copyProject,
          RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .withOperandSupplier(b -> b.exactly(operand))
            .as(Config.class)
            .withCopyFilter(copyFilter)
            .withCopyProject(copyProject));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project project = call.rel(1);

    if (RexOver.containsOver(project.getProjects(), null)) {
      // In general a filter cannot be pushed below a windowing calculation.
      // Applying the filter before the aggregation function changes
      // the results of the windowing invocation.
      //
      // When the filter is on the PARTITION BY expression of the OVER clause
      // it can be pushed down. For now we don't support this.
      return;
    }
    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushPastProject(filter.getCondition(), project);

    final RelBuilder relBuilder = call.builder();
    RelNode newFilterRel;
    if (config.isCopyFilter()) {
      newFilterRel = filter.copy(filter.getTraitSet(), project.getInput(),
          simplifyFilterCondition(newCondition, call));
    } else {
      newFilterRel =
          relBuilder.push(project.getInput()).filter(newCondition).build();
    }

    RelNode newProjRel =
        config.isCopyProject()
            ? project.copy(project.getTraitSet(), newFilterRel,
                project.getProjects(), project.getRowType())
            : relBuilder.push(newFilterRel)
                .project(project.getProjects(), project.getRowType().getFieldNames())
                .build();

    call.transformTo(newProjRel);
  }

  /**
   * Simplifies the filter condition using a simplifier created by the information in the
   * current call.
   *
   * The method is an attempt to replicate the simplification behavior of
   * {@link RelBuilder#filter(RexNode...)} which cannot be used in the case of copying nodes. The
   * main difference with the behavior of that method is that it does not drop entirely the filter
   * if the condition is always false.
   */
  private RexNode simplifyFilterCondition(RexNode condition, RelOptRuleCall call) {
    final RexBuilder xBuilder = call.builder().getRexBuilder();
    final RexExecutor executor =
        Util.first(call.getPlanner().getContext().unwrap(RexExecutor.class),
            Util.first(call.getPlanner().getExecutor(), RexUtil.EXECUTOR));
    // unknownAsFalse => true since in the WHERE clause:
    // 1>null evaluates to unknown and WHERE unknown behaves exactly like WHERE false
    RexSimplify simplifier =
        new RexSimplify(xBuilder, RelOptPredicateList.EMPTY, executor);
    return RexUtil.removeNullabilityCast(
        xBuilder.getTypeFactory(), simplifier.simplifyUnknownAsFalse(condition));
  }

  /** Rule configuration.
   *
   * <p>If {@code copyFilter} is true, creates the same kind of Filter as
   * matched in the rule, otherwise it creates a Filter using the RelBuilder
   * obtained by the {@code relBuilderFactory}.
   * Similarly for {@code copyProject}.
   *
   * <p>Defining predicates for the Filter (using {@code filterPredicate})
   * and/or the Project (using {@code projectPredicate} allows making the rule
   * more restrictive. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(Filter.class,
              f -> !RexUtil.containsCorrelation(f.getCondition()),
                    Project.class, p -> true)
            .withCopyFilter(true)
            .withCopyProject(true);

    @Override default FilterProjectTransposeRule toRule() {
      return new FilterProjectTransposeRule(this);
    }

    /** Whether to create a {@link Filter} of the same convention as the
     * matched Filter. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean isCopyFilter();

    /** Sets {@link #isCopyFilter()}. */
    Config withCopyFilter(boolean copyFilter);

    /** Whether to create a {@link Project} of the same convention as the
     * matched Project. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean isCopyProject();

    /** Sets {@link #isCopyProject()}. */
    Config withCopyProject(boolean copyProject);

    /** Defines an operand tree for the given 2 classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
                                  Predicate<Filter> filterPredicate,
                                  Class<? extends Project> projectClass,
                                  Predicate<Project> projectPredicate) {
      return withOperandSupplier(b0 ->
              b0.operand(filterClass).predicate(filterPredicate).oneInput(b1 ->
                      b1.operand(projectClass).predicate(projectPredicate).anyInputs()))
              .as(Config.class);
    }

    /** Defines an operand tree for the given 3 classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
                                  Class<? extends Project> projectClass,
                                  Class<? extends RelNode> relClass) {
      return withOperandSupplier(b0 ->
              b0.operand(filterClass).oneInput(b1 ->
                      b1.operand(projectClass).oneInput(b2 ->
                              b2.operand(relClass).anyInputs())))
              .as(Config.class);
    }
  }
}

// End FilterProjectTransposeRule.java
