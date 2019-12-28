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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;


/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Filter}.
 *
 * @see CoreRules#PROJECT_FILTER_TRANSPOSE
 * @see CoreRules#PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS
 * @see CoreRules#PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS
 */
public class ProjectFilterTransposeRule
        extends RelRule<ProjectFilterTransposeRule.Config>
        implements TransformationRule {

  /** Creates a ProjectFilterTransposeRule. */
  protected ProjectFilterTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectFilterTransposeRule(
          Class<? extends Project> projectClass,
          Class<? extends Filter> filterClass,
          RelBuilderFactory relBuilderFactory,
          PushProjector.ExprCondition preserveExprCondition) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .as(Config.class)
            .withOperandFor(projectClass, filterClass)
            .withPreserveExprCondition(preserveExprCondition));
  }

  @Deprecated // to be removed before 2.0
  protected ProjectFilterTransposeRule(RelOptRuleOperand operand,
                   PushProjector.ExprCondition preserveExprCondition, boolean wholeProject,
                   boolean wholeFilter, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
            .withOperandSupplier(b -> b.exactly(operand))
            .withRelBuilderFactory(relBuilderFactory)
            .as(Config.class)
            .withPreserveExprCondition(preserveExprCondition)
            .withWholeProject(wholeProject)
            .withWholeFilter(wholeFilter));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    Project origProj;
    Filter filter;
    if (call.rels.length >= 2) {
      origProj = call.rel(0);
      filter = call.rel(1);
    } else {
      origProj = null;
      filter = call.rel(0);
    }
    RelNode rel = filter.getInput();
    RexNode origFilter = filter.getCondition();

    if ((origProj != null)
        && RexOver.containsOver(origProj.getProjects(), null)) {
      // Cannot push project through filter if project contains a windowed
      // aggregate -- it will affect row counts. Abort this rule
      // invocation; pushdown will be considered after the windowed
      // aggregate has been implemented. It's OK if the filter contains a
      // windowed aggregate.
      return;
    }

    if ((origProj != null)
        && origProj.getRowType().isStruct()
        && origProj.getRowType().getFieldList().stream()
          .anyMatch(RelDataTypeField::isDynamicStar)) {
      // The PushProjector would change the plan:
      //
      //    prj(**=[$0])
      //    : - filter
      //        : - scan
      //
      // to form like:
      //
      //    prj(**=[$0])                    (1)
      //    : - filter                      (2)
      //        : - prj(**=[$0], ITEM= ...) (3)
      //            :  - scan
      // This new plan has more cost that the old one, because of the new
      // redundant project (3), if we also have FilterProjectTransposeRule in
      // the rule set, it will also trigger infinite match of the ProjectMergeRule
      // for project (1) and (3).
      return;
    }

    PushProjector pushProjector =
        new PushProjector(
            origProj, origFilter, rel, config.preserveExprCondition(), call.builder());
    RelNode topProject = pushProjector.convertProject(null);

    if (topProject != null) {
      call.transformTo(topProject);
    }
  }
  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(LogicalProject.class, LogicalFilter.class)
            .withPreserveExprCondition(PushProjector.ExprCondition.FALSE)
            .withWholeProject(false)
            .withWholeFilter(false);
<<<<<<< HEAD
=======

    Config PROJECT = DEFAULT.withWholeProject(true);

    Config PROJECT_FILTER = PROJECT.withWholeFilter(true);

    @Override default ProjectFilterTransposeRule toRule() {
      return new ProjectFilterTransposeRule(this);
    }

    /** Expressions that should be preserved in the projection. */
    @ImmutableBeans.Property
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);

    /** Whether to push whole expressions from the project;
     * if false (the default), only pushes references. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isWholeProject();

    /** Sets {@link #isWholeProject()}. */
    Config withWholeProject(boolean wholeProject);

    /** Whether to push whole expressions from the filter;
     * if false (the default), only pushes references. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isWholeFilter();

    /** Sets {@link #isWholeFilter()}. */
    Config withWholeFilter(boolean wholeFilter);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
                                  Class<? extends Filter> filterClass) {
      return withOperandSupplier(b0 ->
              b0.operand(projectClass).oneInput(b1 ->
                      b1.operand(filterClass).anyInputs()))
              .as(Config.class);
    }

    /** Defines an operand tree for the given 3 classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
                                  Class<? extends Filter> filterClass,
                                  Class<? extends RelNode> inputClass) {
      return withOperandSupplier(b0 ->
              b0.operand(projectClass).oneInput(b1 ->
                      b1.operand(filterClass).oneInput(b2 ->
                              b2.operand(inputClass).anyInputs())))
              .as(Config.class);
    }
  }
}
<<<<<<< HEAD
>>>>>>> 79e6848c7 (DX-34864 Add CoreRules and move static references of these rules to CoreRules)

    Config PROJECT = DEFAULT.withWholeProject(true);

    Config PROJECT_FILTER = PROJECT.withWholeFilter(true);

    @Override default ProjectFilterTransposeRule toRule() {
      return new ProjectFilterTransposeRule(this);
    }

    /** Expressions that should be preserved in the projection. */
    @ImmutableBeans.Property
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);

    /** Whether to push whole expressions from the project;
     * if false (the default), only pushes references. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isWholeProject();

    /** Sets {@link #isWholeProject()}. */
    Config withWholeProject(boolean wholeProject);

    /** Whether to push whole expressions from the filter;
     * if false (the default), only pushes references. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isWholeFilter();

    /** Sets {@link #isWholeFilter()}. */
    Config withWholeFilter(boolean wholeFilter);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
                                  Class<? extends Filter> filterClass) {
      return withOperandSupplier(b0 ->
              b0.operand(projectClass).oneInput(b1 ->
                      b1.operand(filterClass).anyInputs()))
              .as(Config.class);
    }

    /** Defines an operand tree for the given 3 classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
                                  Class<? extends Filter> filterClass,
                                  Class<? extends RelNode> inputClass) {
      return withOperandSupplier(b0 ->
              b0.operand(projectClass).oneInput(b1 ->
                      b1.operand(filterClass).oneInput(b2 ->
                              b2.operand(inputClass).anyInputs())))
              .as(Config.class);
    }
  }
}
=======
>>>>>>> 8900eaa6c ([CALCITE-3623] Run autostyleApply)
