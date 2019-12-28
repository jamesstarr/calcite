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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Join}
 * by splitting the projection into a projection on top of each child of
 * the join.
 *
 * @see CoreRules#PROJECT_JOIN_TRANSPOSE
 */
public class ProjectJoinTransposeRule
        extends RelRule<ProjectJoinTransposeRule.Config>
        implements TransformationRule {

  /** Creates a ProjectJoinTransposeRule. */
  protected ProjectJoinTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectJoinTransposeRule(
          Class<? extends Project> projectClass,
          Class<? extends Join> joinClass,
          PushProjector.ExprCondition preserveExprCondition,
          RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .as(Config.class)
            .withOperandFor(projectClass, joinClass)
            .withPreserveExprCondition(preserveExprCondition));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  @Override public void onMatch(final RelOptRuleCall call) {
    Project origProj = call.rel(0);
    final Join join = call.rel(1);

    if (join.isSemiJoin()) {
      return; // TODO: support SemiJoin
=======
    if (!join.getJoinType().projectsRight()) {
      return; // TODO: support SemiJoin / AntiJoin
    }

    // Normalize the join condition so we don't end up misidentified expanded
    // form of IS NOT DISTINCT FROM as PushProject also visit the filter condition
    // and push down expressions.
    RexNode joinFilter = join.getCondition().accept(new RexShuttle() {
      @Override public RexNode visitCall(RexCall rexCall) {
        final RexNode node = super.visitCall(rexCall);
        if (!(node instanceof RexCall)) {
          return node;
        }
        return RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node,
            call.builder().getRexBuilder());
      }
    });

    // locate all fields referenced in the projection and join condition;
    // determine which inputs are referenced in the projection and
    // join condition; if all fields are being referenced and there are no
    // special expressions, no point in proceeding any further
    PushProjector pushProject =
        new PushProjector(
            origProj,
            joinFilter,
            join,
            config.preserveExprCondition(),
            call.builder());
    if (pushProject.locateAllRefs()) {
      return;
    }

    // create left and right projections, projecting only those
    // fields referenced on each side
    RelNode leftProjRel =
        pushProject.createProjectRefsAndExprs(
            join.getLeft(),
            true,
            false);
    RelNode rightProjRel =
        pushProject.createProjectRefsAndExprs(
            join.getRight(),
            true,
            true);

    // convert the join condition to reference the projected columns
    RexNode newJoinFilter = null;
    int[] adjustments = pushProject.getAdjustments();
    if (joinFilter != null) {
      List<RelDataTypeField> projJoinFieldList = new ArrayList<>();
      projJoinFieldList.addAll(
          join.getSystemFieldList());
      projJoinFieldList.addAll(
          leftProjRel.getRowType().getFieldList());
      projJoinFieldList.addAll(
          rightProjRel.getRowType().getFieldList());
      newJoinFilter =
          pushProject.convertRefsAndExprs(
              joinFilter,
              projJoinFieldList,
              adjustments);
    }
    RelTraitSet traits = join.getTraitSet();
    final List<RelCollation> originCollations = traits.getTraits(RelCollationTraitDef.INSTANCE);

    if (originCollations != null && !originCollations.isEmpty()) {
      List<RelCollation> newCollations = new ArrayList<>();
      final int originLeftCnt = join.getLeft().getRowType().getFieldCount();
      final Mappings.TargetMapping leftMapping = RelOptUtil.permutationPushDownProject(
              ((Project) leftProjRel).getProjects(), join.getLeft().getRowType(),
              0, 0);
      final Mappings.TargetMapping rightMapping = RelOptUtil.permutationPushDownProject(
              ((Project) rightProjRel).getProjects(), join.getRight().getRowType(),
              originLeftCnt, leftProjRel.getRowType().getFieldCount());
      for (RelCollation collation: originCollations) {
        List<RelFieldCollation> fc = new ArrayList<>();
        final List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        for (RelFieldCollation relFieldCollation: fieldCollations) {
          final int fieldIndex = relFieldCollation.getFieldIndex();
          if (fieldIndex < originLeftCnt) {
            fc.add(RexUtil.apply(leftMapping, relFieldCollation));
          } else {
            fc.add(RexUtil.apply(rightMapping, relFieldCollation));
          }
        }
        newCollations.add(RelCollations.of(fc));
      }
      if (!newCollations.isEmpty()) {
        traits = traits.replace(newCollations);
      }
    }
    // create a new join with the projected children
    Join newJoinRel =
        join.copy(
            traits,
            newJoinFilter,
            leftProjRel,
            rightProjRel,
            join.getJoinType(),
            join.isSemiJoinDone());

    // put the original project on top of the join, converting it to
    // reference the modified projection list
    RelNode topProject =
        pushProject.createNewProject(newJoinRel, adjustments);

    call.transformTo(topProject);
  }
<<<<<<< HEAD
=======

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(LogicalProject.class, LogicalJoin.class)
            .withPreserveExprCondition(PushProjector.ExprCondition.TRUE);

    @Override default ProjectJoinTransposeRule toRule() {
      return new ProjectJoinTransposeRule(this);
    }

    /** Defines when an expression should not be pushed. */
    @ImmutableBeans.Property
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
                                  Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
              b0.operand(projectClass).oneInput(b1 ->
                      b1.operand(joinClass).anyInputs()))
              .as(Config.class);
    }
  }
}
<<<<<<< HEAD

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(LogicalProject.class, LogicalJoin.class)
            .withPreserveExprCondition(PushProjector.ExprCondition.TRUE);

    @Override default ProjectJoinTransposeRule toRule() {
      return new ProjectJoinTransposeRule(this);
    }

    /** Defines when an expression should not be pushed. */
    @ImmutableBeans.Property
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
                                  Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
              b0.operand(projectClass).oneInput(b1 ->
                      b1.operand(joinClass).anyInputs()))
              .as(Config.class);
    }
  }
}
=======
>>>>>>> 8900eaa6c ([CALCITE-3623] Run autostyleApply)
