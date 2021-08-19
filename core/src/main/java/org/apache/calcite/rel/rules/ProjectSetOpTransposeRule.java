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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalProject}
 * past a {@link org.apache.calcite.rel.core.SetOp}.
 *
 * <p>The children of the {@code SetOp} will project
 * only the {@link RexInputRef}s referenced in the original
 * {@code LogicalProject}.
 *
 * @see CoreRules#PROJECT_SET_OP_TRANSPOSE
 */
public class ProjectSetOpTransposeRule
        extends RelRule<ProjectSetOpTransposeRule.Config>
        implements TransformationRule {

  /** Creates a ProjectSetOpTransposeRule. */
  protected ProjectSetOpTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectSetOpTransposeRule(
          PushProjector.ExprCondition preserveExprCondition,
          RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .as(Config.class)
            .withPreserveExprCondition(preserveExprCondition));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  @Override public void onMatch(RelOptRuleCall call) {
    LogicalProject origProj = call.rel(0);
    SetOp setOp = call.rel(1);

    // cannot push project past a distinct
    if (!setOp.all) {
      return;
    }

    // locate all fields referenced in the projection
    PushProjector pushProject =
        new PushProjector(
            origProj, null, setOp, config.preserveExprCondition(), call.builder());
    pushProject.locateAllRefs();

    List<RelNode> newSetOpInputs = new ArrayList<>();
    int[] adjustments = pushProject.getAdjustments();

    // push the projects completely below the setop; this
    // is different from pushing below a join, where we decompose
    // to try to keep expensive expressions above the join,
    // because UNION ALL does not have any filtering effect,
    // and it is the only operator this rule currently acts on
    for (RelNode input : setOp.getInputs()) {
      // be lazy:  produce two ProjectRels, and let another rule
      // merge them (could probably just clone origProj instead?)
      Project p = pushProject.createProjectRefsAndExprs(input, true, false);
      newSetOpInputs.add(pushProject.createNewProject(p, adjustments));
    }

    // create a new setop whose children are the ProjectRels created above
    SetOp newSetOp =
        setOp.copy(setOp.getTraitSet(), newSetOpInputs);

    call.transformTo(newSetOp);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalProject.class).oneInput(b1 ->
                            b1.operand(SetOp.class).anyInputs()))
            .as(Config.class)
            .withPreserveExprCondition(PushProjector.ExprCondition.FALSE);

    @Override default ProjectSetOpTransposeRule toRule() {
      return new ProjectSetOpTransposeRule(this);
    }

    /** Defines when an expression should not be pushed. */
    @ImmutableBeans.Property
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);
  }
}

// End ProjectSetOpTransposeRule.java
