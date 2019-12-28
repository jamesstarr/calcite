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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.mutable.Holder;
import org.apache.calcite.rel.mutable.MutableAggregate;
import org.apache.calcite.rel.mutable.MutableCalc;
import org.apache.calcite.rel.mutable.MutableFilter;
import org.apache.calcite.rel.mutable.MutableIntersect;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.mutable.MutableProject;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.mutable.MutableRelVisitor;
import org.apache.calcite.rel.mutable.MutableRels;
import org.apache.calcite.rel.mutable.MutableScan;
>>>>>>> 2e6c42a57 ([CALCITE-3334]: Refinement for Substitution-Based MV Matching (#12))
import org.apache.calcite.rel.mutable.MutableUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;

  /**
   * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalFilter}
   * on top of a {@link org.apache.calcite.rel.logical.LogicalProject} into a
   * trivial filter (on a boolean column).
   */
  public static class FilterOnProjectRule extends RelOptRule {
    public static final FilterOnProjectRule INSTANCE =
        new FilterOnProjectRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a FilterOnProjectRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public FilterOnProjectRule(RelBuilderFactory relBuilderFactory) {
      super(
          operandJ(LogicalFilter.class, null,
              filter -> filter.getCondition() instanceof RexInputRef,
              some(operand(LogicalProject.class, any()))),
          relBuilderFactory, null);
    }

    public void onMatch(RelOptRuleCall call) {
      final LogicalFilter filter = call.rel(0);
      final LogicalProject project = call.rel(1);

      final List<RexNode> newProjects = new ArrayList<>(project.getProjects());
      newProjects.add(filter.getCondition());

      final RelOptCluster cluster = filter.getCluster();
      RelDataType newRowType =
          cluster.getTypeFactory().builder()
              .addAll(project.getRowType().getFieldList())
              .add("condition", Util.last(newProjects).getType())
              .build();
      final RelNode newProject =
          project.copy(project.getTraitSet(),
              project.getInput(),
              newProjects,
              newRowType);

      final RexInputRef newCondition =
          cluster.getRexBuilder().makeInputRef(newProject,
              newProjects.size() - 1);

      call.transformTo(LogicalFilter.create(newProject, newCondition));
    }
  }
=======
=======
}
