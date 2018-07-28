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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
  // override JoinProjectTransposeRule
  protected Project getRightChild(RelOptRuleCall call) {
    if (call.rels.length == 4) {
      return call.rel(2);
    } else {
      return call.rel(3);
    }
  }

  // override JoinProjectTransposeRule
  @Override protected RelNode getProjectChild(
      RelOptRuleCall call,
      Project project,
      boolean leftChild) {
    // locate the appropriate MultiJoin based on which rule was fired
    // and which projection we're dealing with
    MultiJoin multiJoin;
    if (leftChild) {
      multiJoin = call.rel(2);
    } else if (call.rels.length == 4) {
      multiJoin = call.rel(3);
    } else {
      multiJoin = call.rel(4);
    }

    // create a new MultiJoin that reflects the columns in the projection
    // above the MultiJoin
    return RelOptUtil.projectMultiJoin(multiJoin, project);
  }

  /** Rule configuration. */
  public interface Config extends JoinProjectTransposeRule.Config {
    Config BOTH_PROJECT = EMPTY
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalJoin.class).inputs(
                        b1 -> b1.operand(LogicalProject.class).oneInput(b2 ->
                                    b2.operand(MultiJoin.class).anyInputs()),
                        b3 -> b3.operand(LogicalProject.class).oneInput(b4 ->
                                    b4.operand(MultiJoin.class).anyInputs())))
            .withDescription(
                    "MultiJoinProjectTransposeRule:TwoLogicalProjects")
            .as(Config.class);

    Config LEFT_PROJECT = EMPTY
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalJoin.class).inputs(b1 ->
                        b1.operand(LogicalProject.class).oneInput(b2 ->
                            b2.operand(MultiJoin.class).anyInputs())))
            .withDescription(
                    "MultiJoinProjectTransposeRule:LeftLogicalProject")
            .as(Config.class);

    Config RIGHT_PROJECT = EMPTY
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalJoin.class).inputs(
                        b1 -> b1.operand(RelNode.class).anyInputs(),
                        b2 -> b2.operand(LogicalProject.class).oneInput(b3 ->
                                    b3.operand(MultiJoin.class).anyInputs())))
            .withDescription(
                    "MultiJoinProjectTransposeRule:RightLogicalProject")
            .as(Config.class);

    @Override default MultiJoinProjectTransposeRule toRule() {
      return new MultiJoinProjectTransposeRule(this);
    }
  }
}
