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
package org.apache.calcite.sql2rel.decorrelator;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * Rule to remove single_value rel. For cases like
 *
 * AGG(groups=[] aggCallList=[single_value($0)])
 *   PROJECT($0="...")
 *     AGG(groups=[] ...)
 * <blockquote>AggRel single_value proj/filter/agg/ join on unique LHS key
 * AggRel single group</blockquote>
 */
public final class RemoveSingleAggregateRule
    extends RelRule<RemoveSingleAggregateRule.Config> {
  static Config config(RelBuilderFactory f) {
    return Config.EMPTY.withRelBuilderFactory(f)
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class).oneInput(b1 ->
                b1.operand(Project.class).oneInput(b2 ->
                    b2.operand(Aggregate.class).anyInputs())))
        .as(Config.class);
  }

  /**
   * Creates a RemoveSingleAggregateRule.
   */
  RemoveSingleAggregateRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Aggregate singleAggregate = call.rel(0);
    Project project = call.rel(1);
    Aggregate aggregate = call.rel(2);

    // check singleAggRel is single_value agg
    if (!singleAggregate.getGroupSet().isEmpty()
        || (singleAggregate.getAggCallList().size() != 1)
        || !(singleAggregate.getAggCallList().get(0).getAggregation()
        instanceof SqlSingleValueAggFunction)) {
      return;
    }

    // check projRel only projects one expression
    // check this project only projects one expression, i.e. scalar
    // sub-queries.
    List<RexNode> projExprs = project.getProjects();
    if (projExprs.size() != 1) {
      return;
    }

    // check the input to project is an aggregate on the entire input
    if (!aggregate.getGroupSet().isEmpty()) {
      return;
    }

    // singleAggRel produces a nullable type, so create the new
    // projection that casts proj expr to a nullable type.
    final RelBuilder relBuilder = call.builder();
    final RelDataType type =
        relBuilder.getTypeFactory()
            .createTypeWithNullability(projExprs.get(0).getType(), true);
    final RexNode cast =
        relBuilder.getRexBuilder().makeCast(type, projExprs.get(0));
    relBuilder.push(aggregate)
        .project(cast);
    call.transformTo(relBuilder.build());
  }

  /**
   * Rule configuration.
   */
  public interface Config extends RelRule.Config {
    @Override default RemoveSingleAggregateRule toRule() {
      return new RemoveSingleAggregateRule(this);
    }
  }
}
