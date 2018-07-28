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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that merges a
 * {@link Filter} into a {@link MultiJoin},
 * creating a richer {@code MultiJoin}.
 *
 * @see org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule
 * @see CoreRules#FILTER_MULTI_JOIN_MERGE
 */
public class FilterMultiJoinMergeRule extends RelOptRule {
  public static final FilterMultiJoinMergeRule INSTANCE =
      new FilterMultiJoinMergeRule(LogicalFilter.class, RelFactories.LOGICAL_BUILDER);

  /** Creates a FilterMultiJoinMergeRule. */
  protected FilterMultiJoinMergeRule(Config config) {
    super(config);
  }

  /**
   * Creates a FilterMultiJoinMergeRule.
   */
  public FilterMultiJoinMergeRule(Class<? extends Filter> filterClass,
      RelBuilderFactory relBuilderFactory) {
    super(
        operand(filterClass,
            operand(MultiJoin.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    MultiJoin multiJoin = call.rel(1);

    // Create a new post-join filter condition
    List<RexNode> filters = new ArrayList<>();
    filters.add(filter.getCondition());
    filters.add(multiJoin.getPostJoinFilter());

    final RexBuilder rexBuilder = multiJoin.getCluster().getRexBuilder();
    MultiJoin newMultiJoin =
        new MultiJoin(
            multiJoin.getCluster(),
            multiJoin.getInputs(),
            multiJoin.getJoinFilter(),
            multiJoin.getRowType(),
            multiJoin.isFullOuterJoin(),
            multiJoin.getOuterJoinConditions(),
            multiJoin.getJoinTypes(),
            multiJoin.getProjFields(),
            multiJoin.getJoinFieldRefCountsMap(),
            RexUtil.composeConjunction(rexBuilder, filters, true));

    call.transformTo(newMultiJoin);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(Filter.class, MultiJoin.class);

    @Override default FilterMultiJoinMergeRule toRule() {
      return new FilterMultiJoinMergeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
                                  Class<? extends MultiJoin> multiJoinClass) {
      return withOperandSupplier(b0 ->
              b0.operand(filterClass).oneInput(b1 ->
                      b1.operand(multiJoinClass).anyInputs()))
              .as(Config.class);
    }
  }
}
