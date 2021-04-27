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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql2rel.decorrelator.RelDecorrelator.SQL2REL_LOGGER;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that removes correlations for scalar projects.
 */
public final class RemoveCorrelationForScalarProjectRule
    extends RelRule<RemoveCorrelationForScalarProjectRule.Config> {
  final RelDecorrelator d;

  static Config config(RelDecorrelator decorrelator,
      RelBuilderFactory relBuilderFactory) {
    return Config.EMPTY
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b0 ->
            b0.operand(Correlate.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Aggregate.class).oneInput(b3 ->
                    b3.operand(Project.class).oneInput(b4 ->
                        b4.operand(RelNode.class).anyInputs()))))
        .as(Config.class)
        .withDecorrelator(decorrelator)
        .as(Config.class);
  }

  /**
   * Creates a RemoveCorrelationForScalarProjectRule.
   */
  RemoveCorrelationForScalarProjectRule(Config config) {
    super(config);
    this.d = requireNonNull(config.decorrelator());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    final RelNode left = call.rel(1);
    final Aggregate aggregate = call.rel(2);
    final Project project = call.rel(3);
    RelNode right = call.rel(4);
    final RelOptCluster cluster = correlate.getCluster();

    d.setCurrent(call.getPlanner().getRoot(), correlate);

    // Check for this pattern.
    // The pattern matching could be simplified if rules can be applied
    // during decorrelation.
    //
    // Correlate(left correlation, condition = true)
    //   leftInput
    //   Aggregate (groupby (0) single_value())
    //     Project-A (may reference corVar)
    //       rightInput
    final JoinRelType joinType = correlate.getJoinType();

    // corRel.getCondition was here, however Correlate was updated so it
    // never includes a join condition. The code was not modified for brevity.
    RexNode joinCond = d.relBuilder.literal(true);
    if ((joinType != JoinRelType.LEFT)
        || (joinCond != d.relBuilder.literal(true))) {
      return;
    }

    // check that the agg is of the following type:
    // doing a single_value() on the entire input
    if (!aggregate.getGroupSet().isEmpty()
        || (aggregate.getAggCallList().size() != 1)
        || !(aggregate.getAggCallList().get(0).getAggregation()
        instanceof SqlSingleValueAggFunction)) {
      return;
    }

    // check this project only projects one expression, i.e. scalar
    // sub-queries.
    if (project.getProjects().size() != 1) {
      return;
    }

    int nullIndicatorPos;

    if ((right instanceof Filter)
        && d.cm.mapRefRelToCorRef.containsKey(right)) {
      // rightInput has this shape:
      //
      //       Filter (references corVar)
      //         filterInput

      // If rightInput is a filter and contains correlated
      // reference, make sure the correlated keys in the filter
      // condition forms a unique key of the RHS.

      Filter filter = (Filter) right;
      right = filter.getInput();

      assert right instanceof HepRelVertex;
      right = ((HepRelVertex) right).getCurrentRel();

      // check filter input contains no correlation
      if (RelOptUtil.getVariablesUsed(right).size() > 0) {
        return;
      }

      // extract the correlation out of the filter

      // First breaking up the filter conditions into equality
      // comparisons between rightJoinKeys (from the original
      // filterInput) and correlatedJoinKeys. correlatedJoinKeys
      // can be expressions, while rightJoinKeys need to be input
      // refs. These comparisons are AND'ed together.
      List<RexNode> tmpRightJoinKeys = new ArrayList<>();
      List<RexNode> correlatedJoinKeys = new ArrayList<>();
      RelOptUtil.splitCorrelatedFilterCondition(
          filter,
          tmpRightJoinKeys,
          correlatedJoinKeys,
          false);

      // check that the columns referenced in these comparisons form
      // an unique key of the filterInput
      final List<RexInputRef> rightJoinKeys = new ArrayList<>();
      for (RexNode key : tmpRightJoinKeys) {
        assert key instanceof RexInputRef;
        rightJoinKeys.add((RexInputRef) key);
      }

      // check that the columns referenced in rightJoinKeys form an
      // unique key of the filterInput
      if (rightJoinKeys.isEmpty()) {
        return;
      }

      // The join filters out the nulls.  So, it's ok if there are
      // nulls in the join keys.
      final RelMetadataQuery mq = call.getMetadataQuery();
      if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, right,
          rightJoinKeys)) {
        SQL2REL_LOGGER.debug("{} are not unique keys for {}",
            rightJoinKeys, right);
        return;
      }

      RexUtil.FieldAccessFinder visitor =
          new RexUtil.FieldAccessFinder();
      RexUtil.apply(visitor, correlatedJoinKeys, null);
      List<RexFieldAccess> correlatedKeyList =
          visitor.getFieldAccessList();

      if (!d.checkCorVars(correlate, project, filter, correlatedKeyList)) {
        return;
      }

      // Change the plan to this structure.
      // Note that the Aggregate is removed.
      //
      // Project-A' (replace corVar to input ref from the Join)
      //   Join (replace corVar to input ref from leftInput)
      //     leftInput
      //     rightInput (previously filterInput)

      // Change the filter condition into a join condition
      joinCond =
          DecorrelatorUtil.removeCorrelationExpr(d, filter.getCondition(), false);

      nullIndicatorPos =
          left.getRowType().getFieldCount()
              + rightJoinKeys.get(0).getIndex();
    } else if (d.cm.mapRefRelToCorRef.containsKey(project)) {
      // check filter input contains no correlation
      if (RelOptUtil.getVariablesUsed(right).size() > 0) {
        return;
      }

      if (!d.checkCorVars(correlate, project, null, null)) {
        return;
      }

      // Change the plan to this structure.
      //
      // Project-A' (replace corVar to input ref from Join)
      //   Join (left, condition = true)
      //     leftInput
      //     Aggregate(groupby(0), single_value(0), s_v(1)....)
      //       Project-B (everything from input plus literal true)
      //         projectInput

      // make the new Project to provide a null indicator
      right =
          d.createProjectWithAdditionalExprs(right,
              ImmutableList.of(
                  Pair.of(d.relBuilder.literal(true), "nullIndicator")));

      // make the new aggRel
      right =
          RelOptUtil.createSingleValueAggRel(cluster, right);

      // The last field:
      //     single_value(true)
      // is the nullIndicator
      nullIndicatorPos =
          left.getRowType().getFieldCount()
              + right.getRowType().getFieldCount() - 1;
    } else {
      return;
    }

    // make the new join rel
    final Join join = (Join) d.relBuilder.push(left).push(right)
        .join(joinType, joinCond).build();

    RelNode newProject =
        d.projectJoinOutputWithNullability(join, project, nullIndicatorPos);

    call.transformTo(newProject);

    d.removeCorVarFromTree(correlate);
  }

  /**
   * Rule configuration.
   *
   * <p>Extends {@link RelDecorrelator.Config} because rule needs a
   * decorrelator instance.
   */
  public interface Config extends RelDecorrelator.Config {
    @Override default RemoveCorrelationForScalarProjectRule toRule() {
      return new RemoveCorrelationForScalarProjectRule(this);
    }
  }
}
