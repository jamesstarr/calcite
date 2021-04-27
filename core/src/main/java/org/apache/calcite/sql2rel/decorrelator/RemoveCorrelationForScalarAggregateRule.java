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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that removes correlations for scalar aggregates.
 */
public final class RemoveCorrelationForScalarAggregateRule
    extends RelRule<RemoveCorrelationForScalarAggregateRule.Config> {
  private final RelDecorrelator d;

  static Config config(RelDecorrelator decorrelator,
      RelBuilderFactory relBuilderFactory) {
    return Config.EMPTY
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b0 ->
            b0.operand(Correlate.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Project.class).oneInput(b3 ->
                    b3.operand(Aggregate.class).predicate(Aggregate::isSimple).oneInput(b4 ->
                        b4.operand(Project.class).oneInput(b5 ->
                            b5.operand(RelNode.class).anyInputs())))))
        .as(Config.class)
        .withDecorrelator(decorrelator)
        .as(Config.class);
  }

  /**
   * Creates a RemoveCorrelationForScalarAggregateRule.
   */
  RemoveCorrelationForScalarAggregateRule(Config config) {
    super(config);
    d = requireNonNull(config.decorrelator());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    final RelNode left = call.rel(1);
    final Project aggOutputProject = call.rel(2);
    final Aggregate aggregate = call.rel(3);
    final Project aggInputProject = call.rel(4);
    RelNode right = call.rel(5);
    final RelBuilder builder = call.builder();
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelOptCluster cluster = correlate.getCluster();

    d.setCurrent(call.getPlanner().getRoot(), correlate);

    // check for this pattern
    // The pattern matching could be simplified if rules can be applied
    // during decorrelation,
    //
    // CorrelateRel(left correlation, condition = true)
    //   leftInput
    //   Project-A (a RexNode)
    //     Aggregate (groupby (0), agg0(), agg1()...)
    //       Project-B (references coVar)
    //         rightInput

    // check aggOutputProject projects only one expression
    final List<RexNode> aggOutputProjects = aggOutputProject.getProjects();
    if (aggOutputProjects.size() != 1) {
      return;
    }

    final JoinRelType joinType = correlate.getJoinType();
    // corRel.getCondition was here, however Correlate was updated so it
    // never includes a join condition. The code was not modified for brevity.
    RexNode joinCond = rexBuilder.makeLiteral(true);
    if ((joinType != JoinRelType.LEFT)
        || (joinCond != rexBuilder.makeLiteral(true))) {
      return;
    }

    // check that the agg is on the entire input
    if (!aggregate.getGroupSet().isEmpty()) {
      return;
    }

    final List<RexNode> aggInputProjects = aggInputProject.getProjects();

    final List<AggregateCall> aggCalls = aggregate.getAggCallList();
    final Set<Integer> isCountStar = new HashSet<>();

    // mark if agg produces count(*) which needs to reference the
    // nullIndicator after the transformation.
    int k = -1;
    for (AggregateCall aggCall : aggCalls) {
      ++k;
      if ((aggCall.getAggregation() instanceof SqlCountAggFunction)
          && (aggCall.getArgList().size() == 0)) {
        isCountStar.add(k);
      }
    }

    if ((right instanceof Filter)
        && d.cm.mapRefRelToCorRef.containsKey(right)) {
      // rightInput has this shape:
      //
      //       Filter (references corVar)
      //         filterInput
      Filter filter = (Filter) right;
      right = filter.getInput();

      assert right instanceof HepRelVertex;
      right = ((HepRelVertex) right).getCurrentRel();

      // check filter input contains no correlation
      if (RelOptUtil.getVariablesUsed(right).size() > 0) {
        return;
      }

      // check filter condition type First extract the correlation out
      // of the filter

      // First breaking up the filter conditions into equality
      // comparisons between rightJoinKeys(from the original
      // filterInput) and correlatedJoinKeys. correlatedJoinKeys
      // can only be RexFieldAccess, while rightJoinKeys can be
      // expressions. These comparisons are AND'ed together.
      List<RexNode> rightJoinKeys = new ArrayList<>();
      List<RexNode> tmpCorrelatedJoinKeys = new ArrayList<>();
      RelOptUtil.splitCorrelatedFilterCondition(
          filter,
          rightJoinKeys,
          tmpCorrelatedJoinKeys,
          true);

      // make sure the correlated reference forms a unique key check
      // that the columns referenced in these comparisons form an
      // unique key of the leftInput
      List<RexFieldAccess> correlatedJoinKeys = new ArrayList<>();
      List<RexInputRef> correlatedInputRefJoinKeys = new ArrayList<>();
      for (RexNode joinKey : tmpCorrelatedJoinKeys) {
        assert joinKey instanceof RexFieldAccess;
        correlatedJoinKeys.add((RexFieldAccess) joinKey);
        RexNode correlatedInputRef =
            DecorrelatorUtil.removeCorrelationExpr(d, joinKey, false);
        assert correlatedInputRef instanceof RexInputRef;
        correlatedInputRefJoinKeys.add(
            (RexInputRef) correlatedInputRef);
      }

      // check that the columns referenced in rightJoinKeys form an
      // unique key of the filterInput
      if (correlatedInputRefJoinKeys.isEmpty()) {
        return;
      }

      // The join filters out the nulls.  So, it's ok if there are
      // nulls in the join keys.
      final RelMetadataQuery mq = call.getMetadataQuery();
      if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, left,
          correlatedInputRefJoinKeys)) {
        RelDecorrelator.SQL2REL_LOGGER.debug("{} are not unique keys for {}",
            correlatedJoinKeys, left);
        return;
      }

      // check corVar references are valid
      if (!d.checkCorVars(correlate, aggInputProject, filter,
          correlatedJoinKeys)) {
        return;
      }

      // Rewrite the above plan:
      //
      // Correlate(left correlation, condition = true)
      //   leftInput
      //   Project-A (a RexNode)
      //     Aggregate (groupby(0), agg0(),agg1()...)
      //       Project-B (may reference corVar)
      //         Filter (references corVar)
      //           rightInput (no correlated reference)
      //

      // to this plan:
      //
      // Project-A' (all gby keys + rewritten nullable ProjExpr)
      //   Aggregate (groupby(all left input refs)
      //                 agg0(rewritten expression),
      //                 agg1()...)
      //     Project-B' (rewritten original projected exprs)
      //       Join(replace corVar w/ input ref from leftInput)
      //         leftInput
      //         rightInput
      //

      // In the case where agg is count(*) or count($corVar), it is
      // changed to count(nullIndicator).
      // Note:  any non-nullable field from the RHS can be used as
      // the indicator however a "true" field is added to the
      // projection list from the RHS for simplicity to avoid
      // searching for non-null fields.
      //
      // Project-A' (all gby keys + rewritten nullable ProjExpr)
      //   Aggregate (groupby(all left input refs),
      //                 count(nullIndicator), other aggs...)
      //     Project-B' (all left input refs plus
      //                    the rewritten original projected exprs)
      //       Join(replace corVar to input ref from leftInput)
      //         leftInput
      //         Project (everything from rightInput plus
      //                     the nullIndicator "true")
      //           rightInput
      //

      // first change the filter condition into a join condition
      joinCond = DecorrelatorUtil.removeCorrelationExpr(d, filter.getCondition(), false);
    } else if (d.cm.mapRefRelToCorRef.containsKey(aggInputProject)) {
      // check rightInput contains no correlation
      if (RelOptUtil.getVariablesUsed(right).size() > 0) {
        return;
      }

      // check corVar references are valid
      if (!d.checkCorVars(correlate, aggInputProject, null, null)) {
        return;
      }

      int nFields = left.getRowType().getFieldCount();
      ImmutableBitSet allCols = ImmutableBitSet.range(nFields);

      // leftInput contains unique keys
      // i.e. each row is distinct and can group by on all the left
      // fields
      final RelMetadataQuery mq = call.getMetadataQuery();
      if (!RelMdUtil.areColumnsDefinitelyUnique(mq, left, allCols)) {
        RelDecorrelator.SQL2REL_LOGGER.debug("There are no unique keys for {}", left);
        return;
      }
      //
      // Rewrite the above plan:
      //
      // CorrelateRel(left correlation, condition = true)
      //   leftInput
      //   Project-A (a RexNode)
      //     Aggregate (groupby(0), agg0(), agg1()...)
      //       Project-B (references coVar)
      //         rightInput (no correlated reference)
      //

      // to this plan:
      //
      // Project-A' (all gby keys + rewritten nullable ProjExpr)
      //   Aggregate (groupby(all left input refs)
      //                 agg0(rewritten expression),
      //                 agg1()...)
      //     Project-B' (rewritten original projected exprs)
      //       Join (LOJ cond = true)
      //         leftInput
      //         rightInput
      //

      // In the case where agg is count($corVar), it is changed to
      // count(nullIndicator).
      // Note:  any non-nullable field from the RHS can be used as
      // the indicator however a "true" field is added to the
      // projection list from the RHS for simplicity to avoid
      // searching for non-null fields.
      //
      // Project-A' (all gby keys + rewritten nullable ProjExpr)
      //   Aggregate (groupby(all left input refs),
      //                 count(nullIndicator), other aggs...)
      //     Project-B' (all left input refs plus
      //                    the rewritten original projected exprs)
      //       Join (replace corVar to input ref from leftInput)
      //         leftInput
      //         Project (everything from rightInput plus
      //                     the nullIndicator "true")
      //           rightInput
    } else {
      return;
    }

    RelDataType leftInputFieldType = left.getRowType();
    int leftInputFieldCount = leftInputFieldType.getFieldCount();
    int joinOutputProjExprCount =
        leftInputFieldCount + aggInputProjects.size() + 1;

    right = d.createProjectWithAdditionalExprs(right,
        ImmutableList.of(
            Pair.of(rexBuilder.makeLiteral(true), "nullIndicator")));

    Join join = (Join) d.relBuilder.push(left).push(right)
        .join(joinType, joinCond).build();

    // To the consumer of joinOutputProjRel, nullIndicator is located
    // at the end
    int nullIndicatorPos = join.getRowType().getFieldCount() - 1;

    RexInputRef nullIndicator =
        new RexInputRef(
            nullIndicatorPos,
            cluster.getTypeFactory().createTypeWithNullability(
                join.getRowType().getFieldList()
                    .get(nullIndicatorPos).getType(),
                true));

    // first project all group-by keys plus the transformed agg input
    List<RexNode> joinOutputProjects = new ArrayList<>();

    // LOJ Join preserves LHS types
    for (int i = 0; i < leftInputFieldCount; i++) {
      joinOutputProjects.add(
          rexBuilder.makeInputRef(
              leftInputFieldType.getFieldList().get(i).getType(), i));
    }

    for (RexNode aggInputProjExpr : aggInputProjects) {
      joinOutputProjects.add(
          DecorrelatorUtil.removeCorrelationExpr(d, aggInputProjExpr,
              joinType.generatesNullsOnRight(),
              nullIndicator));
    }

    joinOutputProjects.add(
        rexBuilder.makeInputRef(join, nullIndicatorPos));

    final RelNode joinOutputProject = builder.push(join)
        .project(joinOutputProjects)
        .build();

    // nullIndicator is now at a different location in the output of
    // the join
    nullIndicatorPos = joinOutputProjExprCount - 1;

    final int groupCount = leftInputFieldCount;

    List<AggregateCall> newAggCalls = new ArrayList<>();
    k = -1;
    for (AggregateCall aggCall : aggCalls) {
      ++k;
      final List<Integer> argList;

      if (isCountStar.contains(k)) {
        // this is a count(*), transform it to count(nullIndicator)
        // the null indicator is located at the end
        argList = Collections.singletonList(nullIndicatorPos);
      } else {
        argList = new ArrayList<>();

        for (int aggArg : aggCall.getArgList()) {
          argList.add(aggArg + groupCount);
        }
      }

      int filterArg = aggCall.filterArg < 0 ? aggCall.filterArg
          : aggCall.filterArg + groupCount;
      newAggCalls.add(
          aggCall.adaptTo(joinOutputProject, argList, filterArg,
              aggregate.getGroupCount(), groupCount));
    }

    ImmutableBitSet groupSet =
        ImmutableBitSet.range(groupCount);
    builder.push(joinOutputProject)
        .aggregate(builder.groupKey(groupSet), newAggCalls);
    List<RexNode> newAggOutputProjectList = new ArrayList<>();
    for (int i : groupSet) {
      newAggOutputProjectList.add(
          rexBuilder.makeInputRef(builder.peek(), i));
    }

    RexNode newAggOutputProjects =
        DecorrelatorUtil.removeCorrelationExpr(d, aggOutputProjects.get(0), false);
    newAggOutputProjectList.add(
        rexBuilder.makeCast(
            cluster.getTypeFactory().createTypeWithNullability(
                newAggOutputProjects.getType(),
                true),
            newAggOutputProjects));

    builder.project(newAggOutputProjectList);
    call.transformTo(builder.build());

    d.removeCorVarFromTree(correlate);
  }

  /**
   * Rule configuration.
   *
   * <p>Extends {@link RelDecorrelator.Config} because rule needs a
   * decorrelator instance.
   */
  public interface Config extends RelDecorrelator.Config {
    @Override default RemoveCorrelationForScalarAggregateRule toRule() {
      return new RemoveCorrelationForScalarAggregateRule(this);
    }
  }
}
