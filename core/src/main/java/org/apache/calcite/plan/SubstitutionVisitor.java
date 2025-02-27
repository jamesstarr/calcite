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
  public static MutableRel unifyAggregates(MutableAggregate query,
      MutableAggregate target) {
    MutableRel result;
    if (query.groupSets.equals(target.groupSets)) {
      // Same level of aggregation. Generate a project.
      final List<Integer> projects = new ArrayList<>();
      final int groupCount = query.groupSet.cardinality();
      for (int i = 0; i < groupCount; i++) {
        projects.add(i);
      }
      for (AggregateCall aggregateCall : query.aggCalls) {
        int i = target.aggCalls.indexOf(aggregateCall);
        if (i < 0) {
          return null;
        }
        projects.add(groupCount + i);
      }
      result = MutableRels.createProject(target, projects);
    } else if (target.getGroupType() == Aggregate.Group.SIMPLE) {
      // Query is coarser level of aggregation. Generate an aggregate.
      final Map<Integer, Integer> map = new HashMap<>();
      target.groupSet.forEach(k -> map.put(k, map.size()));
      for (int c : query.groupSet) {
        if (!map.containsKey(c)) {
          return null;
        }
      }
      final ImmutableBitSet groupSet = query.groupSet.permute(map);
      ImmutableList<ImmutableBitSet> groupSets = null;
      if (query.getGroupType() != Aggregate.Group.SIMPLE) {
        groupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
            ImmutableBitSet.permute(query.groupSets, map));
      }
>>>>>>> df3383b8b ([CALCITE-2259] Allow Java 8 syntax)
      final List<AggregateCall> aggregateCalls = new ArrayList<>();
      for (AggregateCall aggregateCall : query.aggCalls) {
        if (aggregateCall.isDistinct()) {
          return null;
        }
        int i = target.aggCalls.indexOf(aggregateCall);
        if (i < 0) {
          return null;
        }
        aggregateCalls.add(
            AggregateCall.create(getRollup(aggregateCall.getAggregation()),
                aggregateCall.isDistinct(), aggregateCall.isApproximate(),
                aggregateCall.ignoreNulls(),
                ImmutableList.of(target.groupSet.cardinality() + i), -1,
                aggregateCall.collation, aggregateCall.type,
                aggregateCall.name));
      }
      result = MutableAggregate.of(target, groupSet, groupSets,
          aggregateCalls);
    } else {
      return null;
    }
    return MutableRels.createCastRel(result, query.rowType, true);
  }

  /** Implementation of {@link UnifyRule} that matches a
   * {@link MutableAggregate} on
   * a {@link MutableProject} query to an {@link MutableAggregate} target.
   *
   * <p>The rule is necessary when we unify query=Aggregate(x) with
   * target=Aggregate(x, y). Query will tend to have an extra Project(x) on its
   * input, which this rule knows is safe to ignore.</p> */
  public static class AggregateOnProjectToAggregateUnifyRule
      extends AbstractUnifyRule {
    public static final AggregateOnProjectToAggregateUnifyRule INSTANCE =
        new AggregateOnProjectToAggregateUnifyRule();

    private AggregateOnProjectToAggregateUnifyRule() {
      super(
          operand(MutableAggregate.class,
              operand(MutableProject.class, query(0))),
          operand(MutableAggregate.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableAggregate query = (MutableAggregate) call.query;
      final MutableAggregate target = (MutableAggregate) call.target;
      if (!(query.getInput() instanceof MutableProject)) {
        return null;
      }
      final MutableProject project = (MutableProject) query.getInput();
      if (project.getInput() != target.getInput()) {
        return null;
      }
      final Mappings.TargetMapping mapping = project.getMapping();
      if (mapping == null) {
        return null;
      }
      Mapping inverseMapping = mapping.inverse();
      final MutableAggregate aggregate2 =
          permute(query, project.getInput(), inverseMapping);
      final MutableRel unifiedAggregate = unifyAggregates(aggregate2, target);
      if (unifiedAggregate == null) {
        return null;
      }
      MutableRel result = unifiedAggregate;
      // Add Project if the mapping breaks order of fields in GroupSet
      if (!Mappings.keepsOrdering(mapping)) {
        final List<Integer> posList = new ArrayList<>();
        final int fieldCount = aggregate2.rowType.getFieldCount();
        for (int group: aggregate2.groupSet) {
          if (inverseMapping.getTargetOpt(group) != -1) {
            posList.add(inverseMapping.getTarget(group));
          }
        }
        for (int i = posList.size(); i < fieldCount; i++) {
          posList.add(i);
        }
        result = MutableRels.createProject(unifiedAggregate, posList);
      }
      return call.result(result);
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableScan} to a {@link MutableCalc}
   * which has {@link MutableScan} as child.
   */
  protected static class ScanToCalcUnifyRule extends AbstractUnifyRule {

    public static final ScanToCalcUnifyRule INSTANCE = new ScanToCalcUnifyRule();

    private ScanToCalcUnifyRule() {
      super(any(MutableScan.class),
          operand(MutableCalc.class, any(MutableScan.class)), 0);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {

      final MutableScan query = (MutableScan) call.query;

      final MutableCalc target = (MutableCalc) call.target;
      final MutableScan targetInput = (MutableScan) target.getInput();
      final Pair<RexNode, List<RexNode>> targetExplained = explainCalc(target);
      final RexNode targetCond = targetExplained.left;
      final List<RexNode> targetProjs = targetExplained.right;

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      if (!query.equals(targetInput) || !targetCond.isAlwaysTrue()) {
        return null;
      }
      final RexShuttle shuttle = getRexShuttle(targetProjs);
      final List<RexNode> compenProjs;
      try {
        compenProjs = (List<RexNode>) shuttle.apply(
            rexBuilder.identityProjects(query.rowType));
      } catch (MatchFailed e) {
        return null;
      }
      if (RexUtil.isIdentity(compenProjs, target.rowType)) {
        return call.result(target);
      } else {
        RexProgram compenRexProgram = RexProgram.create(
            target.rowType, compenProjs, null, query.rowType, rexBuilder);
        MutableCalc compenCalc = MutableCalc.of(target, compenRexProgram);
        return tryMergeParentCalcAndGenResult(call, compenCalc);
      }
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableCalc} to a {@link MutableCalc}.
   * The matching condition is as below:
   * 1. All columns of query can be expressed by target;
   * 2. The filtering condition of query must equals to or be weaker than target.
   */
  protected static class CalcToCalcUnifyRule extends AbstractUnifyRule {

    public static final CalcToCalcUnifyRule INSTANCE =
        new CalcToCalcUnifyRule();

    private CalcToCalcUnifyRule() {
      super(operand(MutableCalc.class, query(0)),
          operand(MutableCalc.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableCalc query = (MutableCalc) call.query;
      final Pair<RexNode, List<RexNode>> queryExplained = explainCalc(query);
      final RexNode queryCond = queryExplained.left;
      final List<RexNode> queryProjs = queryExplained.right;

      final MutableCalc target = (MutableCalc) call.target;
      final Pair<RexNode, List<RexNode>> targetExplained = explainCalc(target);
      final RexNode targetCond = targetExplained.left;
      final List<RexNode> targetProjs = targetExplained.right;

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      try {
        final RexShuttle shuttle = getRexShuttle(targetProjs);
        final RexNode splitted =
            splitFilter(call.getSimplify(), queryCond, targetCond);

        final RexNode compenCond;
        if (splitted != null) {
          if (splitted.isAlwaysTrue()) {
            compenCond = null;
          } else {
            // Compensate the residual filtering condition.
            compenCond = shuttle.apply(splitted);
          }
        } else if (implies(
            call.getCluster(), queryCond, targetCond, query.getInput().rowType)) {
          // Fail to split filtering condition, but implies that target contains
          // all lines of query, thus just set compensating filtering condition
          // as the filtering condition of query.
          compenCond = shuttle.apply(queryCond);
        } else {
          return null;
        }

        final List<RexNode> compenProjs = shuttle.apply(queryProjs);
        if (compenCond == null
            && RexUtil.isIdentity(compenProjs, target.rowType)) {
          return call.result(target);
        } else {
          final RexProgram compenRexProgram = RexProgram.create(
              target.rowType, compenProjs, compenCond,
              query.rowType, rexBuilder);
          final MutableCalc compenCalc = MutableCalc.of(target, compenRexProgram);
          return tryMergeParentCalcAndGenResult(call, compenCalc);
        }
      } catch (MatchFailed e) {
        return null;
      }
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has {@link MutableCalc} as left child to a {@link MutableJoin}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinOnLeftCalcToJoinUnifyRule extends AbstractUnifyRule {

    public static final JoinOnLeftCalcToJoinUnifyRule INSTANCE =
        new JoinOnLeftCalcToJoinUnifyRule();

    private JoinOnLeftCalcToJoinUnifyRule() {
      super(
        operand(MutableJoin.class, operand(MutableCalc.class, query(0)), query(1)),
        operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableCalc qInput0 = (MutableCalc) query.getLeft();
      final MutableRel qInput1 = query.getRight();
      final Pair<RexNode, List<RexNode>> qInput0Explained = explainCalc(qInput0);
      final RexNode qInput0Cond = qInput0Explained.left;
      final List<RexNode> qInput0Projs = qInput0Explained.right;

      final MutableJoin target = (MutableJoin) call.target;

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      // Try pulling up MutableCalc only when:
      // 1. it's inner join;
      // 2. it's outer join but no filttering condition from MutableCalc.
      final JoinRelType joinRelType = sameJoinType(query.joinType, target.joinType);
      if (joinRelType == null) {
        return null;
      }
      if (joinRelType != JoinRelType.INNER
          && !(joinRelType.isOuterJoin() && qInput0Cond.isAlwaysTrue())) {
        return null;
      }
      // Try pulling up MutableCalc only when Join condition references mapping.
      final List<RexNode> identityProjects =
          (List<RexNode>) rexBuilder.identityProjects(qInput1.rowType);
      if (!referenceByMapping(query.condition, qInput0Projs, identityProjects)) {
        return null;
      }

      final RexNode newQueryJoinCond = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int idx = inputRef.getIndex();
          if (idx < fieldCnt(qInput0)) {
            final int newIdx = ((RexInputRef) qInput0Projs.get(idx)).getIndex();
            return new RexInputRef(newIdx, inputRef.getType());
          } else {
            int newIdx = idx - fieldCnt(qInput0) + fieldCnt(qInput0.getInput());
            return new RexInputRef(newIdx, inputRef.getType());
          }
        }
      }.apply(query.condition);

      final RexNode splitted =
          splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
      // MutableJoin matches only when the conditions are analyzed to be same.
      if (splitted != null && splitted.isAlwaysTrue()) {
        final RexNode compenCond = qInput0Cond;
        final List<RexNode> compenProjs = new ArrayList<>();
        for (int i = 0; i < fieldCnt(query); i++) {
          if (i < fieldCnt(qInput0)) {
            compenProjs.add(qInput0Projs.get(i));
          } else {
            final int newIdx = i - fieldCnt(qInput0) + fieldCnt(qInput0.getInput());
            compenProjs.add(
                new RexInputRef(newIdx, query.rowType.getFieldList().get(i).getType()));
          }
        }
        final RexProgram compenRexProgram = RexProgram.create(
            target.rowType, compenProjs, compenCond,
            query.rowType, rexBuilder);
        final MutableCalc compenCalc = MutableCalc.of(target, compenRexProgram);
        return tryMergeParentCalcAndGenResult(call, compenCalc);
      }

      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has {@link MutableCalc} as right child to a {@link MutableJoin}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinOnRightCalcToJoinUnifyRule extends AbstractUnifyRule {

    public static final JoinOnRightCalcToJoinUnifyRule INSTANCE =
        new JoinOnRightCalcToJoinUnifyRule();

    private JoinOnRightCalcToJoinUnifyRule() {
      super(
          operand(MutableJoin.class, query(0), operand(MutableCalc.class, query(1))),
          operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableRel qInput0 = query.getLeft();
      final MutableCalc qInput1 = (MutableCalc) query.getRight();
      final Pair<RexNode, List<RexNode>> qInput1Explained = explainCalc(qInput1);
      final RexNode qInput1Cond = qInput1Explained.left;
      final List<RexNode> qInput1Projs = qInput1Explained.right;

      final MutableJoin target = (MutableJoin) call.target;

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      // Try pulling up MutableCalc only when:
      // 1. it's inner join;
      // 2. it's outer join but no filttering condition from MutableCalc.
      final JoinRelType joinRelType = sameJoinType(query.joinType, target.joinType);
      if (joinRelType == null) {
        return null;
      }
      if (joinRelType != JoinRelType.INNER
          && !(joinRelType.isOuterJoin() && qInput1Cond.isAlwaysTrue())) {
        return null;
      }
      // Try pulling up MutableCalc only when Join condition references mapping.
      final List<RexNode> identityProjects =
          (List<RexNode>) rexBuilder.identityProjects(qInput0.rowType);
      if (!referenceByMapping(query.condition, identityProjects, qInput1Projs)) {
        return null;
      }

      final RexNode newQueryJoinCond = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int idx = inputRef.getIndex();
          if (idx < fieldCnt(qInput0)) {
            return inputRef;
          } else {
            final int newIdx = ((RexInputRef) qInput1Projs.get(idx - fieldCnt(qInput0)))
                .getIndex() + fieldCnt(qInput0);
            return new RexInputRef(newIdx, inputRef.getType());
          }
        }
      }.apply(query.condition);

      final RexNode splitted =
          splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
      // MutableJoin matches only when the conditions are analyzed to be same.
      if (splitted != null && splitted.isAlwaysTrue()) {
        final RexNode compenCond =
            RexUtil.shift(qInput1Cond, qInput0.rowType.getFieldCount());
        final List<RexNode> compenProjs = new ArrayList<>();
        for (int i = 0; i < query.rowType.getFieldCount(); i++) {
          if (i < fieldCnt(qInput0)) {
            compenProjs.add(
                new RexInputRef(i, query.rowType.getFieldList().get(i).getType()));
          } else {
            final RexNode shifted = RexUtil.shift(qInput1Projs.get(i - fieldCnt(qInput0)),
                qInput0.rowType.getFieldCount());
            compenProjs.add(shifted);
          }
        }
        final RexProgram compensatingRexProgram = RexProgram.create(
            target.rowType, compenProjs, compenCond,
            query.rowType, rexBuilder);
        final MutableCalc compenCalc = MutableCalc.of(target, compensatingRexProgram);
        return tryMergeParentCalcAndGenResult(call, compenCalc);
      }
      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has {@link MutableCalc} as children to a {@link MutableJoin}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinOnCalcsToJoinUnifyRule extends AbstractUnifyRule {

    public static final JoinOnCalcsToJoinUnifyRule INSTANCE =
        new JoinOnCalcsToJoinUnifyRule();

    private JoinOnCalcsToJoinUnifyRule() {
      super(
          operand(MutableJoin.class,
              operand(MutableCalc.class, query(0)), operand(MutableCalc.class, query(1))),
          operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableCalc qInput0 = (MutableCalc) query.getLeft();
      final MutableCalc qInput1 = (MutableCalc) query.getRight();
      final Pair<RexNode, List<RexNode>> qInput0Explained = explainCalc(qInput0);
      final RexNode qInput0Cond = qInput0Explained.left;
      final List<RexNode> qInput0Projs = qInput0Explained.right;
      final Pair<RexNode, List<RexNode>> qInput1Explained = explainCalc(qInput1);
      final RexNode qInput1Cond = qInput1Explained.left;
      final List<RexNode> qInput1Projs = qInput1Explained.right;

      final MutableJoin target = (MutableJoin) call.target;

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      // Try pulling up MutableCalc only when:
      // 1. it's inner join;
      // 2. it's outer join but no filttering condition from MutableCalc.
      final JoinRelType joinRelType = sameJoinType(query.joinType, target.joinType);
      if (joinRelType == null) {
        return null;
      }
      if (joinRelType != JoinRelType.INNER
          && !(joinRelType.isOuterJoin()
              && qInput0Cond.isAlwaysTrue()
              && qInput1Cond.isAlwaysTrue())) {
        return null;
      }
      if (!referenceByMapping(query.condition, qInput0Projs, qInput1Projs)) {
        return null;
      }

      RexNode newQueryJoinCond = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int idx = inputRef.getIndex();
          if (idx < fieldCnt(qInput0)) {
            final int newIdx = ((RexInputRef) qInput0Projs.get(idx)).getIndex();
            return new RexInputRef(newIdx, inputRef.getType());
          } else {
            final int newIdx = ((RexInputRef) qInput1Projs.get(idx - fieldCnt(qInput0)))
                .getIndex() + fieldCnt(qInput0.getInput());
            return new RexInputRef(newIdx, inputRef.getType());
          }
        }
      }.apply(query.condition);
      final RexNode splitted =
          splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
      // MutableJoin matches only when the conditions are analyzed to be same.
      if (splitted != null && splitted.isAlwaysTrue()) {
        final RexNode qInput1CondShifted =
            RexUtil.shift(qInput1Cond, fieldCnt(qInput0.getInput()));
        final RexNode compenCond = RexUtil.composeConjunction(rexBuilder,
            ImmutableList.of(qInput0Cond, qInput1CondShifted));

        final List<RexNode> compenProjs = new ArrayList<>();
        for (int i = 0; i < query.rowType.getFieldCount(); i++) {
          if (i < fieldCnt(qInput0)) {
            compenProjs.add(qInput0Projs.get(i));
          } else {
            RexNode shifted = RexUtil.shift(qInput1Projs.get(i - fieldCnt(qInput0)),
                fieldCnt(qInput0.getInput()));
            compenProjs.add(shifted);
          }
        }
        final RexProgram compensatingRexProgram = RexProgram.create(
            target.rowType, compenProjs, compenCond,
            query.rowType, rexBuilder);
        final MutableCalc compensatingCalc =
            MutableCalc.of(target, compensatingRexProgram);
        return tryMergeParentCalcAndGenResult(call, compensatingCalc);
      }
      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableAggregate}
   * which has {@link MutableCalc} as child to a {@link MutableAggregate}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableAggregate},
   * then match the {@link MutableAggregate} in query to {@link MutableAggregate} in target.
   */
  protected static class AggregateOnCalcToAggUnifyRule extends AbstractUnifyRule {

    public static final AggregateOnCalcToAggUnifyRule INSTANCE =
        new AggregateOnCalcToAggUnifyRule();

    private AggregateOnCalcToAggUnifyRule() {
      super(operand(MutableAggregate.class, operand(MutableCalc.class, query(0))),
          operand(MutableAggregate.class, target(0)), 1);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {
      final MutableAggregate query = (MutableAggregate) call.query;
      final MutableCalc qInput = (MutableCalc) query.getInput();
      final Pair<RexNode, List<RexNode>> qInputExplained = explainCalc(qInput);
      final RexNode qInputCond = qInputExplained.left;
      final List<RexNode> qInputProjs = qInputExplained.right;

      final MutableAggregate target = (MutableAggregate) call.target;

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      final Mappings.TargetMapping mapping =
          Project.getMapping(fieldCnt(qInput.getInput()), qInputProjs);
      if (mapping == null) {
        return null;
      }

      if (!qInputCond.isAlwaysTrue()) {
        try {
          // Fail the matching when filtering condition references
          // non-grouping columns in target.
          qInputCond.accept(new RexVisitorImpl<Void>(true) {
            @Override public Void visitInputRef(RexInputRef inputRef) {
              if (!target.groupSets.stream()
                  .allMatch(groupSet -> groupSet.get(inputRef.getIndex()))) {
                throw Util.FoundOne.NULL;
              }
              return super.visitInputRef(inputRef);
            }
          });
        } catch (Util.FoundOne one) {
          return null;
        }
      }

      final Mapping inverseMapping = mapping.inverse();
      final MutableAggregate aggregate2 =
          permute(query, qInput.getInput(), inverseMapping);

      final Mappings.TargetMapping mappingForQueryCond = Mappings.target(
          target.groupSet::indexOf,
          target.getInput().rowType.getFieldCount(),
          target.groupSet.cardinality());
      final RexNode targetCond = RexUtil.apply(mappingForQueryCond, qInputCond);

      final MutableRel unifiedAggregate =
          unifyAggregates(aggregate2, targetCond, target);
      if (unifiedAggregate == null) {
        return null;
      }
      // Add Project if the mapping breaks order of fields in GroupSet
      if (!Mappings.keepsOrdering(mapping)) {
        final List<Integer> posList = new ArrayList<>();
        final int fieldCount = aggregate2.rowType.getFieldCount();
        for (int group: aggregate2.groupSet) {
          if (inverseMapping.getTargetOpt(group) != -1) {
            posList.add(inverseMapping.getTarget(group));
          }
        }
        for (int i = posList.size(); i < fieldCount; i++) {
          posList.add(i);
        }
        final List<RexNode> compenProjs =
            MutableRels.createProjectExprs(unifiedAggregate, posList);
        final RexProgram compensatingRexProgram = RexProgram.create(
            unifiedAggregate.rowType, compenProjs, null,
            query.rowType, rexBuilder);
        final MutableCalc compenCalc =
            MutableCalc.of(unifiedAggregate, compensatingRexProgram);
        if (unifiedAggregate instanceof MutableCalc) {
          final MutableCalc newCompenCalc =
              mergeCalc(rexBuilder, compenCalc, (MutableCalc) unifiedAggregate);
          return tryMergeParentCalcAndGenResult(call, newCompenCalc);
        } else {
          return tryMergeParentCalcAndGenResult(call, compenCalc);
        }
      } else {
        return tryMergeParentCalcAndGenResult(call, unifiedAggregate);
      }
    }
  }

  /** A {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link org.apache.calcite.rel.core.Aggregate} to a
   * {@link org.apache.calcite.rel.core.Aggregate}, provided
   * that they have the same child. */
  public static class AggregateToAggregateUnifyRule extends AbstractUnifyRule {
    public static final AggregateToAggregateUnifyRule INSTANCE =
        new AggregateToAggregateUnifyRule();

    private AggregateToAggregateUnifyRule() {
      super(operand(MutableAggregate.class, query(0)),
          operand(MutableAggregate.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableAggregate query = (MutableAggregate) call.query;
      final MutableAggregate target = (MutableAggregate) call.target;
      assert query != target;
      // in.query can be rewritten in terms of in.target if its groupSet is
      // a subset, and its aggCalls are a superset. For example:
      //   query: SELECT x, COUNT(b) FROM t GROUP BY x
      //   target: SELECT x, y, SUM(a) AS s, COUNT(b) AS cb FROM t GROUP BY x, y
      // transforms to
      //   result: SELECT x, SUM(cb) FROM (target) GROUP BY x
      if (query.getInput() != target.getInput()) {
        return null;
      }
      if (!target.groupSet.contains(query.groupSet)) {
        return null;
      }
      final MutableRel result = unifyAggregates(query, null, target);
      if (result == null) {
        return null;
      }
      return tryMergeParentCalcAndGenResult(call, result);
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableUnion} to a {@link MutableUnion} where the query and target
   * have the same inputs but might not have the same order.
   */
  public static class UnionToUnionUnifyRule extends AbstractUnifyRule {
    public static final UnionToUnionUnifyRule INSTANCE = new UnionToUnionUnifyRule();

    private UnionToUnionUnifyRule() {
      super(any(MutableUnion.class), any(MutableUnion.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableUnion query = (MutableUnion) call.query;
      final MutableUnion target = (MutableUnion) call.target;
      final List<MutableRel> queryInputs = new ArrayList<>(query.getInputs());
      final List<MutableRel> targetInputs = new ArrayList<>(target.getInputs());
      if (query.isAll() == target.isAll()
          && sameRelCollectionNoOrderConsidered(queryInputs, targetInputs)) {
        return call.result(target);
      }
      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableUnion}
   * which has {@link MutableCalc} as child to a {@link MutableUnion}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableUnion},
   * then match the {@link MutableUnion} in query to {@link MutableUnion} in target.
   */
  protected static class UnionOnCalcsToUnionUnifyRule extends AbstractUnifyRule {
    public static final UnionOnCalcsToUnionUnifyRule INSTANCE =
        new UnionOnCalcsToUnionUnifyRule();

    private UnionOnCalcsToUnionUnifyRule() {
      super(any(MutableUnion.class), any(MutableUnion.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableUnion query = (MutableUnion) call.query;
      final MutableUnion target = (MutableUnion) call.target;
      final List<MutableCalc> queryInputs = new ArrayList<>();
      final List<MutableRel> queryGrandInputs = new ArrayList<>();
      final List<MutableRel> targetInputs = new ArrayList<>(target.getInputs());

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      for (MutableRel rel: query.getInputs()) {
        if (rel instanceof MutableCalc) {
          queryInputs.add((MutableCalc) rel);
          queryGrandInputs.add(((MutableCalc) rel).getInput());
        } else {
          return null;
        }
      }

      if (query.isAll() && target.isAll()
          && sameRelCollectionNoOrderConsidered(queryGrandInputs, targetInputs)) {
        final Pair<RexNode, List<RexNode>> queryInputExplained0 =
            explainCalc(queryInputs.get(0));
        for (int i = 1; i < queryGrandInputs.size(); i++) {
          final Pair<RexNode, List<RexNode>> queryInputExplained =
              explainCalc(queryInputs.get(i));
          // Matching fails when filtering conditions are not equal or projects are not equal.
          if (!splitFilter(call.getSimplify(), queryInputExplained0.left,
              queryInputExplained.left).isAlwaysTrue()) {
            return null;
          }
          for (Pair<RexNode, RexNode> pair : Pair.zip(
              queryInputExplained0.right, queryInputExplained.right)) {
            if (!pair.left.equals(pair.right)) {
              return null;
            }
          }
        }
        final RexProgram compenRexProgram = RexProgram.create(
            target.rowType, queryInputExplained0.right, queryInputExplained0.left,
            query.rowType, rexBuilder);
        final MutableCalc compenCalc = MutableCalc.of(target, compenRexProgram);
        return tryMergeParentCalcAndGenResult(call, compenCalc);
      }

      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableProject} to a
   * {@link MutableProject} where the condition of the target
   * relation is weaker.
   *
   * <p>Example: target has a weaker condition and contains all columns selected
   * by query</p>
   * <ul>
   * <li>query:   Project(projects: [$2, $0])
   *                Filter(condition: &gt;($1, 20))
   *                  Scan(table: [hr, emps])</li>
   * <li>target:  Project(projects: [$0, $1, $2])
   *                Filter(condition: &gt;($1, 10))
   *                  Scan(table: [hr, emps])</li>
   * </ul>
   */
  public static class ProjectToProjectUnifyRule1 extends AbstractUnifyRule {
    public static final ProjectToProjectUnifyRule1 INSTANCE =
        new ProjectToProjectUnifyRule1();

    private ProjectToProjectUnifyRule1() {
      super(operand(MutableProject.class, query(0)),
          operand(MutableProject.class, target(0)), 1);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {
      final MutableProject query = (MutableProject) call.query;

      final List<RelDataTypeField> oldFieldList =
          query.getInput().rowType.getFieldList();
      final List<RelDataTypeField> newFieldList =
          call.target.rowType.getFieldList();
      List<RexNode> newProjects;
      try {
        newProjects = transformRex(query.projects, oldFieldList, newFieldList);
      } catch (MatchFailed e) {
        return null;
      }

      final MutableProject newProject =
          MutableProject.of(query.rowType, call.target, newProjects);

      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }

    @Override protected UnifyRuleCall match(SubstitutionVisitor visitor,
        MutableRel query, MutableRel target) {
      assert query instanceof MutableProject && target instanceof MutableProject;

      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          return null;
        } else if (targetOperand.isWeaker(visitor, target)) {

          final MutableProject queryProject = (MutableProject) query;
          if (queryProject.getInput() instanceof MutableFilter) {
            final MutableFilter innerFilter =
                (MutableFilter) queryProject.getInput();
            RexNode newCondition;
            try {
              newCondition = transformRex(innerFilter.condition,
                  innerFilter.getInput().rowType.getFieldList(),
                  target.rowType.getFieldList());
            } catch (MatchFailed e) {
              return null;
            }
            final MutableFilter newFilter = MutableFilter.of(target,
                newCondition);

            return visitor.new UnifyRuleCall(this, query, newFilter,
                copy(visitor.slots, slotCount));
          }
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableFilter} to a
   * {@link MutableFilter} where the condition of the target
   * relation is weaker.
   *
   * <p>Example: target has a weaker condition</p>
   * <ul>
   * <li>query:   Filter(condition: &gt;($1, 20))
   *                Scan(table: [hr, emps])</li>
   * <li>target:  Filter(condition: &gt;($1, 10))
   *                Scan(table: [hr, emps])</li>
   * </ul>
   */
  public static class FilterToFilterUnifyRule1 extends AbstractUnifyRule {
    public static final FilterToFilterUnifyRule1 INSTANCE =
        new FilterToFilterUnifyRule1();

    private FilterToFilterUnifyRule1() {
      super(operand(MutableFilter.class, query(0)),
          operand(MutableFilter.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableFilter query = (MutableFilter) call.query;
      final MutableFilter target = (MutableFilter) call.target;
      final MutableFilter newFilter = MutableFilter.of(target, query.condition);
      return call.result(newFilter);
    }

    @Override protected UnifyRuleCall match(SubstitutionVisitor visitor,
        MutableRel query, MutableRel target) {
      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          if (visitor.isWeaker(query, target)) {
            return visitor.new UnifyRuleCall(this, query, target,
                copy(visitor.slots, slotCount));
          }
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableFilter} to a
   * {@link MutableProject} on top of a
   * {@link MutableFilter} where the condition of the target
   * relation is weaker.
   *
   * <p>Example: target has a weaker condition and is a permutation projection of
   * its child relation</p>
   * <ul>
   * <li>query:   Filter(condition: &gt;($1, 20))
   *                Scan(table: [hr, emps])</li>
   * <li>target:  Project(projects: [$1, $0, $2, $3, $4])
   *                Filter(condition: &gt;($1, 10))
   *                  Scan(table: [hr, emps])</li>
   * </ul>
   */
  public static class FilterToProjectUnifyRule1 extends AbstractUnifyRule {
    public static final FilterToProjectUnifyRule1 INSTANCE =
        new FilterToProjectUnifyRule1();

    private FilterToProjectUnifyRule1() {
      super(
          operand(MutableFilter.class, query(0)),
          operand(MutableProject.class,
              operand(MutableFilter.class, target(0))), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableRel query = call.query;

      final List<RelDataTypeField> oldFieldList =
          query.rowType.getFieldList();
      final List<RelDataTypeField> newFieldList =
          call.target.rowType.getFieldList();
      List<RexNode> newProjects;
      try {
        newProjects = transformRex(
            (List<RexNode>) call.getCluster().getRexBuilder().identityProjects(
                query.rowType),
            oldFieldList, newFieldList);
      } catch (MatchFailed e) {
        return null;
      }

      final MutableProject newProject =
          MutableProject.of(query.rowType, call.target, newProjects);

      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }

    @Override protected UnifyRuleCall match(SubstitutionVisitor visitor,
        MutableRel query, MutableRel target) {
      assert query instanceof MutableFilter && target instanceof MutableProject;

      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          if (visitor.isWeaker(query, ((MutableProject) target).getInput())) {
            final MutableFilter filter = (MutableFilter) query;
            RexNode newCondition;
            try {
              newCondition = transformRex(filter.condition,
                  filter.getInput().rowType.getFieldList(),
                  target.rowType.getFieldList());
            } catch (MatchFailed e) {
              return null;
            }
            final MutableFilter newFilter = MutableFilter.of(target,
                newCondition);
            return visitor.new UnifyRuleCall(this, query, newFilter,
                copy(visitor.slots, slotCount));
          }
        }
      }
      return null;
    }
  }

  private static RexNode transformRex(RexNode node,
      final List<RelDataTypeField> oldFields,
      final List<RelDataTypeField> newFields) {
    List<RexNode> nodes =
        transformRex(ImmutableList.of(node), oldFields, newFields);
    return nodes.get(0);
  }

  private static List<RexNode> transformRex(
      List<RexNode> nodes,
      final List<RelDataTypeField> oldFields,
      final List<RelDataTypeField> newFields) {
    RexShuttle shuttle = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        RelDataTypeField f = oldFields.get(ref.getIndex());
        for (int index = 0; index < newFields.size(); index++) {
          RelDataTypeField newf = newFields.get(index);
          if (f.getKey().equals(newf.getKey())
              && f.getValue() == newf.getValue()) {
            return new RexInputRef(index, f.getValue());
          }
        }
        throw MatchFailed.INSTANCE;
      }
    };
    return shuttle.apply(nodes);
  }

>>>>>>> 2e6c42a57 ([CALCITE-3334]: Refinement for Substitution-Based MV Matching (#12))
  /** Check if list0 and list1 contains the same nodes -- order is not considered. */
  private static boolean sameRelCollectionNoOrderConsidered(
      List<MutableRel> list0, List<MutableRel> list1) {
    if (list0.size() != list1.size()) {
      return false;
    }
    for (MutableRel rel: list0) {
      int index = list1.indexOf(rel);
      if (index == -1) {
        return false;
      } else {
        list1.remove(index);
      }
    }
    return true;
  }

  private static int fieldCnt(MutableRel rel) {
    return rel.rowType.getFieldCount();
  }

  /** Explain filtering condition and projections from MutableCalc. */
  private static Pair<RexNode, List<RexNode>> explainCalc(MutableCalc calc) {
    final RexShuttle shuttle = getExpandShuttle(calc.program);
    final RexNode condition = shuttle.apply(calc.program.getCondition());
    final List<RexNode> projects = new ArrayList<>();
    for (RexNode rex: shuttle.apply(calc.program.getProjectList())) {
      projects.add(rex);
    }
    if (condition == null) {
      return Pair.of(calc.cluster.getRexBuilder().makeLiteral(true), projects);
    } else {
      return Pair.of(condition, projects);
    }
  }

  /**
   * Generate result by merging parent and child if they are both MutableCalc.
   * Otherwise result is the child itself.
   */
  private static UnifyResult tryMergeParentCalcAndGenResult(
      UnifyRuleCall call, MutableRel child) {
    final MutableRel parent = call.query.getParent();
    if (child instanceof MutableCalc && parent instanceof MutableCalc) {
      final MutableCalc mergedCalc = mergeCalc(call.getCluster().getRexBuilder(),
          (MutableCalc) parent, (MutableCalc) child);
      if (mergedCalc != null) {
        // Note that property of stopTrying in the result is false
        // and this query node deserves further matching iterations.
        return call.create(parent).result(mergedCalc, false);
      }
    }
    return call.result(child);
  }

  /** Merge two MutableCalc together. */
  private static MutableCalc mergeCalc(
      RexBuilder rexBuilder, MutableCalc topCalc, MutableCalc bottomCalc) {
    RexProgram topProgram = topCalc.program;
    if (RexOver.containsOver(topProgram)) {
      return null;
    }

    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topCalc.program,
            bottomCalc.program,
            rexBuilder);
    assert mergedProgram.getOutputRowType()
        == topProgram.getOutputRowType();
    return MutableCalc.of(bottomCalc.getInput(), mergedProgram);
  }

  private static RexShuttle getExpandShuttle(RexProgram rexProgram) {
    return new RexShuttle() {
      @Override public RexNode visitLocalRef(RexLocalRef localRef) {
        return rexProgram.expandLocalRef(localRef);
      }
    };
  }

  /** Check if condition cond0 implies cond1. */
  private static boolean implies(
      RelOptCluster cluster, RexNode cond0, RexNode cond1, RelDataType rowType) {
    RexExecutor executor = cluster.getPlanner().getExecutor();
    RexImplicationChecker rexImplicationChecker =
        new RexImplicationChecker(cluster.getRexBuilder(), executor, rowType);
    return rexImplicationChecker.implies(cond0, cond1);
  }

  /** Check if join condition only references RexInputRef. */
  private static boolean referenceByMapping(
      RexNode joinCondition, List<RexNode>... projectsOfInputs) {
    List<RexNode> projects = new ArrayList<>();
    for (List<RexNode> projectsOfInput: projectsOfInputs) {
      projects.addAll(projectsOfInput);
    }

    try {
      RexVisitor rexVisitor = new RexVisitorImpl<Void>(true) {
        @Override public Void visitInputRef(RexInputRef inputRef) {
          if (!(projects.get(inputRef.getIndex()) instanceof RexInputRef)) {
            throw Util.FoundOne.NULL;
          }
          return super.visitInputRef(inputRef);
        }
      };
      joinCondition.accept(rexVisitor);
    } catch (Util.FoundOne e) {
      return false;
    }
    return true;
  }

  private static JoinRelType sameJoinType(JoinRelType type0, JoinRelType type1) {
    if (type0 == type1) {
      return type0;
    } else {
      return null;
    }
  }

  public static MutableAggregate permute(MutableAggregate aggregate,
      MutableRel input, Mapping mapping) {
    ImmutableBitSet groupSet = Mappings.apply(mapping, aggregate.groupSet);
    ImmutableList<ImmutableBitSet> groupSets =
        Mappings.apply2(mapping, aggregate.groupSets);
    List<AggregateCall> aggregateCalls =
        apply(mapping, aggregate.aggCalls);
    return MutableAggregate.of(input, groupSet, groupSets, aggregateCalls);
  }

  private static List<AggregateCall> apply(final Mapping mapping,
      List<AggregateCall> aggCallList) {
    return Lists.transform(aggCallList,
        call -> call.copy(Mappings.apply2(mapping, call.getArgList()),
            Mappings.apply(mapping, call.filterArg)));
  }

  public static MutableRel unifyAggregates(MutableAggregate query,
      RexNode targetCond, MutableAggregate target) {
    MutableRel result;
    RexBuilder rexBuilder = query.cluster.getRexBuilder();
    if (query.groupSets.equals(target.groupSets)) {
      // Same level of aggregation. Generate a project.
      final List<Integer> projects = new ArrayList<>();
      final int groupCount = query.groupSet.cardinality();
      for (int i = 0; i < groupCount; i++) {
        projects.add(i);
      }
      for (AggregateCall aggregateCall : query.aggCalls) {
        int i = target.aggCalls.indexOf(aggregateCall);
        if (i < 0) {
          return null;
        }
        projects.add(groupCount + i);
      }

      List<RexNode> compenProjs = MutableRels.createProjectExprs(target, projects);
      RexProgram compenRexProgram = RexProgram.create(
          target.rowType, compenProjs, targetCond, query.rowType, rexBuilder);
      result = MutableCalc.of(target, compenRexProgram);
    } else if (target.getGroupType() == Aggregate.Group.SIMPLE) {
      // Query is coarser level of aggregation. Generate an aggregate.
      final Map<Integer, Integer> map = new HashMap<>();
      target.groupSet.forEach(k -> map.put(k, map.size()));
      for (int c : query.groupSet) {
        if (!map.containsKey(c)) {
          return null;
        }
      }
      final ImmutableBitSet groupSet = query.groupSet.permute(map);
      ImmutableList<ImmutableBitSet> groupSets = null;
      if (query.getGroupType() != Aggregate.Group.SIMPLE) {
        groupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
            ImmutableBitSet.permute(query.groupSets, map));
      }
      final List<AggregateCall> aggregateCalls = new ArrayList<>();
      for (AggregateCall aggregateCall : query.aggCalls) {
        if (aggregateCall.isDistinct()) {
          return null;
        }
        int i = target.aggCalls.indexOf(aggregateCall);
        if (i < 0) {
          return null;
        }
        aggregateCalls.add(
            AggregateCall.create(getRollup(aggregateCall.getAggregation()),
                aggregateCall.isDistinct(), aggregateCall.isApproximate(),
                aggregateCall.ignoreNulls(),
                ImmutableList.of(target.groupSet.cardinality() + i), -1,
                aggregateCall.collation, aggregateCall.type,
                aggregateCall.name));
      }
      if (targetCond != null && !targetCond.isAlwaysTrue()) {
        RexProgram compenRexProgram = RexProgram.create(
            target.rowType, rexBuilder.identityProjects(target.rowType),
            targetCond, target.rowType, rexBuilder);

        result = MutableAggregate.of(
            MutableCalc.of(target, compenRexProgram),
            groupSet, groupSets, aggregateCalls);
      } else {
        result = MutableAggregate.of(
            target, groupSet, groupSets, aggregateCalls);
      }
    } else {
      return null;
    }
    return result;
  }

  public static SqlAggFunction getRollup(SqlAggFunction aggregation) {
    if (aggregation == SqlStdOperatorTable.SUM
        || aggregation == SqlStdOperatorTable.MIN
        || aggregation == SqlStdOperatorTable.MAX
        || aggregation == SqlStdOperatorTable.SUM0
        || aggregation == SqlStdOperatorTable.ANY_VALUE) {
      return aggregation;
    } else if (aggregation == SqlStdOperatorTable.COUNT) {
      return SqlStdOperatorTable.SUM0;
    } else {
      return null;
    }
  }

  protected static RexShuttle getRexShuttle(MutableProject target) {
    return getRexShuttle(target.projects);
  }

  /** Builds a shuttle that stores a list of expressions, and can map incoming
   * expressions to references to them. */
  private static RexShuttle getRexShuttle(List<RexNode> rexNodes) {
    final Map<RexNode, Integer> map = new HashMap<>();
    for (RexNode e : rexNodes) {
      map.put(e, map.size());
    }
    return new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        final Integer integer = map.get(ref);
        if (integer != null) {
          return new RexInputRef(integer, ref.getType());
        }
        throw MatchFailed.INSTANCE;
      }

      @Override public RexNode visitCall(RexCall call) {
        final Integer integer = map.get(call);
        if (integer != null) {
          return new RexInputRef(integer, call.getType());
        }
        return super.visitCall(call);
      }

      @Override public RexNode visitLiteral(RexLiteral literal) {
        final Integer integer = map.get(literal);
        if (integer != null) {
          return new RexInputRef(integer, literal.getType());
        }
        return super.visitLiteral(literal);
      }
    };
  }

  /** Returns if one rel is weaker than another. */
  protected boolean isWeaker(MutableRel rel0, MutableRel rel) {
    if (rel0 == rel || equivalents.get(rel0).contains(rel)) {
      return false;
    }

    if (!(rel0 instanceof MutableFilter)
        || !(rel instanceof MutableFilter)) {
      return false;
    }

    if (!rel.rowType.equals(rel0.rowType)) {
      return false;
    }

    final MutableRel rel0input = ((MutableFilter) rel0).getInput();
    final MutableRel relinput = ((MutableFilter) rel).getInput();
    if (rel0input != relinput
        && !equivalents.get(rel0input).contains(relinput)) {
      return false;
    }

    return implies(rel0.cluster, ((MutableFilter) rel0).condition,
        ((MutableFilter) rel).condition, rel.rowType);
  }

  /** Returns whether two relational expressions have the same row-type. */
  public static boolean equalType(String desc0, MutableRel rel0, String desc1,
      MutableRel rel1, Litmus litmus) {
    return RelOptUtil.equal(desc0, rel0.rowType, desc1, rel1.rowType, litmus);
  }

  /** Operand to a {@link UnifyRule}. */
  protected abstract static class Operand {
    protected final Class<? extends MutableRel> clazz;

    protected Operand(Class<? extends MutableRel> clazz) {
      this.clazz = clazz;
    }

    public abstract boolean matches(SubstitutionVisitor visitor, MutableRel rel);

    public boolean isWeaker(SubstitutionVisitor visitor, MutableRel rel) {
      return false;
    }
  }

  /** Operand to a {@link UnifyRule} that matches a relational expression of a
   * given type. It has zero or more child operands. */
  private static class InternalOperand extends Operand {
    private final List<Operand> inputs;

    InternalOperand(Class<? extends MutableRel> clazz, List<Operand> inputs) {
      super(clazz);
      this.inputs = inputs;
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      return clazz.isInstance(rel)
          && allMatch(visitor, inputs, rel.getInputs());
    }

    @Override public boolean isWeaker(SubstitutionVisitor visitor, MutableRel rel) {
      return clazz.isInstance(rel)
          && allWeaker(visitor, inputs, rel.getInputs());
    }
    private static boolean allMatch(SubstitutionVisitor visitor,
        List<Operand> operands, List<MutableRel> rels) {
      if (operands.size() != rels.size()) {
        return false;
      }
      for (Pair<Operand, MutableRel> pair : Pair.zip(operands, rels)) {
        if (!pair.left.matches(visitor, pair.right)) {
          return false;
        }
      }
      return true;
    }

    private static boolean allWeaker(
        SubstitutionVisitor visitor,
        List<Operand> operands, List<MutableRel> rels) {
      if (operands.size() != rels.size()) {
        return false;
      }
      for (Pair<Operand, MutableRel> pair : Pair.zip(operands, rels)) {
        if (!pair.left.isWeaker(visitor, pair.right)) {
          return false;
        }
      }
      return true;
    }
  }

  /** Operand to a {@link UnifyRule} that matches a relational expression of a
   * given type. */
  private static class AnyOperand extends Operand {
    AnyOperand(Class<? extends MutableRel> clazz) {
      super(clazz);
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      return clazz.isInstance(rel);
    }
  }

  /** Operand that assigns a particular relational expression to a variable.
   *
   * <p>It is applied to a descendant of the query, writes the operand into the
   * slots array, and always matches.
   * There is a corresponding operand of type {@link TargetOperand} that checks
   * whether its relational expression, a descendant of the target, is
   * equivalent to this {@code QueryOperand}'s relational expression.
   */
  private static class QueryOperand extends Operand {
    private final int ordinal;

    protected QueryOperand(int ordinal) {
      super(MutableRel.class);
      this.ordinal = ordinal;
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      visitor.slots[ordinal] = rel;
      return true;
    }
  }

  /** Operand that checks that a relational expression matches the corresponding
   * relational expression that was passed to a {@link QueryOperand}. */
  private static class TargetOperand extends Operand {
    private final int ordinal;

    protected TargetOperand(int ordinal) {
      super(MutableRel.class);
      this.ordinal = ordinal;
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      final MutableRel rel0 = visitor.slots[ordinal];
      assert rel0 != null : "QueryOperand should have been called first";
      return rel0 == rel || visitor.equivalents.get(rel0).contains(rel);
    }

    @Override public boolean isWeaker(SubstitutionVisitor visitor, MutableRel rel) {
      final MutableRel rel0 = visitor.slots[ordinal];
      assert rel0 != null : "QueryOperand should have been called first";
      return visitor.isWeaker(rel0, rel);
    }
  }

  /** Visitor that counts how many {@link QueryOperand} and
   * {@link TargetOperand} in an operand tree. */
  private static class SlotCounter {
    int queryCount;
    int targetCount;

    void visit(Operand operand) {
      if (operand instanceof QueryOperand) {
        ++queryCount;
      } else if (operand instanceof TargetOperand) {
        ++targetCount;
      } else if (operand instanceof AnyOperand) {
        // nothing
      } else {
        for (Operand input : ((InternalOperand) operand).inputs) {
          visit(input);
        }
      }
    }
  }
<<<<<<< HEAD
<<<<<<< HEAD
=======

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
}
