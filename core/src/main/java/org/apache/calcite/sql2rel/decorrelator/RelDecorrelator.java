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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * RelDecorrelator replaces all correlated expressions (corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 *
 * <p>TODO:</p>
 * <ul>
 *   <li>replace {@code CorelMap} constructor parameter with a RelNode
 *   <li>make {@link #currentRel} immutable (would require a fresh
 *      RelDecorrelator for each node being decorrelated)</li>
 *   <li>make fields of {@code CorelMap} immutable</li>
 *   <li>make sub-class rules static, and have them create their own
 *   de-correlator</li>
 * </ul>
 *
 * <p>Note: make all the members protected scope so that they can be
 * accessed by the sub-class.
 */
public class RelDecorrelator implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  static final Logger SQL2REL_LOGGER =
      CalciteTrace.getSqlToRelTracer();

  //~ Instance fields --------------------------------------------------------

  protected final RelBuilder relBuilder;

  // map built during translation
  CorelMap cm;

  @SuppressWarnings("method.invocation.invalid")
  protected final ReflectUtil.MethodDispatcher<@Nullable Frame> dispatcher =
      ReflectUtil.<RelNode, @Nullable Frame>createMethodDispatcher(
          Frame.class, getVisitor(), "decorrelateRel",
          RelNode.class,
          boolean.class);

  // The rel which is being visited
  protected @Nullable RelNode currentRel;

  protected final Context context;

  /** Built during decorrelation, of rel to all the newly created correlated
   * variables in its output, and to map old input positions to new input
   * positions. This is from the view point of the parent rel of a new rel. */
  protected final Map<RelNode, Frame> map = new HashMap<>();

  protected final HashSet<Correlate> generatedCorRels = new HashSet<>();

  //~ Constructors -----------------------------------------------------------

  protected RelDecorrelator(
      CorelMap cm,
      Context context,
      RelBuilder relBuilder) {
    this.cm = cm;
    this.context = context;
    this.relBuilder = relBuilder;
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public static RelNode decorrelateQuery(RelNode rootRel) {
    final RelBuilder relBuilder =
        RelFactories.LOGICAL_BUILDER.create(rootRel.getCluster(), null);
    return decorrelateQuery(rootRel, relBuilder);
  }

  /** Decorrelates a query.
   *
   * <p>This is the main entry point to {@code RelDecorrelator}.
   *
   * @param rootRel           Root node of the query
   * @param relBuilder        Builder for relational expressions
   *
   * @return Equivalent query with all
   * {@link org.apache.calcite.rel.core.Correlate} instances removed
   */
  public static RelNode decorrelateQuery(RelNode rootRel,
      RelBuilder relBuilder) {
    final CorelMap corelMap = CorelMap.build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final RelDecorrelator decorrelator =
        new RelDecorrelator(corelMap,
            cluster.getPlanner().getContext(), relBuilder);

    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (SQL2REL_LOGGER.isDebugEnabled()) {
      SQL2REL_LOGGER.debug(
          RelOptUtil.dumpPlan("Plan after removing Correlator", newRootRel,
              SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }

    if (!decorrelator.cm.mapCorToCorRel.isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    // Re-propagate the hints.
    newRootRel = RelOptUtil.propagateRelHints(newRootRel, true);

    return newRootRel;
  }

  void setCurrent(@Nullable RelNode root, @Nullable Correlate corRel) {
    currentRel = corRel;
    if (corRel != null) {
      cm = CorelMap.build(Util.first(root, corRel));
    }
  }

  protected RelBuilderFactory relBuilderFactory() {
    return RelBuilder.proto(relBuilder);
  }

  protected RelNode decorrelate(RelNode root) {
    // first adjust count() expression if any
    final RelBuilderFactory f = relBuilderFactory();
    HepProgram program = HepProgram.builder()
        .addRuleInstance(
            AdjustProjectForCountAggregateRule.config(false, this, f).toRule())
        .addRuleInstance(
            AdjustProjectForCountAggregateRule.config(true, this, f).toRule())
        .addRuleInstance(
            FilterJoinRule.FilterIntoJoinRule.Config.DEFAULT
                .withRelBuilderFactory(f)
                .withOperandSupplier(b0 ->
                    b0.operand(Filter.class).oneInput(b1 ->
                        b1.operand(Join.class).anyInputs()))
                .withDescription("FilterJoinRule:filter")
                .as(FilterJoinRule.FilterIntoJoinRule.Config.class)
                .withSmart(true)
                .withPredicate((join, joinType, exp) -> true)
                .as(FilterJoinRule.FilterIntoJoinRule.Config.class)
                .toRule())
        .addRuleInstance(
            CoreRules.FILTER_PROJECT_TRANSPOSE.config
                .withRelBuilderFactory(f)
                .as(FilterProjectTransposeRule.Config.class)
                .withOperandFor(Filter.class, filter ->
                        !RexUtil.containsCorrelation(filter.getCondition()),
                    Project.class, project -> true)
                .withCopyFilter(true)
                .withCopyProject(true)
                .toRule())
        .addRuleInstance(FilterCorrelateRule.Config.DEFAULT
            .withRelBuilderFactory(f)
            .toRule())
        .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    root = planner.findBestExp();

    // Perform decorrelation.
    map.clear();

    final Frame frame = getInvoke(root, false, null);
    if (frame != null) {
      // has been rewritten; apply rules post-decorrelation
      final HepProgramBuilder builder = HepProgram.builder()
          .addRuleInstance(
              CoreRules.FILTER_INTO_JOIN.config
                  .withRelBuilderFactory(f)
                  .toRule())
          .addRuleInstance(
              CoreRules.JOIN_CONDITION_PUSH.config
                  .withRelBuilderFactory(f)
                  .toRule());
      if (!getPostDecorrelateRules().isEmpty()) {
        builder.addRuleCollection(getPostDecorrelateRules());
      }
      final HepProgram program2 = builder.build();

      final HepPlanner planner2 = createPlanner(program2);
      final RelNode newRoot = frame.r;
      planner2.setRoot(newRoot);
      return planner2.findBestExp();
    }

    return root;
  }

  private Function2<RelNode, RelNode, @Nullable Void> createCopyHook() {
    return (oldNode, newNode) -> {
      if (cm.mapRefRelToCorRef.containsKey(oldNode)) {
        cm.mapRefRelToCorRef.putAll(newNode,
            cm.mapRefRelToCorRef.get(oldNode));
      }
      if (oldNode instanceof Correlate
          && newNode instanceof Correlate) {
        Correlate oldCor = (Correlate) oldNode;
        CorrelationId c = oldCor.getCorrelationId();
        if (cm.mapCorToCorRel.get(c) == oldNode) {
          cm.mapCorToCorRel.put(c, newNode);
        }

        if (generatedCorRels.contains(oldNode)) {
          generatedCorRels.add((Correlate) newNode);
        }
      }
      return null;
    };
  }

  private HepPlanner createPlanner(HepProgram program) {
    // Create a planner with a hook to update the mapping tables when a
    // node is copied when it is registered.
    return new HepPlanner(
        program,
        context,
        true,
        createCopyHook(),
        RelOptCostImpl.FACTORY);
  }

  public RelNode removeCorrelationViaRule(RelNode root) {
    final RelBuilderFactory f = relBuilderFactory();
    HepProgram program = HepProgram.builder()
        .addRuleInstance(RemoveSingleAggregateRule.config(f).toRule())
        .addRuleInstance(
            RemoveCorrelationForScalarProjectRule.config(this, f).toRule())
        .addRuleInstance(
            RemoveCorrelationForScalarAggregateRule.config(this, f).toRule())
        .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    return planner.findBestExp();
  }

  protected RexNode decorrelateExpr(RelNode currentRel,
      Map<RelNode, Frame> map, CorelMap cm, RexNode exp) {
    DecorrelateRexShuttle shuttle =
        new DecorrelateRexShuttle(currentRel, map, cm);
    return exp.accept(shuttle);
  }

  /** Fallback if none of the other {@code decorrelateRel} methods match. */
  public @Nullable Frame decorrelateRel(RelNode rel, boolean isCorVarDefined) {
    RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());

    if (rel.getInputs().size() > 0) {
      List<RelNode> oldInputs = rel.getInputs();
      List<RelNode> newInputs = new ArrayList<>();
      for (int i = 0; i < oldInputs.size(); ++i) {
        final Frame frame = getInvoke(oldInputs.get(i), isCorVarDefined, rel);
        if (frame == null || !frame.corDefOutputs.isEmpty()) {
          // if input is not rewritten, or if it produces correlated
          // variables, terminate rewrite
          return null;
        }
        newInputs.add(frame.r);
        newRel.replaceInput(i, frame.r);
      }

      if (!Util.equalShallow(oldInputs, newInputs)) {
        newRel = rel.copy(rel.getTraitSet(), newInputs);
      }
    }

    // the output position should not change since there are no corVars
    // coming from below.
    return register(rel, newRel, identityMap(rel.getRowType().getFieldCount()),
        ImmutableSortedMap.of());
  }

  public @Nullable Frame decorrelateRel(Sort rel, boolean isCorVarDefined) {
    //
    // Rewrite logic:
    //
    // 1. change the collations field to reference the new input.
    //

    // Sort itself should not reference corVars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    // Sort only references field positions in collations field.
    // The collations field in the newRel now need to refer to the
    // new output positions in its input.
    // Its output does not change the input ordering, so there's no
    // need to call propagateExpr.

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, isCorVarDefined, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }

    final RelNode newInput = frame.r;

    Mappings.TargetMapping mapping =
        Mappings.target(frame.oldToNewOutputs,
            oldInput.getRowType().getFieldCount(),
            newInput.getRowType().getFieldCount());

    RelCollation oldCollation = rel.getCollation();
    RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

    final int offset = rel.offset == null ? -1 : RexLiteral.intValue(rel.offset);
    final int fetch = rel.fetch == null ? -1 : RexLiteral.intValue(rel.fetch);

    final RelNode newSort = relBuilder
            .push(newInput)
            .sortLimit(offset, fetch, relBuilder.fields(newCollation))
            .build();

    // Sort does not change input ordering
    return register(rel, newSort, frame.oldToNewOutputs, frame.corDefOutputs);
  }

  public @Nullable Frame decorrelateRel(LogicalAggregate rel, boolean isCorVarDefined) {
    return decorrelateRel((Aggregate) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(Aggregate rel, boolean isCorVarDefined) {
    //
    // Rewrite logic:
    //
    // 1. Permute the group by keys to the front.
    // 2. If the input of an aggregate produces correlated variables,
    //    add them to the group list.
    // 3. Change aggCalls to reference the new project.
    //

    // Aggregate itself should not reference corVars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, isCorVarDefined, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final RelNode newInput = frame.r;

    // aggregate outputs mapping: group keys and aggregates
    final Map<Integer, Integer> outputMap = new HashMap<>();

    // map from newInput
    final Map<Integer, Integer> mapNewInputToProjOutputs = new HashMap<>();
    final int oldGroupKeyCount = rel.getGroupSet().cardinality();

    // Project projects the original expressions,
    // plus any correlated variables the input wants to pass along.
    final List<Pair<RexNode, String>> projects = new ArrayList<>();

    List<RelDataTypeField> newInputOutput =
        newInput.getRowType().getFieldList();

    int newPos = 0;

    // oldInput has the original group by keys in the front.
    final NavigableMap<Integer, RexLiteral> omittedConstants = new TreeMap<>();
    for (int i = 0; i < oldGroupKeyCount; i++) {
      final RexLiteral constant = projectedLiteral(newInput, i);
      if (constant != null) {
        // Exclude constants. Aggregate({true}) occurs because Aggregate({})
        // would generate 1 row even when applied to an empty table.
        omittedConstants.put(i, constant);
        continue;
      }

      // add mapping of group keys.
      outputMap.put(i, newPos);
      int newInputPos = frame.oldToNewOutputs.get(i);
      projects.add(RexInputRef.of2(newInputPos, newInputOutput));
      mapNewInputToProjOutputs.put(newInputPos, newPos);
      newPos++;
    }

    final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    if (!frame.corDefOutputs.isEmpty()) {
      // If input produces correlated variables, move them to the front,
      // right after any existing GROUP BY fields.

      // Now add the corVars from the input, starting from
      // position oldGroupKeyCount.
      for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
        projects.add(RexInputRef.of2(entry.getValue(), newInputOutput));

        corDefOutputs.put(entry.getKey(), newPos);
        mapNewInputToProjOutputs.put(entry.getValue(), newPos);
        newPos++;
      }
    }

    // add the remaining fields
    final int newGroupKeyCount = newPos;
    for (int i = 0; i < newInputOutput.size(); i++) {
      if (!mapNewInputToProjOutputs.containsKey(i)) {
        projects.add(RexInputRef.of2(i, newInputOutput));
        mapNewInputToProjOutputs.put(i, newPos);
        newPos++;
      }
    }

    assert newPos == newInputOutput.size();

    // This Project will be what the old input maps to,
    // replacing any previous mapping from old input).
    RelNode newProject = relBuilder.push(newInput)
        .projectNamed(Pair.left(projects), Pair.right(projects), true)
        .build();

    newProject = RelOptUtil.copyRelHints(newInput, newProject);

    // update mappings:
    // oldInput ----> newInput
    //
    //                newProject
    //                   |
    // oldInput ----> newInput
    //
    // is transformed to
    //
    // oldInput ----> newProject
    //                   |
    //                newInput
    Map<Integer, Integer> combinedMap = new HashMap<>();

    for (Map.Entry<Integer, Integer> entry : frame.oldToNewOutputs.entrySet()) {
      combinedMap.put(entry.getKey(),
          requireNonNull(mapNewInputToProjOutputs.get(entry.getValue()),
              () -> "mapNewInputToProjOutputs.get(" + entry.getValue() + ")"));
    }

    register(oldInput, newProject, combinedMap, corDefOutputs);

    // now it's time to rewrite the Aggregate
    final ImmutableBitSet newGroupSet = ImmutableBitSet.range(newGroupKeyCount);
    List<AggregateCall> newAggCalls = new ArrayList<>();
    List<AggregateCall> oldAggCalls = rel.getAggCallList();

    final Iterable<ImmutableBitSet> newGroupSets;
    if (rel.getGroupType() == Aggregate.Group.SIMPLE) {
      newGroupSets = null;
    } else {
      final ImmutableBitSet addedGroupSet =
          ImmutableBitSet.range(oldGroupKeyCount, newGroupKeyCount);
      newGroupSets =
          ImmutableBitSet.ORDERING.immutableSortedCopy(
              Util.transform(rel.getGroupSets(),
                  bitSet -> bitSet.union(addedGroupSet)));
    }

    int oldInputOutputFieldCount = rel.getGroupSet().cardinality();
    int newInputOutputFieldCount = newGroupSet.cardinality();

    int i = -1;
    for (AggregateCall oldAggCall : oldAggCalls) {
      ++i;
      List<Integer> oldAggArgs = oldAggCall.getArgList();

      List<Integer> aggArgs = new ArrayList<>();

      // Adjust the Aggregate argument positions.
      // Note Aggregate does not change input ordering, so the input
      // output position mapping can be used to derive the new positions
      // for the argument.
      for (int oldPos : oldAggArgs) {
        aggArgs.add(
            requireNonNull(combinedMap.get(oldPos),
                () -> "combinedMap.get(" + oldPos + ")"));
      }
      final int filterArg = oldAggCall.filterArg < 0 ? oldAggCall.filterArg
          : requireNonNull(combinedMap.get(oldAggCall.filterArg),
              () -> "combinedMap.get(" + oldAggCall.filterArg + ")");

      newAggCalls.add(
          oldAggCall.adaptTo(newProject, aggArgs, filterArg,
              oldGroupKeyCount, newGroupKeyCount));

      // The old to new output position mapping will be the same as that
      // of newProject, plus any aggregates that the oldAgg produces.
      outputMap.put(
          oldInputOutputFieldCount + i,
          newInputOutputFieldCount + i);
    }

    relBuilder.push(newProject)
        .aggregate(newGroupSets == null
                ? relBuilder.groupKey(newGroupSet)
                : relBuilder.groupKey(newGroupSet, newGroupSets),
            newAggCalls);

    if (!omittedConstants.isEmpty()) {
      final List<RexNode> postProjects = new ArrayList<>(relBuilder.fields());
      for (Map.Entry<Integer, RexLiteral> entry
          : omittedConstants.descendingMap().entrySet()) {
        int index = entry.getKey() + frame.corDefOutputs.size();
        postProjects.add(index, entry.getValue());
        // Shift the outputs whose index equals with or bigger than the added index
        // with 1 offset.
        shiftMapping(outputMap, index, 1);
        // Then add the constant key mapping.
        outputMap.put(entry.getKey(), index);
      }
      relBuilder.project(postProjects);
    }

    RelNode newRel = RelOptUtil.copyRelHints(rel, relBuilder.build());

    // Aggregate does not change input ordering so corVars will be
    // located at the same position as the input newProject.
    return register(rel, newRel, outputMap, corDefOutputs);
  }

  /**
   * Shift the mapping to fixed offset from the {@code startIndex}.
   *
   * @param mapping    The original mapping
   * @param startIndex Any output whose index equals with or bigger than the starting index
   *                   would be shift
   * @param offset     Shift offset
   */
  private static void shiftMapping(Map<Integer, Integer> mapping, int startIndex, int offset) {
    for (Map.Entry<Integer, Integer> entry : mapping.entrySet()) {
      if (entry.getValue() >= startIndex) {
        entry.setValue(entry.getValue() + offset);
      }
    }
  }

  public @Nullable Frame getInvoke(RelNode r, boolean isCorVarDefined, @Nullable RelNode parent) {
    final Frame frame = dispatcher.invoke(r, isCorVarDefined);
    currentRel = parent;
    if (frame != null && isCorVarDefined && r instanceof Sort) {
      final Sort sort = (Sort) r;
      // Can not decorrelate if the sort has per-correlate-key attributes like
      // offset or fetch limit, because these attributes scope would change to
      // global after decorrelation. They should take effect within the scope
      // of the correlation key actually.
      if (sort.offset != null || sort.fetch != null) {
        return null;
      }
    }
    if (frame != null) {
      map.put(r, frame);
    }
    return frame;
  }

  /** Returns a literal output field, or null if it is not literal. */
  private static @Nullable RexLiteral projectedLiteral(RelNode rel, int i) {
    if (rel instanceof Project) {
      final Project project = (Project) rel;
      final RexNode node = project.getProjects().get(i);
      if (node instanceof RexLiteral) {
        return (RexLiteral) node;
      }
    }
    return null;
  }

  public @Nullable Frame decorrelateRel(LogicalProject rel, boolean isCorVarDefined) {
    return decorrelateRel((Project) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(Project rel, boolean isCorVarDefined) {
    //
    // Rewrite logic:
    //
    // 1. Pass along any correlated variables coming from the input.
    //

    final RelNode oldInput = rel.getInput();
    Frame frame = getInvoke(oldInput, isCorVarDefined, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final List<RexNode> oldProjects = rel.getProjects();
    final List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

    // Project projects the original expressions,
    // plus any correlated variables the input wants to pass along.
    final List<Pair<RexNode, String>> projects = new ArrayList<>();

    // If this Project has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorRef.containsKey(rel)) {
      frame = decorrelateInputWithValueGenerator(rel, frame);
    }

    // Project projects the original expressions
    final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
    int newPos;
    for (newPos = 0; newPos < oldProjects.size(); newPos++) {
      projects.add(
          newPos,
          Pair.of(
              decorrelateExpr(requireNonNull(currentRel, "currentRel"),
                  map, cm, oldProjects.get(newPos)),
              relOutput.get(newPos).getName()));
      mapOldToNewOutputs.put(newPos, newPos);
    }

    // Project any correlated variables the input wants to pass along.
    final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
      projects.add(
          RexInputRef.of2(entry.getValue(),
              frame.r.getRowType().getFieldList()));
      corDefOutputs.put(entry.getKey(), newPos);
      newPos++;
    }

    RelNode newProject = relBuilder.push(frame.r)
        .projectNamed(Pair.left(projects), Pair.right(projects), true)
        .build();

    newProject = RelOptUtil.copyRelHints(rel, newProject);

    return register(rel, newProject, mapOldToNewOutputs, corDefOutputs);
  }

  /**
   * Create RelNode tree that produces a list of correlated variables.
   *
   * @param correlations         correlated variables to generate
   * @param valueGenFieldOffset  offset in the output that generated columns
   *                             will start
   * @param corDefOutputs        output positions for the correlated variables
   *                             generated
   * @return RelNode the root of the resultant RelNode tree
   */
  private @Nullable RelNode createValueGenerator(
      Iterable<CorRef> correlations,
      int valueGenFieldOffset,
      NavigableMap<CorDef, Integer> corDefOutputs) {
    final Map<RelNode, List<Integer>> mapNewInputToOutputs = new HashMap<>();

    final Map<RelNode, Integer> mapNewInputToNewOffset = new HashMap<>();

    // Input provides the definition of a correlated variable.
    // Add to map all the referenced positions (relative to each input rel).
    for (CorRef corVar : correlations) {
      final int oldCorVarOffset = corVar.field;

      final RelNode oldInput = getCorRel(corVar);
      assert oldInput != null;
      final Frame frame = getOrCreateFrame(oldInput);
      assert frame != null;
      final RelNode newInput = frame.r;

      final List<Integer> newLocalOutputs;
      if (!mapNewInputToOutputs.containsKey(newInput)) {
        newLocalOutputs = new ArrayList<>();
      } else {
        newLocalOutputs = mapNewInputToOutputs.get(newInput);
      }

      final int newCorVarOffset = frame.oldToNewOutputs.get(oldCorVarOffset);

      // Add all unique positions referenced.
      if (!newLocalOutputs.contains(newCorVarOffset)) {
        newLocalOutputs.add(newCorVarOffset);
      }
      mapNewInputToOutputs.put(newInput, newLocalOutputs);
    }

    int offset = 0;

    // Project only the correlated fields out of each input
    // and join the project together.
    // To make sure the plan does not change in terms of join order,
    // join these rels based on their occurrence in corVar list which
    // is sorted.
    final Set<RelNode> joinedInputs = new HashSet<>();

    RelNode r = null;
    for (CorRef corVar : correlations) {
      final RelNode oldInput = getCorRel(corVar);
      assert oldInput != null;
      final RelNode newInput = getOrCreateFrame(oldInput).r;
      assert newInput != null;

      if (!joinedInputs.contains(newInput)) {
        final List<Integer> positions = requireNonNull(mapNewInputToOutputs.get(newInput),
            () -> "mapNewInputToOutputs.get(" + newInput + ")");

        RelNode distinct = relBuilder.push(newInput)
            .project(relBuilder.fields(positions))
            .distinct()
            .build();
        RelOptCluster cluster = distinct.getCluster();

        joinedInputs.add(newInput);
        mapNewInputToNewOffset.put(newInput, offset);
        offset += distinct.getRowType().getFieldCount();

        if (r == null) {
          r = distinct;
        } else {
          r = relBuilder.push(r).push(distinct)
              .join(JoinRelType.INNER, cluster.getRexBuilder().makeLiteral(true)).build();
        }
      }
    }

    // Translate the positions of correlated variables to be relative to
    // the join output, leaving room for valueGenFieldOffset because
    // valueGenerators are joined with the original left input of the rel
    // referencing correlated variables.
    for (CorRef corRef : correlations) {
      // The first input of a Correlate is always the rel defining
      // the correlated variables.
      final RelNode oldInput = getCorRel(corRef);
      assert oldInput != null;
      final Frame frame = getOrCreateFrame(oldInput);
      final RelNode newInput = frame.r;
      assert newInput != null;

      final List<Integer> newLocalOutputs = requireNonNull(mapNewInputToOutputs.get(newInput),
          () -> "mapNewInputToOutputs.get(" + newInput + ")");

      final int newLocalOutput = frame.oldToNewOutputs.get(corRef.field);

      // newOutput is the index of the corVar in the referenced
      // position list plus the offset of referenced position list of
      // each newInput.
      final int newOutput =
          newLocalOutputs.indexOf(newLocalOutput)
              + requireNonNull(mapNewInputToNewOffset.get(newInput),
                  () -> "mapNewInputToNewOffset.get(" + newInput + ")")
              + valueGenFieldOffset;

      corDefOutputs.put(corRef.def(), newOutput);
    }

    return r;
  }

  private Frame getOrCreateFrame(RelNode r) {
    final Frame frame = getFrame(r);
    if (frame == null) {
      return new Frame(r, r, ImmutableSortedMap.of(),
          identityMap(r.getRowType().getFieldCount()));
    }
    return frame;
  }

  private @Nullable Frame getFrame(RelNode r) {
    return map.get(r);
  }

  private RelNode getCorRel(CorRef corVar) {
    final RelNode r = requireNonNull(cm.mapCorToCorRel.get(corVar.corr),
        () -> "cm.mapCorToCorRel.get(" + corVar.corr + ")");
    return requireNonNull(r.getInput(0),
        () -> "r.getInput(0) is null for " + r);
  }

  /** Adds a value generator to satisfy the correlating variables used by
   * a relational expression, if those variables are not already provided by
   * its input. */
  private Frame maybeAddValueGenerator(RelNode rel, Frame frame) {
    final CorelMap cm1 = CorelMap.build(frame.r, rel);
    if (!cm1.mapRefRelToCorRef.containsKey(rel)) {
      return frame;
    }
    final Collection<CorRef> needs = cm1.mapRefRelToCorRef.get(rel);
    final ImmutableSortedSet<CorDef> haves = frame.corDefOutputs.keySet();
    if (hasAll(needs, haves)) {
      return frame;
    }
    return decorrelateInputWithValueGenerator(rel, frame);
  }

  /** Returns whether all of a collection of {@link CorRef}s are satisfied
   * by at least one of a collection of {@link CorDef}s. */
  private static boolean hasAll(Collection<CorRef> corRefs,
      Collection<CorDef> corDefs) {
    for (CorRef corRef : corRefs) {
      if (!has(corDefs, corRef)) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether a {@link CorrelationId} is satisfied by at least one of a
   * collection of {@link CorDef}s. */
  private static boolean has(Collection<CorDef> corDefs, CorRef corr) {
    for (CorDef corDef : corDefs) {
      if (corDef.corr.equals(corr.corr) && corDef.field == corr.field) {
        return true;
      }
    }
    return false;
  }

  private Frame decorrelateInputWithValueGenerator(RelNode rel, Frame frame) {
    // currently only handles one input
    assert rel.getInputs().size() == 1;
    RelNode oldInput = frame.r;

    final NavigableMap<CorDef, Integer> corDefOutputs =
        new TreeMap<>(frame.corDefOutputs);

    final Collection<CorRef> corVarList = cm.mapRefRelToCorRef.get(rel);

    // Try to populate correlation variables using local fields.
    // This means that we do not need a value generator.
    if (rel instanceof Filter) {
      NavigableMap<CorDef, Integer> map = new TreeMap<>();
      List<RexNode> projects = new ArrayList<>();
      for (CorRef correlation : corVarList) {
        final CorDef def = correlation.def();
        if (corDefOutputs.containsKey(def) || map.containsKey(def)) {
          continue;
        }
        try {
          findCorrelationEquivalent(correlation, ((Filter) rel).getCondition());
        } catch (Util.FoundOne e) {
          Object node = requireNonNull(e.getNode(), "e.getNode()");
          if (node instanceof RexInputRef) {
            map.put(def, ((RexInputRef) node).getIndex());
          } else {
            map.put(def,
                frame.r.getRowType().getFieldCount() + projects.size());
            projects.add((RexNode) node);
          }
        }
      }
      // If all correlation variables are now satisfied, skip creating a value
      // generator.
      if (map.size() == corVarList.size()) {
        map.putAll(frame.corDefOutputs);
        final RelNode r;
        if (!projects.isEmpty()) {
          relBuilder.push(oldInput)
              .project(Iterables.concat(relBuilder.fields(), projects));
          r = relBuilder.build();
        } else {
          r = oldInput;
        }
        return register(rel.getInput(0), r,
            frame.oldToNewOutputs, map);
      }
    }

    int leftInputOutputCount = frame.r.getRowType().getFieldCount();

    // can directly add positions into corDefOutputs since join
    // does not change the output ordering from the inputs.
    RelNode valueGen = requireNonNull(
        createValueGenerator(corVarList, leftInputOutputCount, corDefOutputs),
        "createValueGenerator(...) is null");

    RelNode join = relBuilder.push(frame.r).push(valueGen)
        .join(JoinRelType.INNER, relBuilder.literal(true),
        ImmutableSet.of()).build();

    // Join or Filter does not change the old input ordering. All
    // input fields from newLeftInput (i.e. the original input to the old
    // Filter) are in the output and in the same position.
    return register(rel.getInput(0), join, frame.oldToNewOutputs,
        corDefOutputs);
  }

  /** Finds a {@link RexInputRef} that is equivalent to a {@link CorRef},
   * and if found, throws a {@link org.apache.calcite.util.Util.FoundOne}. */
  private static void findCorrelationEquivalent(CorRef correlation, RexNode e)
      throws Util.FoundOne {
    switch (e.getKind()) {
    case EQUALS:
      final RexCall call = (RexCall) e;
      final List<RexNode> operands = call.getOperands();
      if (references(operands.get(0), correlation)) {
        throw new Util.FoundOne(operands.get(1));
      }
      if (references(operands.get(1), correlation)) {
        throw new Util.FoundOne(operands.get(0));
      }
      break;
    case AND:
      for (RexNode operand : ((RexCall) e).getOperands()) {
        findCorrelationEquivalent(correlation, operand);
      }
      break;
    default:
      break;
    }
  }

  private static boolean references(RexNode e, CorRef correlation) {
    switch (e.getKind()) {
    case CAST:
      final RexNode operand = ((RexCall) e).getOperands().get(0);
      if (isWidening(e.getType(), operand.getType())) {
        return references(operand, correlation);
      }
      return false;
    case FIELD_ACCESS:
      final RexFieldAccess f = (RexFieldAccess) e;
      if (f.getField().getIndex() == correlation.field
          && f.getReferenceExpr() instanceof RexCorrelVariable) {
        if (((RexCorrelVariable) f.getReferenceExpr()).id == correlation.corr) {
          return true;
        }
      }
      // fall through
    default:
      return false;
    }
  }

  /** Returns whether one type is just a widening of another.
   *
   * <p>For example:<ul>
   * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(5)}.
   * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(10) NOT NULL}.
   * </ul>
   */
  private static boolean isWidening(RelDataType type, RelDataType type1) {
    return type.getSqlTypeName() == type1.getSqlTypeName()
        && type.getPrecision() >= type1.getPrecision();
  }

  public @Nullable Frame decorrelateRel(LogicalSnapshot rel, boolean isCorVarDefined) {
    if (RexUtil.containsCorrelation(rel.getPeriod())) {
      return null;
    }
    return decorrelateRel((RelNode) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(LogicalTableFunctionScan rel, boolean isCorVarDefined) {
    if (RexUtil.containsCorrelation(rel.getCall())) {
      return null;
    }
    return decorrelateRel((RelNode) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(LogicalFilter rel, boolean isCorVarDefined) {
    return decorrelateRel((Filter) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(Filter rel, boolean isCorVarDefined) {
    //
    // Rewrite logic:
    //
    // 1. If a Filter references a correlated field in its filter
    // condition, rewrite the Filter to be
    //   Filter
    //     Join(cross product)
    //       originalFilterInput
    //       ValueGenerator(produces distinct sets of correlated variables)
    // and rewrite the correlated fieldAccess in the filter condition to
    // reference the Join output.
    //
    // 2. If Filter does not reference correlated variables, simply
    // rewrite the filter condition using new input.
    //

    final RelNode oldInput = rel.getInput();
    Frame frame = getInvoke(oldInput, isCorVarDefined, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }

    // If this Filter has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (false) {
      if (cm.mapRefRelToCorRef.containsKey(rel)) {
        frame = decorrelateInputWithValueGenerator(rel, frame);
      }
    } else {
      frame = maybeAddValueGenerator(rel, frame);
    }

    final CorelMap cm2 = CorelMap.build(rel);

    // Replace the filter expression to reference output of the join
    // Map filter to the new filter over join
    relBuilder.push(frame.r)
        .filter(decorrelateExpr(castNonNull(currentRel), map, cm2, rel.getCondition()));

    // Filter does not change the input ordering.
    // Filter rel does not permute the input.
    // All corVars produced by filter will have the same output positions in the
    // input rel.
    return register(rel, relBuilder.build(), frame.oldToNewOutputs,
        frame.corDefOutputs);
  }

  public @Nullable Frame decorrelateRel(LogicalCorrelate rel, boolean isCorVarDefined) {
    return decorrelateRel((Correlate) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(Correlate rel, boolean isCorVarDefined) {
    //
    // Rewrite logic:
    //
    // The original left input will be joined with the new right input that
    // has generated correlated variables propagated up. For any generated
    // corVars that are not used in the join key, pass them along to be
    // joined later with the Correlates that produce them.
    //

    // the right input to Correlate should produce correlated variables
    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    final Frame leftFrame = getInvoke(oldLeft, isCorVarDefined, rel);
    final Frame rightFrame = getInvoke(oldRight, true, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    if (rightFrame.corDefOutputs.isEmpty()) {
      return null;
    }

    assert rel.getRequiredColumns().cardinality()
        <= rightFrame.corDefOutputs.keySet().size();

    // Change correlator rel into a join.
    // Join all the correlated variables produced by this correlator rel
    // with the values generated and propagated from the right input
    final NavigableMap<CorDef, Integer> corDefOutputs =
        new TreeMap<>(rightFrame.corDefOutputs);
    final List<RexNode> conditions = new ArrayList<>();
    final List<RelDataTypeField> newLeftOutput =
        leftFrame.r.getRowType().getFieldList();
    int newLeftFieldCount = newLeftOutput.size();

    final List<RelDataTypeField> newRightOutput =
        rightFrame.r.getRowType().getFieldList();

    for (Map.Entry<CorDef, Integer> rightOutput
        : new ArrayList<>(corDefOutputs.entrySet())) {
      final CorDef corDef = rightOutput.getKey();
      if (!corDef.corr.equals(rel.getCorrelationId())) {
        continue;
      }
      final int newLeftPos = leftFrame.oldToNewOutputs.get(corDef.field);
      final int newRightPos = rightOutput.getValue();
      conditions.add(
          relBuilder.call(SqlStdOperatorTable.EQUALS,
              RexInputRef.of(newLeftPos, newLeftOutput),
              new RexInputRef(newLeftFieldCount + newRightPos,
                  newRightOutput.get(newRightPos).getType())));

      // remove this corVar from output position mapping
      corDefOutputs.remove(corDef);
    }

    // Update the output position for the corVars: only pass on the cor
    // vars that are not used in the join key.
    for (Map.Entry<CorDef, Integer> entry : corDefOutputs.entrySet()) {
      entry.setValue(entry.getValue() + newLeftFieldCount);
    }

    // then add any corVar from the left input. Do not need to change
    // output positions.
    corDefOutputs.putAll(leftFrame.corDefOutputs);

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    //noinspection AssertWithSideEffects
    assert rel.getRowType().getFieldCount()
        == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(i + oldLeftFieldCount,
          rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final RexNode condition =
        RexUtil.composeConjunction(relBuilder.getRexBuilder(), conditions);
    RelNode newJoin = relBuilder.push(leftFrame.r).push(rightFrame.r)
        .join(rel.getJoinType(), condition).build();

    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  public @Nullable Frame decorrelateRel(LogicalJoin rel, boolean isCorVarDefined) {
    return decorrelateRel((Join) rel, isCorVarDefined);
  }

  public @Nullable Frame decorrelateRel(Join rel, boolean isCorVarDefined) {
    // For SEMI/ANTI join decorrelate it's input directly,
    // because the correlate variables can only be propagated from
    // the left side, which is not supported yet.
    if (!rel.getJoinType().projectsRight()) {
      return decorrelateRel((RelNode) rel, isCorVarDefined);
    }
    //
    // Rewrite logic:
    //
    // 1. rewrite join condition.
    // 2. map output positions and produce corVars if any.
    //

    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    final Frame leftFrame = getInvoke(oldLeft, isCorVarDefined, rel);
    final Frame rightFrame = getInvoke(oldRight, isCorVarDefined, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    RelNode newJoin = relBuilder
        .push(leftFrame.r)
        .push(rightFrame.r)
        .join(rel.getJoinType(),
            decorrelateExpr(castNonNull(currentRel), map, cm, rel.getCondition()),
            ImmutableSet.of())
        .hints(rel.getHints())
        .build();

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
    int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    //noinspection AssertWithSideEffects
    assert rel.getRowType().getFieldCount()
        == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(i + oldLeftFieldCount,
          rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final NavigableMap<CorDef, Integer> corDefOutputs =
        new TreeMap<>(leftFrame.corDefOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (Map.Entry<CorDef, Integer> entry
        : rightFrame.corDefOutputs.entrySet()) {
      corDefOutputs.put(entry.getKey(),
          entry.getValue() + newLeftFieldCount);
    }
    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  private static RexInputRef getNewForOldInputRef(RelNode currentRel,
      Map<RelNode, Frame> map, RexInputRef oldInputRef) {
    assert currentRel != null;

    int oldOrdinal = oldInputRef.getIndex();
    int newOrdinal = 0;

    // determine which input rel oldOrdinal references, and adjust
    // oldOrdinal to be relative to that input rel
    RelNode oldInput = null;

    for (RelNode oldInput0 : currentRel.getInputs()) {
      RelDataType oldInputType = oldInput0.getRowType();
      int n = oldInputType.getFieldCount();
      if (oldOrdinal < n) {
        oldInput = oldInput0;
        break;
      }
      RelNode newInput = requireNonNull(map.get(oldInput0),
          () -> "map.get(oldInput0) for " + oldInput0).r;
      newOrdinal += newInput.getRowType().getFieldCount();
      oldOrdinal -= n;
    }

    assert oldInput != null;

    final Frame frame = map.get(oldInput);
    assert frame != null;

    // now oldOrdinal is relative to oldInput
    int oldLocalOrdinal = oldOrdinal;

    // figure out the newLocalOrdinal, relative to the newInput.
    int newLocalOrdinal = oldLocalOrdinal;

    if (!frame.oldToNewOutputs.isEmpty()) {
      newLocalOrdinal = frame.oldToNewOutputs.get(oldLocalOrdinal);
    }

    newOrdinal += newLocalOrdinal;

    return new RexInputRef(newOrdinal,
        frame.r.getRowType().getFieldList().get(newLocalOrdinal).getType());
  }

  /**
   * Pulls project above the join from its RHS input. Enforces nullability
   * for join output.
   *
   * @param join          Join
   * @param project       Original project as the right-hand input of the join
   * @param nullIndicatorPos Position of null indicator
   * @return the subtree with the new Project at the root
   */
  RelNode projectJoinOutputWithNullability(
      Join join,
      Project project,
      int nullIndicatorPos) {
    final RelDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
    final RelNode left = join.getLeft();
    final JoinRelType joinType = join.getJoinType();

    RexInputRef nullIndicator =
        new RexInputRef(
            nullIndicatorPos,
            typeFactory.createTypeWithNullability(
                join.getRowType().getFieldList().get(nullIndicatorPos)
                    .getType(),
                true));

    // now create the new project
    List<Pair<RexNode, String>> newProjExprs = new ArrayList<>();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
        left.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjExprs.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
        joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      RexNode newProjExpr =
          DecorrelatorUtil.removeCorrelationExpr(
              RelDecorrelator.this,
              pair.left,
              projectPulledAboveLeftCorrelator,
              nullIndicator);

      newProjExprs.add(Pair.of(newProjExpr, pair.right));
    }

    return relBuilder.push(join)
        .projectNamed(Pair.left(newProjExprs), Pair.right(newProjExprs), true)
        .build();
  }

  /**
   * Pulls a {@link Project} above a {@link Correlate} from its RHS input.
   * Enforces nullability for join output.
   *
   * @param correlate  Correlate
   * @param project the original project as the RHS input of the join
   * @param isCount Positions which are calls to the <code>COUNT</code>
   *                aggregation function
   * @return the subtree with the new Project at the root
   */
  RelNode aggregateCorrelatorOutput(
      Correlate correlate,
      Project project,
      Set<Integer> isCount) {
    final RelNode left = correlate.getLeft();
    final JoinRelType joinType = correlate.getJoinType();

    // now create the new project
    final List<Pair<RexNode, String>> newProjects = new ArrayList<>();

    // Project everything from the LHS and then those from the original
    // project
    final List<RelDataTypeField> leftInputFields =
        left.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjects.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
        joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      RexNode newProjExpr =
          DecorrelatorUtil.removeCorrelationExpr(
              this,
              pair.left,
              projectPulledAboveLeftCorrelator,
              isCount);
      newProjects.add(Pair.of(newProjExpr, pair.right));
    }

    return relBuilder.push(correlate)
        .projectNamed(Pair.left(newProjects), Pair.right(newProjects), true)
        .build();
  }

  /**
   * Checks whether the correlations in projRel and filter are related to
   * the correlated variables provided by corRel.
   *
   * @param correlate    Correlate
   * @param project   The original Project as the RHS input of the join
   * @param filter    Filter
   * @param correlatedJoinKeys Correlated join keys
   * @return true if filter and proj only references corVar provided by corRel
   */
  boolean checkCorVars(
      Correlate correlate,
      @Nullable Project project,
      @Nullable Filter filter,
      @Nullable List<RexFieldAccess> correlatedJoinKeys) {
    if (filter != null) {
      assert correlatedJoinKeys != null;

      // check that all correlated refs in the filter condition are
      // used in the join(as field access).
      Set<CorRef> corVarInFilter =
          Sets.newHashSet(cm.mapRefRelToCorRef.get(filter));

      for (RexFieldAccess correlatedJoinKey : correlatedJoinKeys) {
        corVarInFilter.remove(cm.mapFieldAccessToCorRef.get(correlatedJoinKey));
      }

      if (!corVarInFilter.isEmpty()) {
        return false;
      }

      // Check that the correlated variables referenced in these
      // comparisons do come from the Correlate.
      corVarInFilter.addAll(cm.mapRefRelToCorRef.get(filter));

      for (CorRef corVar : corVarInFilter) {
        if (cm.mapCorToCorRel.get(corVar.corr) != correlate) {
          return false;
        }
      }
    }

    // if project has any correlated reference, make sure they are also
    // provided by the current correlate. They will be projected out of the LHS
    // of the correlate.
    if ((project != null) && cm.mapRefRelToCorRef.containsKey(project)) {
      for (CorRef corVar : cm.mapRefRelToCorRef.get(project)) {
        if (cm.mapCorToCorRel.get(corVar.corr) != correlate) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Removes correlated variables from the tree at root corRel.
   *
   * @param correlate Correlate
   */
  void removeCorVarFromTree(Correlate correlate) {
    cm.mapCorToCorRel.remove(correlate.getCorrelationId(), correlate);
  }

  /**
   * Projects all {@code input} output fields plus the additional expressions.
   *
   * @param input        Input relational expression
   * @param additionalExprs Additional expressions and names
   * @return the new Project
   */
  RelNode createProjectWithAdditionalExprs(
      RelNode input,
      List<Pair<RexNode, String>> additionalExprs) {
    final List<RelDataTypeField> fieldList =
        input.getRowType().getFieldList();
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    Ord.forEach(fieldList, (field, i) ->
        projects.add(
            Pair.of(relBuilder.getRexBuilder().makeInputRef(field.getType(), i),
                field.getName())));
    projects.addAll(additionalExprs);
    return relBuilder.push(input)
        .projectNamed(Pair.left(projects), Pair.right(projects), true)
        .build();
  }

  /* Returns an immutable map with the identity [0: 0, .., count-1: count-1]. */
  static Map<Integer, Integer> identityMap(int count) {
    ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
    for (int i = 0; i < count; i++) {
      builder.put(i, i);
    }
    return builder.build();
  }

  /** Registers a relational expression and the relational expression it became
   * after decorrelation. */
  Frame register(RelNode rel, RelNode newRel,
      Map<Integer, Integer> oldToNewOutputs,
      NavigableMap<CorDef, Integer> corDefOutputs) {
    final Frame frame = new Frame(rel, newRel, corDefOutputs, oldToNewOutputs);
    map.put(rel, frame);
    return frame;
  }

  static boolean allLessThan(Collection<Integer> integers, int limit,
      Litmus ret) {
    for (int value : integers) {
      if (value >= limit) {
        return ret.fail("out of range; value: {}, limit: {}", value, limit);
      }
    }
    return ret.succeed();
  }


  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that decorrelates. */
  private static class DecorrelateRexShuttle extends RexShuttle {
    private final RelNode currentRel;
    private final Map<RelNode, Frame> map;
    private final CorelMap cm;

    private DecorrelateRexShuttle(RelNode currentRel,
        Map<RelNode, Frame> map, CorelMap cm) {
      this.currentRel = requireNonNull(currentRel, "currentRel");
      this.map = requireNonNull(map, "map");
      this.cm = requireNonNull(cm, "cm");
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      int newInputOutputOffset = 0;
      for (RelNode input : currentRel.getInputs()) {
        final Frame frame = map.get(input);

        if (frame != null) {
          // try to find in this input rel the position of corVar
          final CorRef corRef = cm.mapFieldAccessToCorRef.get(fieldAccess);

          if (corRef != null) {
            Integer newInputPos = frame.corDefOutputs.get(corRef.def());
            if (newInputPos != null) {
              // This input does produce the corVar referenced.
              return new RexInputRef(newInputPos + newInputOutputOffset,
                  frame.r.getRowType().getFieldList().get(newInputPos)
                      .getType());
            }
          }

          // this input does not produce the corVar needed
          newInputOutputOffset += frame.r.getRowType().getFieldCount();
        } else {
          // this input is not rewritten
          newInputOutputOffset += input.getRowType().getFieldCount();
        }
      }
      return fieldAccess;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      final RexInputRef ref = getNewForOldInputRef(currentRel, map, inputRef);
      if (ref.getIndex() == inputRef.getIndex()
          && ref.getType() == inputRef.getType()) {
        return inputRef; // re-use old object, to prevent needless expr cloning
      }
      return ref;
    }
  }

  // REVIEW jvs 29-Oct-2007:  Shouldn't it also be incorporating
  // the flavor attribute into the description?

  /**
   * A unique reference to a correlation field.
   *
   * <p>For instance, if a RelNode references emp.name multiple times, it would
   * result in multiple {@code CorRef} objects that differ just in
   * {@link CorRef#uniqueKey}.
   */
  static class CorRef implements Comparable<CorRef> {
    public final int uniqueKey;
    public final CorrelationId corr;
    public final int field;

    CorRef(CorrelationId corr, int field, int uniqueKey) {
      this.corr = corr;
      this.field = field;
      this.uniqueKey = uniqueKey;
    }

    @Override public String toString() {
      return corr.getName() + '.' + field;
    }

    @Override public int hashCode() {
      return Objects.hash(uniqueKey, corr, field);
    }

    @Override public boolean equals(@Nullable Object o) {
      return this == o
          || o instanceof CorRef
          && uniqueKey == ((CorRef) o).uniqueKey
          && corr == ((CorRef) o).corr
          && field == ((CorRef) o).field;
    }

    @Override public int compareTo(CorRef o) {
      int c = corr.compareTo(o.corr);
      if (c != 0) {
        return c;
      }
      c = Integer.compare(field, o.field);
      if (c != 0) {
        return c;
      }
      return Integer.compare(uniqueKey, o.uniqueKey);
    }

    public CorDef def() {
      return new CorDef(corr, field);
    }
  }

  /** A correlation and a field. */
  static class CorDef implements Comparable<CorDef> {
    public final CorrelationId corr;
    public final int field;

    CorDef(CorrelationId corr, int field) {
      this.corr = corr;
      this.field = field;
    }

    @Override public String toString() {
      return corr.getName() + '.' + field;
    }

    @Override public int hashCode() {
      return Objects.hash(corr, field);
    }

    @Override public boolean equals(@Nullable Object o) {
      return this == o
          || o instanceof CorDef
          && corr == ((CorDef) o).corr
          && field == ((CorDef) o).field;
    }

    @Override public int compareTo(CorDef o) {
      int c = corr.compareTo(o.corr);
      if (c != 0) {
        return c;
      }
      return Integer.compare(field, o.field);
    }
  }

  /** Frame describing the relational expression after decorrelation
   * and where to find the output fields and correlation variables
   * among its output fields. */
  static class Frame {
    final RelNode r;
    final ImmutableSortedMap<CorDef, Integer> corDefOutputs;
    final ImmutableSortedMap<Integer, Integer> oldToNewOutputs;

    Frame(RelNode oldRel, RelNode r, NavigableMap<CorDef, Integer> corDefOutputs,
        Map<Integer, Integer> oldToNewOutputs) {
      this.r = requireNonNull(r, "r");
      this.corDefOutputs = ImmutableSortedMap.copyOf(corDefOutputs);
      this.oldToNewOutputs = ImmutableSortedMap.copyOf(oldToNewOutputs);
      assert allLessThan(this.corDefOutputs.values(),
          r.getRowType().getFieldCount(), Litmus.THROW);
      assert allLessThan(this.oldToNewOutputs.keySet(),
          oldRel.getRowType().getFieldCount(), Litmus.THROW);
      assert allLessThan(this.oldToNewOutputs.values(),
          r.getRowType().getFieldCount(), Litmus.THROW);
    }
  }

  /** Base configuration for rules that are non-static in a RelDecorrelator. */
  public interface Config extends RelRule.Config {
    /** Returns the RelDecorrelator that will be context for the created
     * rule instance. */
    @ImmutableBeans.Property
    RelDecorrelator decorrelator();

    /** Sets {@link #decorrelator}. */
    Config withDecorrelator(RelDecorrelator decorrelator);
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  /**
   * Returns the {@code visitor} on which the {@code MethodDispatcher} dispatches
   * each {@code decorrelateRel} method, the default implementation returns this instance,
   * if you got a sub-class, override this method to replace the {@code visitor} as the
   * sub-class instance.
   */
  protected RelDecorrelator getVisitor() {
    return this;
  }

  /** Returns the rules applied on the rel after decorrelation, never null. */
  protected Collection<RelOptRule> getPostDecorrelateRules() {
    return Collections.emptyList();
  }
}
