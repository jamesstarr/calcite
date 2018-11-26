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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
          RexUtil.composeConjunction(rexBuilder, residualPredicates, false));
      // Add those that are not part of the final set to residual
      for (RexNode e : finalPredicates) {
        if (!predicates.contains(e)) {
          // This node was in previous union inputs, but it is not in this one
          for (int j = 0; j < input.i; j++) {
            finalResidualPredicates.set(j,
                RexUtil.composeConjunction(rexBuilder,
                    Arrays.asList(finalResidualPredicates.get(j), e), false));
          }
        }
      }
      // Final predicates
      finalPredicates = predicates;
    }

    final List<RexNode> predicates = new ArrayList<>(finalPredicates);
    final RelOptCluster cluster = union.getCluster();
    final RexExecutor executor =
        Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    RexNode disjunctivePredicate =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, executor)
            .simplifyOrs(finalResidualPredicates);
    if (!disjunctivePredicate.isAlwaysTrue()) {
      predicates.add(disjunctivePredicate);
    }
    return RelOptPredicateList.of(rexBuilder, predicates);
  }

  /**
   * Infers predicates for a Intersect.
   */
  public RelOptPredicateList getPredicates(Intersect intersect, RelMetadataQuery mq) {
    final RexBuilder rexBuilder = intersect.getCluster().getRexBuilder();

    final RexExecutor executor =
        Util.first(intersect.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);

    final RexImplicationChecker rexImplicationChecker =
        new RexImplicationChecker(rexBuilder, executor, intersect.getRowType());

    Set<RexNode> finalPredicates = new HashSet<>();

    for (Ord<RelNode> input : Ord.zip(intersect.getInputs())) {
      RelOptPredicateList info = mq.getPulledUpPredicates(input.e);
      if (info == null || info.pulledUpPredicates.isEmpty()) {
        continue;
      }

      for (RexNode pred: info.pulledUpPredicates) {
        if (finalPredicates.stream().anyMatch(
            finalPred -> rexImplicationChecker.implies(finalPred, pred))) {
          // There's already a stricter predicate in finalPredicates,
          // thus no need to count this one.
          continue;
        }
        // Remove looser predicate and add this one into finalPredicates
        finalPredicates = finalPredicates.stream()
            .filter(finalPred -> !rexImplicationChecker.implies(pred, finalPred))
            .collect(Collectors.toSet());
        finalPredicates.add(pred);
      }
    }

    return RelOptPredicateList.of(rexBuilder, finalPredicates);
  }

  /**
   * Infers predicates for a Minus.
   */
  public RelOptPredicateList getPredicates(Minus minus, RelMetadataQuery mq) {
    return mq.getPulledUpPredicates(minus.getInput(0));
  }


  /**
   * Infers predicates for a Sort.
   */
  public RelOptPredicateList getPredicates(Sort sort, RelMetadataQuery mq) {
    RelNode input = sort.getInput();
    return mq.getPulledUpPredicates(input);
  }

  /**
   * Infers predicates for a TableModify.
   */
  public RelOptPredicateList getPredicates(TableModify tableModify, RelMetadataQuery mq) {
    return mq.getPulledUpPredicates(tableModify.getInput());
  }

  /**
   * Infers predicates for an Exchange.
   */
  public RelOptPredicateList getPredicates(Exchange exchange,
      RelMetadataQuery mq) {
    RelNode input = exchange.getInput();
    return mq.getPulledUpPredicates(input);
  }

  /** @see RelMetadataQuery#getPulledUpPredicates(RelNode) */
  public RelOptPredicateList getPredicates(RelSubset r,
      RelMetadataQuery mq) {
    if (!Bug.CALCITE_1048_FIXED) {
      return RelOptPredicateList.EMPTY;
    }
    final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
    RelOptPredicateList list = null;
    for (RelNode r2 : r.getRels()) {
      RelOptPredicateList list2 = mq.getPulledUpPredicates(r2);
      if (list2 != null) {
        list = list == null ? list2 : list.union(rexBuilder, list2);
      }
    }
    return Util.first(list, RelOptPredicateList.EMPTY);
  }

  /**
   * Utility to infer predicates from one side of the join that apply on the
   * other side.
   *
   * <p>Contract is:<ul>
   *
   * <li>initialize with a {@link org.apache.calcite.rel.core.Join} and
   * optional predicates applicable on its left and right subtrees.
   *
   * <li>you can
   * then ask it for equivalentPredicate(s) given a predicate.
   *
   * </ul>
   *
   * <p>So for:
   * <ol>
   * <li>'<code>R1(x) join R2(y) on x = y</code>' a call for
   * equivalentPredicates on '<code>x &gt; 7</code>' will return '
   * <code>[y &gt; 7]</code>'
   * <li>'<code>R1(x) join R2(y) on x = y join R3(z) on y = z</code>' a call for
   * equivalentPredicates on the second join '<code>x &gt; 7</code>' will return
   * </ol>
   */
  static class JoinConditionBasedPredicateInference {
    final Join joinRel;
    final int nSysFields;
    final int nFieldsLeft;
    final int nFieldsRight;
    final ImmutableBitSet leftFieldsBitSet;
    final ImmutableBitSet rightFieldsBitSet;
    final ImmutableBitSet allFieldsBitSet;
    SortedMap<Integer, BitSet> equivalence;
    final Map<RexNode, ImmutableBitSet> exprFields;
    final Set<RexNode> allExprs;
    final Set<RexNode> equalityPredicates;
    final RexNode leftChildPredicates;
    final RexNode rightChildPredicates;
    final RexSimplify simplify;

    JoinConditionBasedPredicateInference(Join joinRel,
        RexNode leftPredicates, RexNode rightPredicates) {
      this(joinRel, joinRel instanceof SemiJoin, leftPredicates, rightPredicates);
    }

    private JoinConditionBasedPredicateInference(Join joinRel, boolean isSemiJoin,
        RexNode leftPredicates, RexNode rightPredicates) {
      super();
      this.joinRel = joinRel;
      this.simplify = simplify;
      nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
      nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
      nSysFields = joinRel.getSystemFieldList().size();
      leftFieldsBitSet = ImmutableBitSet.range(nSysFields,
          nSysFields + nFieldsLeft);
      rightFieldsBitSet = ImmutableBitSet.range(nSysFields + nFieldsLeft,
          nSysFields + nFieldsLeft + nFieldsRight);
      allFieldsBitSet = ImmutableBitSet.range(0,
          nSysFields + nFieldsLeft + nFieldsRight);

      exprFields = new HashMap<>();
      allExprs = new HashSet<>();

      if (leftPredicates == null) {
        leftChildPredicates = null;
      } else {
        Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft, nSysFields, 0, nFieldsLeft);
        leftChildPredicates = leftPredicates.accept(
            new RexPermuteInputsShuttle(leftMapping, joinRel.getInput(0)));

        allExprs.add(leftChildPredicates);
        for (RexNode r : RelOptUtil.conjunctions(leftChildPredicates)) {
          exprFields.put(r, RelOptUtil.InputFinder.bits(r));
          allExprs.add(r);
        }
      }
      if (rightPredicates == null) {
        rightChildPredicates = null;
      } else {
        Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft, 0, nFieldsRight);
        rightChildPredicates = rightPredicates.accept(
            new RexPermuteInputsShuttle(rightMapping, joinRel.getInput(1)));

        allExprs.add(rightChildPredicates);
        for (RexNode r : RelOptUtil.conjunctions(rightChildPredicates)) {
          exprFields.put(r, RelOptUtil.InputFinder.bits(r));
          allExprs.add(r);
        }
      }

      equivalence = new TreeMap();
      equalityPredicates = new HashSet<>();
      for (int i = 0; i < nSysFields + nFieldsLeft + nFieldsRight; i++) {
        equivalence.put(i, BitSets.of(i));
      }

      // Only process equivalences found in the join conditions. Processing
      // Equivalences from the left or right side infer predicates that are
      // already present in the Tree below the join.
      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      List<RexNode> exprs =
          RelOptUtil.conjunctions(
              compose(rexBuilder, ImmutableList.of(joinRel.getCondition())));

      final EquivalenceFinder eF = new EquivalenceFinder();
      exprs.forEach(input -> input.accept(eF));

      equivalence = BitSets.closure(equivalence);
    }

    /**
     * The PullUp Strategy is sound but not complete.
     * <ol>
     * <li>We only pullUp inferred predicates for now. Pulling up existing
     * predicates causes an explosion of duplicates. The existing predicates are
     * pushed back down as new predicates. Once we have rules to eliminate
     * duplicate Filter conditions, we should pullUp all predicates.
     * <li>For Left Outer: we infer new predicates from the left and set them as
     * applicable on the Right side. No predicates are pulledUp.
     * <li>Right Outer Joins are handled in an analogous manner.
     * <li>For Full Outer Joins no predicates are pulledUp or inferred.
     * </ol>
     */
    public RelOptPredicateList inferPredicates(
        boolean includeEqualityInference) {
      final List<RexNode> inferredPredicates = new ArrayList<>();
      final Set<RexNode> allExprs = new HashSet<>(this.allExprs);
      final JoinRelType joinType = joinRel.getJoinType();
      switch (joinType) {
      case SEMI:
      case INNER:
      case LEFT:
        infer(leftChildPredicates, allExprs, inferredPredicates,
            includeEqualityInference,
            joinType == JoinRelType.LEFT ? rightFieldsBitSet
                : allFieldsBitSet);
        break;
      }
      switch (joinType) {
      case SEMI:
      case INNER:
      case RIGHT:
        infer(rightChildPredicates, allExprs, inferredPredicates,
            includeEqualityInference,
            joinType == JoinRelType.RIGHT ? leftFieldsBitSet
                : allFieldsBitSet);
        break;
      }

      Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft + nFieldsRight,
          0, nSysFields + nFieldsLeft, nFieldsRight);
      final RexPermuteInputsShuttle rightPermute =
          new RexPermuteInputsShuttle(rightMapping, joinRel);
      Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft, 0, nSysFields, nFieldsLeft);
      final RexPermuteInputsShuttle leftPermute =
          new RexPermuteInputsShuttle(leftMapping, joinRel);
      final List<RexNode> leftInferredPredicates = new ArrayList<>();
      final List<RexNode> rightInferredPredicates = new ArrayList<>();

      for (RexNode iP : inferredPredicates) {
        ImmutableBitSet iPBitSet = RelOptUtil.InputFinder.bits(iP);
        if (leftFieldsBitSet.contains(iPBitSet)) {
          leftInferredPredicates.add(iP.accept(leftPermute));
        } else if (rightFieldsBitSet.contains(iPBitSet)) {
          rightInferredPredicates.add(iP.accept(rightPermute));
        }
      }

      final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      switch (joinType) {
      case SEMI:
        Iterable<RexNode> pulledUpPredicates;
        pulledUpPredicates = Iterables.concat(
            RelOptUtil.conjunctions(leftChildPredicates),
            leftInferredPredicates);
        return RelOptPredicateList.of(rexBuilder, pulledUpPredicates,
            leftInferredPredicates, rightInferredPredicates);
      case INNER:
        pulledUpPredicates = Iterables.concat(
            RelOptUtil.conjunctions(leftChildPredicates),
            RelOptUtil.conjunctions(rightChildPredicates),
            RexUtil.retainDeterministic(
                RelOptUtil.conjunctions(joinRel.getCondition())),
            inferredPredicates);
        return RelOptPredicateList.of(rexBuilder, pulledUpPredicates,
          leftInferredPredicates, rightInferredPredicates);
      case LEFT:
        return RelOptPredicateList.of(rexBuilder,
            RelOptUtil.conjunctions(leftChildPredicates),
            leftInferredPredicates, rightInferredPredicates);
      case RIGHT:
        return RelOptPredicateList.of(rexBuilder,
            RelOptUtil.conjunctions(rightChildPredicates),
            inferredPredicates, EMPTY_LIST);
      default:
        assert inferredPredicates.size() == 0;
        return RelOptPredicateList.EMPTY;
      }
    }

    public RexNode left() {
      return leftChildPredicates;
    }

    public RexNode right() {
      return rightChildPredicates;
    }

    private void infer(RexNode predicates, Set<RexNode> allExprs,
        List<RexNode> inferredPredicates, boolean includeEqualityInference,
        ImmutableBitSet inferringFields) {
      for (RexNode r : RelOptUtil.conjunctions(predicates)) {
        if (!includeEqualityInference
            && equalityPredicates.contains(r)) {
          continue;
        }
        for (Mapping m : mappings(r)) {
          RexNode tr = r.accept(
              new RexPermuteInputsShuttle(m, joinRel.getInput(0),
                  joinRel.getInput(1)));
          if (inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
              && !allExprs.contains(tr)
              && !isAlwaysTrue(tr)) {
            inferredPredicates.add(tr);
            allExprs.add(tr);
          }
        }
      }
    }

    Iterable<Mapping> mappings(final RexNode predicate) {
      final ImmutableBitSet fields = exprFields.get(predicate);
      if (fields.cardinality() == 0) {
        return Collections.emptyList();
      }
      return () -> new ExprsItr(fields);
    }

    private boolean checkTarget(ImmutableBitSet inferringFields,
        Set<RexNode> allExprs, RexNode tr) {
      return inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
          && !allExprs.contains(tr)
          && !isAlwaysTrue(tr);
    }

    private void markAsEquivalent(int p1, int p2) {
      BitSet b = equivalence.get(p1);
      b.set(p2);

      b = equivalence.get(p2);
      b.set(p1);
    }

    RexNode compose(RexBuilder rexBuilder, Iterable<RexNode> exprs) {
      exprs = Linq4j.asEnumerable(exprs).where(Objects::nonNull);
      return RexUtil.composeConjunction(rexBuilder, exprs);
    }

    /**
     * Find expressions of the form 'col_x = col_y'.
     */
    class EquivalenceFinder extends RexVisitorImpl<Void> {
      protected EquivalenceFinder() {
        super(true);
      }

      @Override public Void visitCall(RexCall call) {
        if (call.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(call.getOperands().get(0));
          int rPos = pos(call.getOperands().get(1));
          if (lPos != -1 && rPos != -1) {
            markAsEquivalent(lPos, rPos);
            equalityPredicates.add(call);
          }
        }
        return null;
      }
    }

    /**
     * Given an expression returns all the possible substitutions.
     *
     * <p>For example, for an expression 'a + b + c' and the following
     * equivalences: <pre>
     * a : {a, b}
     * b : {a, b}
     * c : {c, e}
     * </pre>
     *
     * <p>The following Mappings will be returned:
     * <pre>
     * {a &rarr; a, b &rarr; a, c &rarr; c}
     * {a &rarr; a, b &rarr; a, c &rarr; e}
     * {a &rarr; a, b &rarr; b, c &rarr; c}
     * {a &rarr; a, b &rarr; b, c &rarr; e}
     * {a &rarr; b, b &rarr; a, c &rarr; c}
     * {a &rarr; b, b &rarr; a, c &rarr; e}
     * {a &rarr; b, b &rarr; b, c &rarr; c}
     * {a &rarr; b, b &rarr; b, c &rarr; e}
     * </pre>
     *
     * <p>which imply the following inferences:
     * <pre>
     * a + a + c
     * a + a + e
     * a + b + c
     * a + b + e
     * b + a + c
     * b + a + e
     * b + b + c
     * b + b + e
     * </pre>
     */
    class ExprsItr implements Iterator<Mapping> {
      final int[] columns;
      final BitSet[] columnSets;
      final int[] iterationIdx;
      Mapping nextMapping;
      boolean firstCall;

      ExprsItr(ImmutableBitSet fields) {
        nextMapping = null;
        columns = new int[fields.cardinality()];
        columnSets = new BitSet[fields.cardinality()];
        iterationIdx = new int[fields.cardinality()];
        for (int j = 0, i = fields.nextSetBit(0); i >= 0; i = fields
            .nextSetBit(i + 1), j++) {
          columns[j] = i;
          columnSets[j] = equivalence.get(i);
          iterationIdx[j] = 0;
        }
        firstCall = true;
      }

      public boolean hasNext() {
        if (firstCall) {
          initializeMapping();
          firstCall = false;
        } else {
          computeNextMapping(iterationIdx.length - 1);
        }
        return nextMapping != null;
      }

      public Mapping next() {
        return nextMapping;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      private void computeNextMapping(int level) {
        int t = columnSets[level].nextSetBit(iterationIdx[level]);
        if (t < 0) {
          if (level == 0) {
            nextMapping = null;
          } else {
            int tmp = columnSets[level].nextSetBit(0);
            nextMapping.set(columns[level], tmp);
            iterationIdx[level] = tmp + 1;
            computeNextMapping(level - 1);
          }
        } else {
          nextMapping.set(columns[level], t);
          iterationIdx[level] = t + 1;
        }
      }

      private void initializeMapping() {
        nextMapping = Mappings.create(MappingType.PARTIAL_FUNCTION,
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft + nFieldsRight);
        for (int i = 0; i < columnSets.length; i++) {
          BitSet c = columnSets[i];
          int t = c.nextSetBit(iterationIdx[i]);
          if (t < 0) {
            nextMapping = null;
            return;
          }
          nextMapping.set(columns[i], t);
          iterationIdx[i] = t + 1;
        }
      }
    }

    private int pos(RexNode expr) {
      if (expr instanceof RexInputRef) {
        return ((RexInputRef) expr).getIndex();
      }
      return -1;
    }

    private boolean isAlwaysTrue(RexNode predicate) {
      if (predicate instanceof RexCall) {
        RexCall c = (RexCall) predicate;
        if (c.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(c.getOperands().get(0));
          int rPos = pos(c.getOperands().get(1));
          return lPos != -1 && lPos == rPos;
        }
      }
      return predicate.isAlwaysTrue();
    }
  }
}
