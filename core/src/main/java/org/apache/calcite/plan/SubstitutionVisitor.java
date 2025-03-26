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
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.calcite.rex.RexUtil.andNot;
import static org.apache.calcite.rex.RexUtil.removeAll;

/**
 * Substitutes part of a tree of relational expressions with another tree.
 *
 * <p>The call {@code new SubstitutionVisitor(target, query).go(replacement))}
 * will return {@code query} with every occurrence of {@code target} replaced
 * by {@code replacement}.</p>
 *
 * <p>The following example shows how {@code SubstitutionVisitor} can be used
 * for materialized view recognition.</p>
 *
 * <ul>
 * <li>query = SELECT a, c FROM t WHERE x = 5 AND b = 4</li>
 * <li>target = SELECT a, b, c FROM t WHERE x = 5</li>
 * <li>replacement = SELECT * FROM mv</li>
 * <li>result = SELECT a, c FROM mv WHERE b = 4</li>
 * </ul>
 *
 * <p>Note that {@code result} uses the materialized view table {@code mv} and a
 * simplified condition {@code b = 4}.</p>
 *
 * <p>Uses a bottom-up matching algorithm. Nodes do not need to be identical.
 * At each level, returns the residue.</p>
 *
 * <p>The inputs must only include the core relational operators:
 * {@link org.apache.calcite.rel.core.TableScan},
 * {@link org.apache.calcite.rel.core.Filter},
 * {@link org.apache.calcite.rel.core.Project},
 * {@link org.apache.calcite.rel.core.Join},
 * {@link org.apache.calcite.rel.core.Union},
 * {@link org.apache.calcite.rel.core.Aggregate}.</p>
 */
public class SubstitutionVisitor {
  private static final boolean DEBUG = CalciteSystemProperty.DEBUG.value();

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  protected static final ImmutableList<UnifyRule> DEFAULT_RULES =
      ImmutableList.of(
          TrivialRule.INSTANCE,
          ScanToCalcUnifyRule.INSTANCE,
          CalcToCalcUnifyRule.INSTANCE,
          JoinOnLeftCalcToJoinUnifyRule.INSTANCE,
          JoinOnRightCalcToJoinUnifyRule.INSTANCE,
          JoinOnCalcsToJoinUnifyRule.INSTANCE,
          AggregateToAggregateUnifyRule.INSTANCE,
          AggregateOnCalcToAggregateUnifyRule.INSTANCE,
          UnionToUnionUnifyRule.INSTANCE,
          UnionOnCalcsToUnionUnifyRule.INSTANCE);

  /**
   * Factory for a builder for relational expressions.
   */
  protected final RelBuilder relBuilder;

  private final ImmutableList<UnifyRule> rules;
  private final Map<Pair<Class, Class>, List<UnifyRule>> ruleMap =
      new HashMap<>();
  private final RelOptCluster cluster;
  private final RexSimplify simplify;
  private final Holder query;
  private final MutableRel target;

  /**
   * Nodes in {@link #target} that have no children.
   */
  final List<MutableRel> targetLeaves;

  /**
   * Nodes in {@link #query} that have no children.
   */
  final List<MutableRel> queryLeaves;

  final Map<MutableRel, MutableRel> replacementMap = new HashMap<>();

  final Multimap<MutableRel, MutableRel> equivalents =
      LinkedHashMultimap.create();

  /** Workspace while rule is being matched.
   * Careful, re-entrant!
   * Assumes no rule needs more than 2 slots. */
  protected final MutableRel[] slots = new MutableRel[2];

  /** Creates a SubstitutionVisitor with the default rule set. */
  public SubstitutionVisitor(RelNode target_, RelNode query_) {
    this(target_, query_, DEFAULT_RULES, RelFactories.LOGICAL_BUILDER);
  }

  /** Creates a SubstitutionVisitor with the default logical builder. */
  public SubstitutionVisitor(RelNode target_, RelNode query_,
      ImmutableList<UnifyRule> rules) {
    this(target_, query_, rules, RelFactories.LOGICAL_BUILDER);
  }

  public SubstitutionVisitor(RelNode target_, RelNode query_,
      ImmutableList<UnifyRule> rules, RelBuilderFactory relBuilderFactory) {
    this.cluster = target_.getCluster();
    final RexExecutor executor =
        Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
    this.simplify =
        new RexSimplify(cluster.getRexBuilder(), predicates, executor);
    this.rules = rules;
    this.query = Holder.of(MutableRels.toMutable(query_));
    this.target = MutableRels.toMutable(target_);
    this.relBuilder = relBuilderFactory.create(cluster, null);
    final Set<MutableRel> parents = Sets.newIdentityHashSet();
    final List<MutableRel> allNodes = new ArrayList<>();
    final MutableRelVisitor visitor =
        new MutableRelVisitor() {
          public void visit(MutableRel node) {
            parents.add(node.getParent());
            allNodes.add(node);
            super.visit(node);
          }
        };
    visitor.go(target);

    // Populate the list of leaves in the tree under "target".
    // Leaves are all nodes that are not parents.
    // For determinism, it is important that the list is in scan order.
    allNodes.removeAll(parents);
    targetLeaves = ImmutableList.copyOf(allNodes);

    allNodes.clear();
    parents.clear();
    visitor.go(query);
    allNodes.removeAll(parents);
    queryLeaves = ImmutableList.copyOf(allNodes);
  }

  void register(MutableRel result, MutableRel query) {
  }

  /**
   * Maps a condition onto a target.
   *
   * <p>If condition is stronger than target, returns the residue.
   * If it is equal to target, returns the expression that evaluates to
   * the constant {@code true}. If it is weaker than target, returns
   * {@code null}.</p>
   *
   * <p>The terms satisfy the relation</p>
   *
   * <blockquote>
   * <pre>{@code condition = target AND residue}</pre>
   * </blockquote>
   *
   * <p>and {@code residue} must be as weak as possible.</p>
   *
   * <p>Example #1: condition stronger than target</p>
   * <ul>
   * <li>condition: x = 1 AND y = 2</li>
   * <li>target: x = 1</li>
   * <li>residue: y = 2</li>
   * </ul>
   *
   * <p>Note that residue {@code x &gt; 0 AND y = 2} would also satisfy the
   * relation {@code condition = target AND residue} but is stronger than
   * necessary, so we prefer {@code y = 2}.</p>
   *
   * <p>Example #2: target weaker than condition (valid, but not currently
   * implemented)</p>
   * <ul>
   * <li>condition: x = 1</li>
   * <li>target: x = 1 OR z = 3</li>
   * <li>residue: x = 1</li>
   * </ul>
   *
   * <p>Example #3: condition and target are equivalent</p>
   * <ul>
   * <li>condition: x = 1 AND y = 2</li>
   * <li>target: y = 2 AND x = 1</li>
   * <li>residue: TRUE</li>
   * </ul>
   *
   * <p>Example #4: condition weaker than target</p>
   * <ul>
   * <li>condition: x = 1</li>
   * <li>target: x = 1 AND y = 2</li>
   * <li>residue: null (i.e. no match)</li>
   * </ul>
   *
   * <p>There are many other possible examples. It amounts to solving
   * whether {@code condition AND NOT target} can ever evaluate to
   * true, and therefore is a form of the NP-complete
   * <a href="http://en.wikipedia.org/wiki/Satisfiability">Satisfiability</a>
   * problem.</p>
   */
  @VisibleForTesting
  public static RexNode splitFilter(final RexSimplify simplify,
      RexNode condition, RexNode target) {
    final RexBuilder rexBuilder = simplify.rexBuilder;
    RexNode condition2 = canonizeNode(rexBuilder, condition);
    RexNode target2 = canonizeNode(rexBuilder, target);

    // First, try splitting into ORs.
    // Given target    c1 OR c2 OR c3 OR c4
    // and condition   c2 OR c4
    // residue is      c2 OR c4
    // Also deals with case target [x] condition [x] yields residue [true].
    RexNode z = splitOr(rexBuilder, condition2, target2);
    if (z != null) {
      return z;
    }

    if (isEquivalent(rexBuilder, condition2, target2)) {
      return rexBuilder.makeLiteral(true);
    }

    RexNode x = andNot(rexBuilder, target2, condition2);
    if (mayBeSatisfiable(x)) {
      RexNode x2 = RexUtil.composeConjunction(rexBuilder,
          ImmutableList.of(condition2, target2));
      RexNode r = canonizeNode(rexBuilder,
          simplify.simplifyUnknownAsFalse(x2));
      if (!r.isAlwaysFalse() && isEquivalent(rexBuilder, condition2, r)) {
        List<RexNode> conjs = RelOptUtil.conjunctions(r);
        for (RexNode e : RelOptUtil.conjunctions(target2)) {
          removeAll(conjs, e);
        }
        return RexUtil.composeConjunction(rexBuilder, conjs);
      }
    }
    return null;
  }

  /**
   * Reorders some of the operands in this expression so structural comparison,
   * i.e., based on string representation, can be more precise.
   */
  private static RexNode canonizeNode(RexBuilder rexBuilder, RexNode condition) {
    switch (condition.getKind()) {
    case AND:
    case OR: {
      RexCall call = (RexCall) condition;
      SortedMap<String, RexNode> newOperands = new TreeMap<>();
      for (RexNode operand : call.operands) {
        operand = canonizeNode(rexBuilder, operand);
        newOperands.put(operand.toString(), operand);
      }
      if (newOperands.size() < 2) {
        return newOperands.values().iterator().next();
      }
      return rexBuilder.makeCall(call.getOperator(),
          ImmutableList.copyOf(newOperands.values()));
    }
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case GREATER_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL: {
      RexCall call = (RexCall) condition;
      final RexNode left = call.getOperands().get(0);
      final RexNode right = call.getOperands().get(1);
      if (left.toString().compareTo(right.toString()) <= 0) {
        return call;
      }
      return RexUtil.invert(rexBuilder, call);
    }
    default:
      return condition;
    }
  }

  private static RexNode splitOr(
      final RexBuilder rexBuilder, RexNode condition, RexNode target) {
    List<RexNode> conditions = RelOptUtil.disjunctions(condition);
    int conditionsLength = conditions.size();
    int targetsLength = 0;
    for (RexNode e : RelOptUtil.disjunctions(target)) {
      removeAll(conditions, e);
      targetsLength++;
    }
    if (conditions.isEmpty() && conditionsLength == targetsLength) {
      return rexBuilder.makeLiteral(true);
    } else if (conditions.isEmpty()) {
      return condition;
    }
    return null;
  }

  private static boolean isEquivalent(RexBuilder rexBuilder, RexNode condition, RexNode target) {
    // Example:
    //  e: x = 1 AND y = 2 AND z = 3 AND NOT (x = 1 AND y = 2)
    //  disjunctions: {x = 1, y = 2, z = 3}
    //  notDisjunctions: {x = 1 AND y = 2}
    final Set<String> conditionDisjunctions = new HashSet<>(
        RexUtil.strings(RelOptUtil.conjunctions(condition)));
    final Set<String> targetDisjunctions = new HashSet<>(
        RexUtil.strings(RelOptUtil.conjunctions(target)));
    if (conditionDisjunctions.equals(targetDisjunctions)) {
      return true;
    }
    return false;
  }

  /**
   * Returns whether a boolean expression ever returns true.
   *
   * <p>This method may give false positives. For instance, it will say
   * that {@code x = 5 AND x > 10} is satisfiable, because at present it
   * cannot prove that it is not.</p>
   */
  public static boolean mayBeSatisfiable(RexNode e) {
    // Example:
    //  e: x = 1 AND y = 2 AND z = 3 AND NOT (x = 1 AND y = 2)
    //  disjunctions: {x = 1, y = 2, z = 3}
    //  notDisjunctions: {x = 1 AND y = 2}
    final List<RexNode> disjunctions = new ArrayList<>();
    final List<RexNode> notDisjunctions = new ArrayList<>();
    RelOptUtil.decomposeConjunction(e, disjunctions, notDisjunctions);

    // If there is a single FALSE or NOT TRUE, the whole expression is
    // always false.
    for (RexNode disjunction : disjunctions) {
      switch (disjunction.getKind()) {
      case LITERAL:
        if (!RexLiteral.booleanValue(disjunction)) {
          return false;
        }
      }
    }
    for (RexNode disjunction : notDisjunctions) {
      switch (disjunction.getKind()) {
      case LITERAL:
        if (RexLiteral.booleanValue(disjunction)) {
          return false;
        }
      }
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    for (RexNode notDisjunction : notDisjunctions) {
      final List<RexNode> disjunctions2 =
          RelOptUtil.conjunctions(notDisjunction);
      if (disjunctions.containsAll(disjunctions2)) {
        return false;
      }
    }
    return true;
  }

  public RelNode go0(RelNode replacement_) {
    assert false; // not called
    MutableRel replacement = MutableRels.toMutable(replacement_);
    assert equalType(
        "target", target, "replacement", replacement, Litmus.THROW);
    replacementMap.put(target, replacement);
    final UnifyResult unifyResult = matchRecurse(target);
    if (unifyResult == null) {
      return null;
    }
    final MutableRel node0 = unifyResult.result;
    MutableRel node = node0; // replaceAncestors(node0);
    if (DEBUG) {
      System.out.println("Convert: query:\n"
          + query.deep()
          + "\nunify.query:\n"
          + unifyResult.call.query.deep()
          + "\nunify.result:\n"
          + unifyResult.result.deep()
          + "\nunify.target:\n"
          + unifyResult.call.target.deep()
          + "\nnode0:\n"
          + node0.deep()
          + "\nnode:\n"
          + node.deep());
    }
    return MutableRels.fromMutable(node, relBuilder);
  }

  /**
   * Returns a list of all possible rels that result from substituting the
   * matched RelNode with the replacement RelNode within the query.
   *
   * <p>For example, the substitution result of A join B, while A and B
   * are both a qualified match for replacement R, is R join B, R join R,
   * A join R.
   */
  public List<RelNode> go(RelNode replacement_) {
    List<List<Replacement>> matches = go(MutableRels.toMutable(replacement_));
    if (matches.isEmpty()) {
      return ImmutableList.of();
    }
    List<RelNode> sub = new ArrayList<>();
    sub.add(MutableRels.fromMutable(query.getInput(), relBuilder));
    reverseSubstitute(relBuilder, query, matches, sub, 0, matches.size());
    return sub;
  }

  /**
   * Substitutes the query with replacement whenever possible but meanwhile
   * keeps track of all the substitutions and their original rel before
   * replacement, so that in later processing stage, the replacement can be
   * recovered individually to produce a list of all possible rels with
   * substitution in different places.
   */
  private List<List<Replacement>> go(MutableRel replacement) {
    assert equalType(
        "target", target, "replacement", replacement, Litmus.THROW);
    final List<MutableRel> queryDescendants = MutableRels.descendants(query);
    final List<MutableRel> targetDescendants = MutableRels.descendants(target);

    // Populate "equivalents" with (q, t) for each query descendant q and
    // target descendant t that are equal.
    final Map<MutableRel, MutableRel> map = new HashMap<>();
    for (MutableRel queryDescendant : queryDescendants) {
      map.put(queryDescendant, queryDescendant);
    }
    for (MutableRel targetDescendant : targetDescendants) {
      MutableRel queryDescendant = map.get(targetDescendant);
      if (queryDescendant != null) {
        assert rowTypesAreEquivalent(
            queryDescendant, targetDescendant, Litmus.THROW);
        equivalents.put(queryDescendant, targetDescendant);
      }
    }
    map.clear();

    final List<Replacement> attempted = new ArrayList<>();
    List<List<Replacement>> substitutions = new ArrayList<>();

    for (;;) {
      int count = 0;
      MutableRel queryDescendant = query;
    outer:
      while (queryDescendant != null) {
        for (Replacement r : attempted) {
          if (r.stopTrying && queryDescendant == r.after) {
            // This node has been replaced by previous iterations in the
            // hope to match its ancestors and stopTrying indicates
            // there's no need to be matched again.
            queryDescendant = MutableRels.preOrderTraverseNext(queryDescendant);
            continue outer;
          }
        }
        final MutableRel next = MutableRels.preOrderTraverseNext(queryDescendant);
        final MutableRel childOrNext =
            queryDescendant.getInputs().isEmpty()
                ? next : queryDescendant.getInputs().get(0);
        for (MutableRel targetDescendant : targetDescendants) {
          for (UnifyRule rule
              : applicableRules(queryDescendant, targetDescendant)) {
            UnifyRuleCall call =
                rule.match(this, queryDescendant, targetDescendant);
            if (call != null) {
              final UnifyResult result = rule.apply(call);
              if (result != null) {
                ++count;
                attempted.add(
                    new Replacement(result.call.query, result.result, result.stopTrying));
                result.call.query.replaceInParent(result.result);

                // Replace previous equivalents with new equivalents, higher up
                // the tree.
                for (int i = 0; i < rule.slotCount; i++) {
                  Collection<MutableRel> equi = equivalents.get(slots[i]);
                  if (!equi.isEmpty()) {
                    equivalents.remove(slots[i], equi.iterator().next());
                  }
                }
                assert rowTypesAreEquivalent(result.result, result.call.query, Litmus.THROW);
                equivalents.put(result.result, result.call.query);
                if (targetDescendant == target) {
                  // A real substitution happens. We purge the attempted
                  // replacement list and add them into substitution list.
                  // Meanwhile we stop matching the descendants and jump
                  // to the next subtree in pre-order traversal.
                  if (!target.equals(replacement)) {
                    Replacement r = replace(
                        query.getInput(), target, replacement.clone());
                    assert r != null
                        : rule + "should have returned a result containing the target.";
                    attempted.add(r);
                  }
                  substitutions.add(ImmutableList.copyOf(attempted));
                  attempted.clear();
                  queryDescendant = next;
                  continue outer;
                }
                // We will try walking the query tree all over again to see
                // if there can be any substitutions after the replacement
                // attempt.
                break outer;
              }
            }
          }
        }
        queryDescendant = childOrNext;
      }
      // Quit the entire loop if:
      // 1) we have walked the entire query tree with one or more successful
      //    substitutions, thus count != 0 && attempted.isEmpty();
      // 2) we have walked the entire query tree but have made no replacement
      //    attempt, thus count == 0 && attempted.isEmpty();
      // 3) we had done some replacement attempt in a previous walk, but in
      //    this one we have not found any potential matches or substitutions,
      //    thus count == 0 && !attempted.isEmpty().
      if (count == 0 || attempted.isEmpty()) {
        break;
      }
    }
    if (!attempted.isEmpty()) {
      // We had done some replacement attempt in the previous walk, but that
      // did not lead to any substitutions in this walk, so we need to recover
      // the replacement.
      undoReplacement(attempted);
    }
    return substitutions;
  }

  /**
   * Equivalence checking for row types, but except for the field names.
   */
  private boolean rowTypesAreEquivalent(
      MutableRel rel0, MutableRel rel1, Litmus litmus) {
    if (rel0.rowType.getFieldCount() != rel1.rowType.getFieldCount()) {
      return litmus.fail("Mismatch for column count: [{}]", Pair.of(rel0, rel1));
    }
    for (Pair<RelDataTypeField, RelDataTypeField> pair
        : Pair.zip(rel0.rowType.getFieldList(), rel1.rowType.getFieldList())) {
      if (!pair.left.getType().equals(pair.right.getType())) {
        return litmus.fail("Mismatch for column type: [{}]", Pair.of(rel0, rel1));
      }
    }
    return litmus.succeed();
  }

  /**
   * Represents a replacement action: before &rarr; after.
   * {@code stopTrying} indicates whether there's no need
   * to do matching for the same query node again.
   */
  static class Replacement {
    final MutableRel before;
    final MutableRel after;
    final boolean stopTrying;

    Replacement(MutableRel before, MutableRel after) {
      this(before, after, true);
    }

    Replacement(MutableRel before, MutableRel after, boolean stopTrying) {
      this.before = before;
      this.after = after;
      this.stopTrying = stopTrying;
    }
  }

  /** Within a relational expression {@code query}, replaces occurrences of
   * {@code find} with {@code replace}.
   *
   * <p>Assumes relational expressions (and their descendants) are not null.
   * Does not handle cycles. */
  public static Replacement replace(MutableRel query, MutableRel find,
      MutableRel replace) {
    if (find.equals(replace)) {
      // Short-cut common case.
      return null;
    }
    assert equalType("find", find, "replace", replace, Litmus.THROW);
    return replaceRecurse(query, find, replace);
  }

  /** Helper for {@link #replace}. */
  private static Replacement replaceRecurse(MutableRel query,
      MutableRel find, MutableRel replace) {
    if (find.equals(query)) {
      query.replaceInParent(replace);
      return new Replacement(query, replace);
    }
    for (MutableRel input : query.getInputs()) {
      Replacement r = replaceRecurse(input, find, replace);
      if (r != null) {
        return r;
      }
    }
    return null;
  }

  private static void undoReplacement(List<Replacement> replacement) {
    for (int i = replacement.size() - 1; i >= 0; i--) {
      Replacement r = replacement.get(i);
      r.after.replaceInParent(r.before);
    }
  }

  private static void redoReplacement(List<Replacement> replacement) {
    for (Replacement r : replacement) {
      r.before.replaceInParent(r.after);
    }
  }

  private static void reverseSubstitute(RelBuilder relBuilder, Holder query,
      List<List<Replacement>> matches, List<RelNode> sub,
      int replaceCount, int maxCount) {
    if (matches.isEmpty()) {
      return;
    }
    final List<List<Replacement>> rem = matches.subList(1, matches.size());
    reverseSubstitute(relBuilder, query, rem, sub, replaceCount, maxCount);
    undoReplacement(matches.get(0));
    if (++replaceCount < maxCount) {
      sub.add(MutableRels.fromMutable(query.getInput(), relBuilder));
    }
    reverseSubstitute(relBuilder, query, rem, sub, replaceCount, maxCount);
    redoReplacement(matches.get(0));
  }

  private UnifyResult matchRecurse(MutableRel target) {
    assert false; // not called
    final List<MutableRel> targetInputs = target.getInputs();
    MutableRel queryParent = null;

    for (MutableRel targetInput : targetInputs) {
      UnifyResult unifyResult = matchRecurse(targetInput);
      if (unifyResult == null) {
        return null;
      }
      queryParent = unifyResult.call.query.replaceInParent(unifyResult.result);
    }

    if (targetInputs.isEmpty()) {
      for (MutableRel queryLeaf : queryLeaves) {
        for (UnifyRule rule : applicableRules(queryLeaf, target)) {
          final UnifyResult x = apply(rule, queryLeaf, target);
          if (x != null) {
            if (DEBUG) {
              System.out.println("Rule: " + rule
                  + "\nQuery:\n"
                  + queryParent
                  + (x.call.query != queryParent
                     ? "\nQuery (original):\n"
                     + queryParent
                     : "")
                  + "\nTarget:\n"
                  + target.deep()
                  + "\nResult:\n"
                  + x.result.deep()
                  + "\n");
            }
            return x;
          }
        }
      }
    } else {
      assert queryParent != null;
      for (UnifyRule rule : applicableRules(queryParent, target)) {
        final UnifyResult x = apply(rule, queryParent, target);
        if (x != null) {
          if (DEBUG) {
            System.out.println(
                "Rule: " + rule
                + "\nQuery:\n"
                + queryParent.deep()
                + (x.call.query != queryParent
                   ? "\nQuery (original):\n"
                   + queryParent.toString()
                   : "")
                + "\nTarget:\n"
                + target.deep()
                + "\nResult:\n"
                + x.result.deep()
                + "\n");
          }
          return x;
        }
      }
    }
    if (DEBUG) {
      System.out.println(
          "Unify failed:"
          + "\nQuery:\n"
          + queryParent.toString()
          + "\nTarget:\n"
          + target.toString()
          + "\n");
    }
    return null;
  }

  private UnifyResult apply(UnifyRule rule, MutableRel query,
      MutableRel target) {
    final UnifyRuleCall call =
        new UnifyRuleCall(rule, query, target, ImmutableList.of());
    return rule.apply(call);
  }

  private List<UnifyRule> applicableRules(MutableRel query,
      MutableRel target) {
    final Class queryClass = query.getClass();
    final Class targetClass = target.getClass();
    final Pair<Class, Class> key = Pair.of(queryClass, targetClass);
    List<UnifyRule> list = ruleMap.get(key);
    if (list == null) {
      final ImmutableList.Builder<UnifyRule> builder =
          ImmutableList.builder();
      for (UnifyRule rule : rules) {
        //noinspection unchecked
        if (mightMatch(rule, queryClass, targetClass)) {
          builder.add(rule);
        }
      }
      list = builder.build();
      ruleMap.put(key, list);
    }
    return list;
  }

  private static boolean mightMatch(UnifyRule rule,
      Class queryClass, Class targetClass) {
    return rule.queryOperand.clazz.isAssignableFrom(queryClass)
        && rule.targetOperand.clazz.isAssignableFrom(targetClass);
  }

  /** Exception thrown to exit a matcher. Not really an error. */
  protected static class MatchFailed extends ControlFlowException {
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final MatchFailed INSTANCE = new MatchFailed();
  }

  /** Rule that attempts to match a query relational expression
   * against a target relational expression.
   *
   * <p>The rule declares the query and target types; this allows the
   * engine to fire only a few rules in a given context.</p>
   */
  public abstract static class UnifyRule {
    protected final int slotCount;
    protected final Operand queryOperand;
    protected final Operand targetOperand;

    protected UnifyRule(int slotCount, Operand queryOperand,
        Operand targetOperand) {
      this.slotCount = slotCount;
      this.queryOperand = queryOperand;
      this.targetOperand = targetOperand;
    }

    /**
     * <p>Applies this rule to a particular node in a query. The goal is
     * to convert {@code query} into {@code target}. Before the rule is
     * invoked, Calcite has made sure that query's children are equivalent
     * to target's children.
     *
     * <p>There are 3 possible outcomes:</p>
     *
     * <ul>
     *
     * <li>{@code query} already exactly matches {@code target}; returns
     * {@code target}</li>
     *
     * <li>{@code query} is sufficiently close to a match for
     * {@code target}; returns {@code target}</li>
     *
     * <li>{@code query} cannot be made to match {@code target}; returns
     * null</li>
     *
     * </ul>
     *
     * <p>REVIEW: Is possible that we match query PLUS one or more of its
     * ancestors?</p>
     *
     * @param call Input parameters
     */
    protected abstract UnifyResult apply(UnifyRuleCall call);

    protected UnifyRuleCall match(SubstitutionVisitor visitor, MutableRel query,
        MutableRel target) {
      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          return visitor.new UnifyRuleCall(this, query, target,
              copy(visitor.slots, slotCount));
        }
      }
      return null;
    }

    protected <E> ImmutableList<E> copy(E[] slots, int slotCount) {
      // Optimize if there are 0 or 1 slots.
      switch (slotCount) {
      case 0:
        return ImmutableList.of();
      case 1:
        return ImmutableList.of(slots[0]);
      default:
        return ImmutableList.copyOf(slots).subList(0, slotCount);
      }
    }
  }

  /**
   * Arguments to an application of a {@link UnifyRule}.
   */
  protected class UnifyRuleCall {
    protected final UnifyRule rule;
    public final MutableRel query;
    public final MutableRel target;
    protected final ImmutableList<MutableRel> slots;

    public UnifyRuleCall(UnifyRule rule, MutableRel query, MutableRel target,
        ImmutableList<MutableRel> slots) {
      this.rule = Objects.requireNonNull(rule);
      this.query = Objects.requireNonNull(query);
      this.target = Objects.requireNonNull(target);
      this.slots = Objects.requireNonNull(slots);
    }

    public UnifyResult result(MutableRel result) {
      return result(result,  true);
    }

    public UnifyResult result(MutableRel result, boolean stopTrying) {
      assert MutableRels.contains(result, target);
      assert equalType("result", result, "query", query,
          Litmus.THROW);
      MutableRel replace = replacementMap.get(target);
      if (replace != null) {
        assert false; // replacementMap is always empty
        // result =
        replace(result, target, replace);
      }
      register(result, query);
      return new UnifyResult(this, result, stopTrying);
    }

    /**
     * Creates a {@link UnifyRuleCall} based on the parent of {@code query}.
     */
    public UnifyRuleCall create(MutableRel query) {
      return new UnifyRuleCall(rule, query, target, slots);
    }

    public RelOptCluster getCluster() {
      return cluster;
    }

    public RexSimplify getSimplify() {
      return simplify;
    }
  }

  /**
   * Result of an application of a {@link UnifyRule} indicating that the
   * rule successfully matched {@code query} against {@code target} and
   * generated a {@code result} that is equivalent to {@code query} and
   * contains {@code target}. {@code stopTrying} indicates whether there's
   * no need to do matching for the same query node again.
   */
  protected static class UnifyResult {
    private final UnifyRuleCall call;
    private final MutableRel result;
    private final boolean stopTrying;

    UnifyResult(UnifyRuleCall call, MutableRel result, boolean stopTrying) {
      this.call = call;
      assert equalType("query", call.query, "result", result,
          Litmus.THROW);
      this.result = result;
      this.stopTrying = stopTrying;
    }
  }

  /** Abstract base class for implementing {@link UnifyRule}. */
  protected abstract static class AbstractUnifyRule extends UnifyRule {
    public AbstractUnifyRule(Operand queryOperand, Operand targetOperand,
        int slotCount) {
      super(slotCount, queryOperand, targetOperand);
      //noinspection AssertWithSideEffects
      assert isValid();
    }

    protected boolean isValid() {
      final SlotCounter slotCounter = new SlotCounter();
      slotCounter.visit(queryOperand);
      assert slotCounter.queryCount == slotCount;
      assert slotCounter.targetCount == 0;
      slotCounter.queryCount = 0;
      slotCounter.visit(targetOperand);
      assert slotCounter.queryCount == 0;
      assert slotCounter.targetCount == slotCount;
      return true;
    }

    /** Creates an operand with given inputs. */
    protected static Operand operand(Class<? extends MutableRel> clazz,
        Operand... inputOperands) {
      return new InternalOperand(clazz, ImmutableList.copyOf(inputOperands));
    }

    /** Creates an operand that doesn't check inputs. */
    protected static Operand any(Class<? extends MutableRel> clazz) {
      return new AnyOperand(clazz);
    }

    /** Creates an operand that matches a relational expression in the query. */
    protected static Operand query(int ordinal) {
      return new QueryOperand(ordinal);
    }

    /** Creates an operand that matches a relational expression in the
     * target. */
    protected static Operand target(int ordinal) {
      return new TargetOperand(ordinal);
    }
  }

  /** Implementation of {@link UnifyRule} that matches if the query is already
   * equal to the target.
   *
   * <p>Matches scans to the same table, because these will be
   * {@link MutableScan}s with the same
   * {@link org.apache.calcite.rel.core.TableScan} instance.</p>
   */
  public static class TrivialRule extends AbstractUnifyRule {
    public static final TrivialRule INSTANCE = new TrivialRule();

    private TrivialRule() {
      super(any(MutableRel.class), any(MutableRel.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      if (call.query.equals(call.target)) {
        return call.result(call.target);
      }
      return null;
    }
  }

  /** Implementation of {@link UnifyRule} that matches
   * {@link org.apache.calcite.rel.core.TableScan}. */
  public static class ScanToProjectUnifyRule extends AbstractUnifyRule {
    public static final ScanToProjectUnifyRule INSTANCE =
        new ScanToProjectUnifyRule();

    private ScanToProjectUnifyRule() {
      super(any(MutableScan.class),
          any(MutableProject.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableProject target = (MutableProject) call.target;
      final MutableScan query = (MutableScan) call.query;
      // We do not need to check query's parent type to avoid duplication
      // of ProjectToProjectUnifyRule or FilterToProjectUnifyRule, since
      // SubstitutionVisitor performs a top-down match.
      if (!query.equals(target.getInput())) {
        return null;
      }
      final RexShuttle shuttle = getRexShuttle(target);
      final RexBuilder rexBuilder = target.cluster.getRexBuilder();
      final List<RexNode> newProjects;
      try {
        newProjects = (List<RexNode>)
            shuttle.apply(rexBuilder.identityProjects(query.rowType));
      } catch (MatchFailed e) {
        return null;
      }
      final MutableProject newProject =
          MutableProject.of(query.rowType, target, newProjects);
      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }
  }

  /** Implementation of {@link UnifyRule} that matches
   * {@link org.apache.calcite.rel.core.Project}. */
  public static class ProjectToProjectUnifyRule extends AbstractUnifyRule {
    public static final ProjectToProjectUnifyRule INSTANCE =
        new ProjectToProjectUnifyRule();

    private ProjectToProjectUnifyRule() {
      super(operand(MutableProject.class, query(0)),
          operand(MutableProject.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableProject target = (MutableProject) call.target;
      final MutableProject query = (MutableProject) call.query;
      final RexShuttle shuttle = getRexShuttle(target);
      final List<RexNode> newProjects;
      try {
        newProjects = shuttle.apply(query.projects);
      } catch (MatchFailed e) {
        return null;
      }
      final MutableProject newProject =
          MutableProject.of(query.rowType, target, newProjects);
      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }
  }


  /** Implementation of {@link UnifyRule} that matches a {@link MutableFilter}
   * to a {@link MutableProject}. */
  public static class FilterToProjectUnifyRule extends AbstractUnifyRule {
    public static final FilterToProjectUnifyRule INSTANCE =
        new FilterToProjectUnifyRule();

    private FilterToProjectUnifyRule() {
      super(operand(MutableFilter.class, query(0)),
          operand(MutableProject.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      // Child of projectTarget is equivalent to child of filterQuery.
      try {
        // TODO: make sure that constants are ok
        final MutableProject target = (MutableProject) call.target;
        final RexShuttle shuttle = getRexShuttle(target);
        final RexNode newCondition;
        final MutableFilter query = (MutableFilter) call.query;
        try {
          newCondition = query.condition.accept(shuttle);
        } catch (MatchFailed e) {
          return null;
        }
        final MutableFilter newFilter = MutableFilter.of(target, newCondition);
        if (query.getParent() instanceof MutableProject) {
          final MutableRel inverse =
              invert(((MutableProject) query.getParent()).getNamedProjects(),
                  newFilter, shuttle);
          return call.create(query.getParent()).result(inverse);
        } else {
          final MutableRel inverse = invert(query, newFilter, target);
          return call.result(inverse);
        }
      } catch (MatchFailed e) {
        return null;
      }
    }

    protected MutableRel invert(List<Pair<RexNode, String>> namedProjects,
        MutableRel input,
        RexShuttle shuttle) {
      LOGGER.trace("SubstitutionVisitor: invert:\nprojects: {}\ninput: {}\nproject: {}\n",
          namedProjects, input, shuttle);
      final List<RexNode> exprList = new ArrayList<>();
      final RexBuilder rexBuilder = input.cluster.getRexBuilder();
      final List<RexNode> projects = Pair.left(namedProjects);
      for (RexNode expr : projects) {
        exprList.add(rexBuilder.makeZeroLiteral(expr.getType()));
      }
      for (Ord<RexNode> expr : Ord.zip(projects)) {
        final RexNode node = expr.e.accept(shuttle);
        if (node == null) {
          throw MatchFailed.INSTANCE;
        }
        exprList.set(expr.i, node);
      }
      return MutableProject.of(input, exprList, Pair.right(namedProjects));
    }

    protected MutableRel invert(MutableRel model, MutableRel input,
        MutableProject project) {
      LOGGER.trace("SubstitutionVisitor: invert:\nmodel: {}\ninput: {}\nproject: {}\n",
          model, input, project);
      if (project.projects.size() < model.rowType.getFieldCount()) {
        throw MatchFailed.INSTANCE;
      }
      final List<RexNode> exprList = new ArrayList<>();
      final RexBuilder rexBuilder = model.cluster.getRexBuilder();
      for (int i = 0; i < model.rowType.getFieldCount(); i++) {
        exprList.add(null);
      }
      for (Ord<RexNode> expr : Ord.zip(project.projects)) {
        if (expr.e instanceof RexInputRef) {
          final int target = ((RexInputRef) expr.e).getIndex();
          if (exprList.get(target) == null) {
            exprList.set(target,
                rexBuilder.ensureType(expr.e.getType(),
                    RexInputRef.of(expr.i, input.rowType),
                    false));
          }
        }
      }
      if (exprList.indexOf(null) != -1) {
        throw MatchFailed.INSTANCE;
      }
      return MutableProject.of(model.rowType, input, exprList);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a
   * {@link MutableFilter}. */
  protected static class FilterToFilterUnifyRule extends AbstractUnifyRule {
    public static final FilterToFilterUnifyRule INSTANCE =
        new FilterToFilterUnifyRule();

    private FilterToFilterUnifyRule() {
      super(operand(MutableFilter.class, query(0)),
          operand(MutableFilter.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      // in.query can be rewritten in terms of in.target if its condition
      // is weaker. For example:
      //   query: SELECT * FROM t WHERE x = 1 AND y = 2
      //   target: SELECT * FROM t WHERE x = 1
      // transforms to
      //   result: SELECT * FROM (target) WHERE y = 2
      final MutableFilter query = (MutableFilter) call.query;
      final MutableFilter target = (MutableFilter) call.target;
      final MutableFilter newFilter =
          createFilter(call, query, target);
      if (newFilter == null) {
        return null;
      }
      return call.result(newFilter);
    }

    MutableFilter createFilter(UnifyRuleCall call, MutableFilter query,
        MutableFilter target) {
      final RexNode newCondition =
          splitFilter(call.getSimplify(), query.condition,
              target.condition);
      if (newCondition == null) {
        // Could not map query onto target.
        return null;
      }
      if (newCondition.isAlwaysTrue()) {
        return target;
      }
      return MutableFilter.of(target, newCondition);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a {@link MutableProject}
   * to a {@link MutableFilter}. */
  protected static class ProjectToFilterUnifyRule extends AbstractUnifyRule {
    public static final ProjectToFilterUnifyRule INSTANCE =
        new ProjectToFilterUnifyRule();

    private ProjectToFilterUnifyRule() {
      super(operand(MutableProject.class, query(0)),
          operand(MutableFilter.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      if (call.query.getParent() instanceof MutableFilter) {
        final UnifyRuleCall in2 = call.create(call.query.getParent());
        final MutableFilter query = (MutableFilter) in2.query;
        final MutableFilter target = (MutableFilter) in2.target;
        final MutableFilter newFilter =
            FilterToFilterUnifyRule.INSTANCE.createFilter(call, query, target);
        if (newFilter == null) {
          return null;
        }
        return in2.result(query.replaceInParent(newFilter));
      }
      return null;
    }
  }

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
  private static class AggregateOnCalcToAggregateUnifyRule extends AbstractUnifyRule {

    public static final AggregateOnCalcToAggregateUnifyRule INSTANCE =
        new AggregateOnCalcToAggregateUnifyRule();

    private AggregateOnCalcToAggregateUnifyRule() {
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
        final List<Pair<Integer, Integer>> pairs = new ArrayList<>();
        final List<Integer> groupings = aggregate2.groupSet.toList();
        for (int i = 0; i < groupings.size(); i++) {
          pairs.add(Pair.of(mapping.getTarget(groupings.get(i)), i));
        }
        Collections.sort(pairs);
        pairs.forEach(pair -> posList.add(pair.right));
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
}

// End SubstitutionVisitor.java
