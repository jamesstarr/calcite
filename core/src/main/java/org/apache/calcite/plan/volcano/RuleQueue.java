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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.SubstitutionRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;

import java.util.ArrayDeque;
   * Computes the importance of a set (which is that of its most important
   * subset).
   */
  public double getImportance(RelSet set) {
    double importance = 0;
    for (RelSubset subset : set.subsets) {
      importance =
          Math.max(
              importance,
              getImportance(subset));
    }
    return importance;
  }

  /**
   * Recomputes the importance of the given RelSubset.
   *
   * @param subset RelSubset whose importance is to be recomputed
   * @param force  if true, forces an importance update even if the subset has
   *               not been registered
   */
  public void recompute(RelSubset subset, boolean force) {
    Double previousImportance = subsetImportances.get(subset);
    if (previousImportance == null) {
      if (!force) {
        // Subset has not been registered yet. Don't worry about it.
        return;
      }

      previousImportance = Double.NEGATIVE_INFINITY;
    }

    double importance = computeImportance(subset);
    if (previousImportance == importance) {
      return;
    }

    updateImportance(subset, importance);
  }

  /**
   * Equivalent to
   * {@link #recompute(RelSubset, boolean) recompute(subset, false)}.
   */
  public void recompute(RelSubset subset) {
    recompute(subset, false);
  }

  /**
   * Artificially boosts the importance of the given {@link RelSubset}s by a
   * given factor.
   *
   * <p>Iterates over the currently boosted RelSubsets and removes their
   * importance boost, forcing a recalculation of the RelSubsets' importances
   * (see {@link #recompute(RelSubset)}).
   *
   * <p>Once RelSubsets have been restored to their normal importance, the
   * given RelSubsets have their importances boosted. A RelSubset's boosted
   * importance is always less than 1.0 (and never equal to 1.0).
   *
   * @param subsets RelSubsets to boost importance (priority)
   * @param factor  the amount to boost their importances (e.g., 1.25 increases
   *                importance by 25%)
   */
  public void boostImportance(Collection<RelSubset> subsets, double factor) {
    LOGGER.trace("boostImportance({}, {})", factor, subsets);
    final List<RelSubset> boostRemovals = new ArrayList<>();
    final Iterator<RelSubset> iter = boostedSubsets.iterator();
    while (iter.hasNext()) {
      RelSubset subset = iter.next();

      if (!subsets.contains(subset)) {
        iter.remove();
        boostRemovals.add(subset);
      }
    }

    boostRemovals.sort(new Comparator<RelSubset>() {
      public int compare(RelSubset o1, RelSubset o2) {
        int o1children = countChildren(o1);
        int o2children = countChildren(o2);
        int c = compare(o1children, o2children);
        if (c == 0) {
          // for determinism
          c = compare(o1.getId(), o2.getId());
        }
        return c;
      }

      private int compare(int i1, int i2) {
        return (i1 < i2) ? -1 : ((i1 == i2) ? 0 : 1);
      }

      private int countChildren(RelSubset subset) {
        int count = 0;
        for (RelNode rel : subset.getRels()) {
          count += rel.getInputs().size();
        }
        return count;
      }
    });

    for (RelSubset subset : boostRemovals) {
      subset.propagateBoostRemoval(planner);
    }

    for (RelSubset subset : subsets) {
      double importance = subsetImportances.get(subset);

      updateImportance(
          subset,
          Math.min(ONE_MINUS_EPSILON, importance * factor));

      subset.boosted = true;
      boostedSubsets.add(subset);
    }
  }

  void updateImportance(RelSubset subset, Double importance) {
    subsetImportances.put(subset, importance);

    for (PhaseMatchList matchList : matchListMap.values()) {
      Multimap<RelSubset, VolcanoRuleMatch> relMatchMap =
          matchList.matchMap;
      if (relMatchMap.containsKey(subset)) {
        for (VolcanoRuleMatch match : relMatchMap.get(subset)) {
          match.clearCachedImportance();
        }
      }
    }
  }

  /**
   * Returns the importance of an equivalence class of relational expressions.
   * Subset importances are held in a lookup table, and importance changes
   * gradually propagate through that table.
   *
   * <p>If a subset in the same set but with a different calling convention is
   * deemed to be important, then this subset has at least half of its
   * importance. (This rule is designed to encourage conversions to take
   * place.)</p>
   */
  double getImportance(RelSubset rel) {
    assert rel != null;

    double importance = 0;
    final RelSet set = planner.getSet(rel);
    assert set != null;
    for (RelSubset subset2 : set.subsets) {
      final Double d = subsetImportances.get(subset2);
      if (d == null) {
        continue;
      }
      double subsetImportance = d;
      if (subset2 != rel) {
        subsetImportance /= 2;
      }
      if (subsetImportance > importance) {
        importance = subsetImportance;
      }
    }
    return importance;
  }

  /**
   * Adds a rule match. The rule-matches are automatically added to all
   * existing {@link PhaseMatchList per-phase rule-match lists} which allow
   * the rule referenced by the match.
   */
  protected void addMatch(VolcanoRuleMatch match) {
    final String matchName = match.toString();
    for (PhaseMatchList matchList : matchListMap.values()) {
      Set<String> phaseRuleSet = phaseRuleMapping.get(matchList.phase);
      if (phaseRuleSet != ALL_RULES) {
        String ruleDescription = match.getRule().toString();
        if (!phaseRuleSet.contains(ruleDescription)) {
          continue;
        }
      }

      if (!matchList.names.add(matchName)) {
        // Identical match has already been added.
        continue;
      }

      LOGGER.trace("{} Rule-match queued: {}", matchList.phase.toString(), matchName);

      if (match.getRule() instanceof SubstitutionRule) {
        matchList.list.offerFirst(match);
      } else {
        matchList.list.offer(match);
      }

      matchList.matchMap.put(
          planner.getSubset(match.rels[0]), match);
    }
  }

  /**
   * Removes the rule match from the head of match list, and returns it.
   *
   * <p>Returns {@code null} if there are no more matches.</p>
   *
   * <p>Note that the VolcanoPlanner may still decide to reject rule matches
   * which have become invalid, say if one of their operands belongs to an
   * obsolete set or has importance=0.
   *
   * @throws java.lang.AssertionError if this method is called with a phase
   *                              previously marked as completed via
   *                              {@link #phaseCompleted(VolcanoPlannerPhase)}.
   */
  protected VolcanoRuleMatch popMatch(VolcanoPlannerPhase phase) {
    PhaseMatchList phaseMatchList = matchListMap.get(phase);
    if (phaseMatchList == null) {
      throw new AssertionError("Used match list for phase " + phase
          + " after phase complete");
    }

    VolcanoRuleMatch match;
    for (;;) {
      if (phaseMatchList.list.isEmpty()) {
        return null;
      }
<<<<<<< HEAD
=======
      if (LOGGER.isTraceEnabled()) {
        matchList.sort(MATCH_COMPARATOR);
        match = matchList.remove(0);
>>>>>>> df3383b8b ([CALCITE-2259] Allow Java 8 syntax)

      match = phaseMatchList.list.poll();

      if (skipMatch(match)) {
        LOGGER.debug("Skip match: {}", match);
      } else {
        break;
      }
    }

    // If sets have merged since the rule match was enqueued, the match
    // may not be removed from the matchMap because the subset may have
    // changed, it is OK to leave it since the matchMap will be cleared
    // at the end.
    phaseMatchList.matchMap.remove(
        planner.getSubset(match.rels[0]), match);

    LOGGER.debug("Pop match: {}", match);
    return match;
  }

  /** Returns whether to skip a match. This happens if any of the
   * {@link RelNode}s have importance zero. */
  private boolean skipMatch(VolcanoRuleMatch match) {
    for (RelNode rel : match.rels) {
      Double importance = planner.relImportances.get(rel);
      if (importance != null && importance == 0d) {
        return true;
      }
    }

    // If the same subset appears more than once along any path from root
    // operand to a leaf operand, we have matched a cycle. A relational
    // expression that consumes its own output can never be implemented, and
    // furthermore, if we fire rules on it we may generate lots of garbage.
    // For example, if
    //   Project(A, X = X + 0)
    // is in the same subset as A, then we would generate
    //   Project(A, X = X + 0 + 0)
    //   Project(A, X = X + 0 + 0 + 0)
    // also in the same subset. They are valid but useless.
    final Deque<RelSubset> subsets = new ArrayDeque<>();
    try {
      checkDuplicateSubsets(subsets, match.rule.getOperand(), match.rels);
    } catch (Util.FoundOne e) {
      return true;
    }
    return false;
  }

  /** Recursively checks whether there are any duplicate subsets along any path
   * from root of the operand tree to one of the leaves.
   *
   * <p>It is OK for a match to have duplicate subsets if they are not on the
   * same path. For example,
   *
   * <blockquote><pre>
   *   Join
   *  /   \
   * X     X
   * </pre></blockquote>
   *
   * <p>is a valid match.
   *
   * @throws org.apache.calcite.util.Util.FoundOne on match
   */
  private void checkDuplicateSubsets(Deque<RelSubset> subsets,
      RelOptRuleOperand operand, RelNode[] rels) {
    final RelSubset subset = planner.getSubset(rels[operand.ordinalInRule]);
    if (subsets.contains(subset)) {
      throw Util.FoundOne.NULL;
    }
    if (!operand.getChildOperands().isEmpty()) {
      subsets.push(subset);
      for (RelOptRuleOperand childOperand : operand.getChildOperands()) {
        checkDuplicateSubsets(subsets, childOperand, rels);
      }
      final RelSubset x = subsets.pop();
      assert x == subset;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * PhaseMatchList represents a set of {@link VolcanoRuleMatch rule-matches}
   * for a particular
   * {@link VolcanoPlannerPhase phase of the planner's execution}.
   */
  private static class PhaseMatchList {
    /**
     * The VolcanoPlannerPhase that this PhaseMatchList is used in.
     */
    final VolcanoPlannerPhase phase;

    /**
     * Current list of VolcanoRuleMatches for this phase. New rule-matches
     * are appended to the end of this list.
     * The rules are not sorted in any way.
     */
    final Deque<VolcanoRuleMatch> list = new LinkedList<>();

    /**
     * A set of rule-match names contained in {@link #list}. Allows fast
     * detection of duplicate rule-matches.
     */
    final Set<String> names = new HashSet<>();

    /**
     * Multi-map of RelSubset to VolcanoRuleMatches.
     */
    final Multimap<RelSubset, VolcanoRuleMatch> matchMap =
        HashMultimap.create();

    PhaseMatchList(VolcanoPlannerPhase phase) {
      this.phase = phase;
    }

    void clear() {
      list.clear();
      names.clear();
      matchMap.clear();
    }
  }
}
