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

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;

            // make sure this bad rel didn't get into the
            // set in any way (fixupInputs will do this but it
            // doesn't know if it should so it does it anyway)
            set.obliterateRelNode(rel);

            // There is already an equivalent expression. Use that
            // one, and forget about this one.
            return getSubset(equivRel);
          }
        }
      } else {
        set = childSet;
      }
    }

    // Place the expression in the appropriate equivalence set.
    if (set == null) {
      set = new RelSet(
          nextSetId++,
          Util.minus(
              RelOptUtil.getVariablesSet(rel),
              rel.getVariablesSet()),
          RelOptUtil.getVariablesUsed(rel));
      this.allSets.add(set);
    }

    // Chain to find 'live' equivalent set, just in case several sets are
    // merging at the same time.
    while (set.equivalentSet != null) {
      set = set.equivalentSet;
    }

    // Allow each rel to register its own rules.
    registerClass(rel);

    registerCount++;
    final int subsetBeforeCount = set.subsets.size();
    RelSubset subset = addRelToSet(rel, set);

    final RelNode xx = mapDigestToRel.put(key, rel);
    assert xx == null || xx == rel : rel.getDigest();

    LOGGER.trace("Register {} in {}", rel.getDescription(), subset.getDescription());

    // This relational expression may have been registered while we
    // recursively registered its children. If this is the case, we're done.
    if (xx != null) {
      return subset;
    }

    // Create back-links from its children, which makes children more
    // important.
    if (rel == this.root) {
      ruleQueue.subsetImportances.put(
          subset,
          1.0); // todo: remove
    }
    for (RelNode input : rel.getInputs()) {
      RelSubset childSubset = (RelSubset) input;
      childSubset.set.parents.add(rel);

      // Child subset is more important now a new parent uses it.
      ruleQueue.recompute(childSubset);
    }
    if (rel == this.root) {
      ruleQueue.subsetImportances.remove(subset);
    }

    // Remember abstract converters until they're satisfied
    if (rel instanceof AbstractConverter) {
      set.abstractConverters.add((AbstractConverter) rel);
    }

    // If this set has any unsatisfied converters, try to satisfy them.
    checkForSatisfiedConverters(set, rel);

    // Make sure this rel's subset importance is updated
    ruleQueue.recompute(subset, true);

    // Queue up all rules triggered by this relexp's creation.
    fireRules(rel, true);

    // It's a new subset.
    if (set.subsets.size() > subsetBeforeCount) {
      fireRules(subset, true);
    }

    return subset;
  }

  private RelSubset addRelToSet(RelNode rel, RelSet set) {
    RelSubset subset = set.add(rel);
    mapRel2Subset.put(rel, subset);

    // While a tree of RelNodes is being registered, sometimes nodes' costs
    // improve and the subset doesn't hear about it. You can end up with
    // a subset with a single rel of cost 99 which thinks its best cost is
    // 100. We think this happens because the back-links to parents are
    // not established. So, give the subset another change to figure out
    // its cost.
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    subset.propagateCostImprovements(this, mq, rel);

    return subset;
  }

  private RelSubset registerSubset(
      RelSet set,
      RelSubset subset) {
    if ((set != subset.set)
        && (set != null)
        && (set.equivalentSet == null)) {
      LOGGER.trace("Register #{} {}, and merge sets", subset.getId(), subset);
      merge(set, subset.set);
      registerCount++;
    }
    return subset;
  }

  // implement RelOptPlanner
  public void addListener(RelOptListener newListener) {
    // TODO jvs 6-Apr-2006:  new superclass AbstractRelOptPlanner
    // now defines a multicast listener; just need to hook it in
    if (listener != null) {
      throw Util.needToImplement("multiple VolcanoPlanner listeners");
    }
    listener = newListener;
  }

  // implement RelOptPlanner
  @Deprecated // to be removed before 2.0
  @Override public void registerMetadataProviders(List<RelMetadataProvider> list) {
    list.add(0, new VolcanoRelMetadataProvider());
  }

  // implement RelOptPlanner
  @Deprecated
  @Override public long getRelMetadataTimestamp(RelNode rel) {
    List<Long> inputSubsets = new ArrayList<>();
    for (RelNode input : rel.getInputs()) {
      RelSubset inputSubset = getSubset(input);
      if (inputSubset != null) {
        inputSubsets.add(inputSubset.timestamp);
      }
    }
    return inputSubsets.hashCode();
  }

  /**
   * Normalizes references to subsets within the string representation of a
   * plan.
   *
   * <p>This is useful when writing tests: it helps to ensure that tests don't
   * break when an extra rule is introduced that generates a new subset and
   * causes subsequent subset numbers to be off by one.
   *
   * <p>For example,
   *
   * <blockquote>
   * FennelAggRel.FENNEL_EXEC(child=Subset#17.FENNEL_EXEC,groupCount=1,
   * EXPR$1=COUNT())<br>
   * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#2.FENNEL_EXEC,
   * key=[0], discardDuplicates=false)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
   * child=Subset#4.FENNEL_EXEC, expr#0..8={inputs}, expr#9=3456,
   * DEPTNO=$t7, $f0=$t9)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
   * table=[CATALOG, SALES, EMP])</blockquote>
   *
   * <p>becomes
   *
   * <blockquote>
   * FennelAggRel.FENNEL_EXEC(child=Subset#{0}.FENNEL_EXEC, groupCount=1,
   * EXPR$1=COUNT())<br>
   * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#{1}.FENNEL_EXEC,
   * key=[0], discardDuplicates=false)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
   * child=Subset#{2}.FENNEL_EXEC,expr#0..8={inputs},expr#9=3456,DEPTNO=$t7,
   * $f0=$t9)<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
   * table=[CATALOG, SALES, EMP])</blockquote>
   *
   * @param plan Plan
   * @return Normalized plan
   */
  public static String normalizePlan(String plan) {
    if (plan == null) {
      return null;
    }
    final Pattern poundDigits = Pattern.compile("Subset#[0-9]+\\.");
    int i = 0;
    while (true) {
      final Matcher matcher = poundDigits.matcher(plan);
      if (!matcher.find()) {
        return plan;
      }
      final String token = matcher.group(); // e.g. "Subset#23."
      plan = plan.replace(token, "Subset#{" + i++ + "}.");
    }
  }

  /**
   * Sets whether this planner is locked. A locked planner does not accept
   * new rules. {@link #addRule(org.apache.calcite.plan.RelOptRule)} will do
   * nothing and return false.
   *
   * @param locked Whether planner is locked
   */
  public void setLocked(boolean locked) {
    this.locked = locked;
  }

<<<<<<< HEAD
=======
  public void ensureRegistered(
      RelNode rel,
      RelNode equivRel,
      VolcanoRuleCall ruleCall) {
    ensureRegistered(rel, equivRel);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * A rule call which defers its actions. Whereas {@link RelOptRuleCall}
   * invokes the rule when it finds a match, a <code>DeferringRuleCall</code>
   * creates a {@link VolcanoRuleMatch} which can be invoked later.
   */
  private static class DeferringRuleCall extends VolcanoRuleCall {
    DeferringRuleCall(
        VolcanoPlanner planner,
        RelOptRuleOperand operand) {
      super(planner, operand);
    }

    /**
     * Rather than invoking the rule (as the base method does), creates a
     * {@link VolcanoRuleMatch} which can be invoked later.
     */
    protected void onMatch() {
      final VolcanoRuleMatch match =
          new VolcanoRuleMatch(
              volcanoPlanner,
              getOperand0(),
              rels,
              nodeInputs);
      volcanoPlanner.ruleQueue.addMatch(match);
    }
  }

  /**
   * Where a RelNode came from.
   */
  private abstract static class Provenance {
    public static final Provenance EMPTY = new UnknownProvenance();
  }

  /**
   * We do not know where this RelNode came from. Probably created by hand,
   * or by sql-to-rel converter.
   */
  private static class UnknownProvenance extends Provenance {
  }

  /**
   * A RelNode that came directly from another RelNode via a copy.
   */
  static class DirectProvenance extends Provenance {
    final RelNode source;

    DirectProvenance(RelNode source) {
      this.source = source;
    }
  }

  /**
   * A RelNode that came via the firing of a rule.
   */
  static class RuleProvenance extends Provenance {
    final RelOptRule rule;
    final ImmutableList<RelNode> rels;
    final int callId;

    RuleProvenance(RelOptRule rule, ImmutableList<RelNode> rels, int callId) {
      this.rule = rule;
      this.rels = rels;
      this.callId = callId;
    }
  }
}
