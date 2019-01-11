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
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
>>>>>>> 01a1681b6 ([CALCITE-2454] Avoid treating Project(x=1) and Project(x=1) equal in case the type of 1 is int in the first rel and long in the second one)
  /**
   * Re-computes the digest of a {@link RelNode}.
   *
   * <p>Since a relational expression's digest contains the identifiers of its
   * children, this method needs to be called when the child has been renamed,
   * for example if the child's set merges with another.
   *
   * @param rel Relational expression
   */
  void rename(RelNode rel) {
    final String oldDigest = rel.getDigest();
    if (fixUpInputs(rel)) {
      final RelNode removed = mapDigestToRel.remove(oldDigest);
      assert removed == rel;
      rel.recomputeDigest();
      final String newDigest = rel.getDigest();
      LOGGER.trace("Rename #{} from '{}' to '{}'", rel.getId(), oldDigest, newDigest);
      final RelNode equivRel = mapDigestToRel.put(newDigest, rel);
      if (equivRel != null) {
        assert equivRel != rel;

        // There's already an equivalent with the same name, and we
        // just knocked it out. Put it back, and forget about 'rel'.
        LOGGER.trace("After renaming rel#{} it is now equivalent to rel#{}",
            rel.getId(), equivRel.getId());

<<<<<<< HEAD
        mapDigestToRel.put(key, equivRel);
=======
        assert RelOptUtil.equal(
            "rel rowtype",
            rel.getRowType(),
            "equivRel rowtype",
            equivRel.getRowType(),
            Litmus.THROW);

        mapDigestToRel.put(newDigest, equivRel);

        RelSubset equivRelSubset = getSubset(equivRel);

        // Remove back-links from children.
        for (RelNode input : rel.getInputs()) {
          ((RelSubset) input).set.parents.remove(rel);
        }

        // Remove rel from its subset. (This may leave the subset
        // empty, but if so, that will be dealt with when the sets
        // get merged.)
        final RelSubset subset = mapRel2Subset.put(rel, equivRelSubset);
        assert subset != null;
        boolean existed = subset.set.rels.remove(rel);
        assert existed : "rel was not known to its set";
        final RelSubset equivSubset = getSubset(equivRel);
        for (RelSubset s : subset.set.subsets) {
          if (s.best == rel) {
            s.best = equivRel;

            // Propagate cost improvement since this potentially would change the subset's best cost
            s.propagateCostImprovements(
                    this, equivRel.getCluster().getMetadataQuery(),
                    equivRel);
          }
        }

        if (equivSubset != subset) {
          // The equivalent relational expression is in a different
          // subset, therefore the sets are equivalent.
          assert equivSubset.getTraitSet().equals(
              subset.getTraitSet());
          assert equivSubset.set != subset.set;
          merge(equivSubset.set, subset.set);
        }
      }
    }
  }

  /**
   * Registers a {@link RelNode}, which has already been registered, in a new
   * {@link RelSet}.
   *
   * @param set Set
   * @param rel Relational expression
   */
  void reregister(
      RelSet set,
      RelNode rel) {
    // Is there an equivalent relational expression? (This might have
    // just occurred because the relational expression's child was just
    // found to be equivalent to another set.)
    RelNode equivRel = mapDigestToRel.get(rel.getDigest());
    if (equivRel != null && equivRel != rel) {
      assert equivRel.getClass() == rel.getClass();
      assert equivRel.getTraitSet().equals(rel.getTraitSet());
      assert RelOptUtil.equal(
          "rel rowtype",
          rel.getRowType(),
          "equivRel rowtype",
          equivRel.getRowType(),
          Litmus.THROW);

      return;
    }

    // Add the relational expression into the correct set and subset.
    addRelToSet(rel, set);
  }

  /**
   * If a subset has one or more equivalent subsets (owing to a set having
   * merged with another), returns the subset which is the leader of the
   * equivalence class.
   *
   * @param subset Subset
   * @return Leader of subset's equivalence class
   */
  private RelSubset canonize(final RelSubset subset) {
    if (subset.set.equivalentSet == null) {
      return subset;
    }
    RelSet set = subset.set;
    do {
      set = set.equivalentSet;
    } while (set.equivalentSet != null);
    return set.getOrCreateSubset(
        subset.getCluster(), subset.getTraitSet());
  }

  /**
   * Fires all rules matched by a relational expression.
   *
   * @param rel      Relational expression which has just been created (or maybe
   *                 from the queue)
   */
  void fireRules(RelNode rel) {
    for (RelOptRuleOperand operand : classOperands.get(rel.getClass())) {
      if (operand.matches(rel)) {
        final VolcanoRuleCall ruleCall;
        ruleCall = new DeferringRuleCall(this, operand);
        ruleCall.match(rel);
      }
    }
  }

  private boolean fixUpInputs(RelNode rel) {
    List<RelNode> inputs = rel.getInputs();
    int i = -1;
    int changeCount = 0;
    for (RelNode input : inputs) {
      ++i;
      if (input instanceof RelSubset) {
        final RelSubset subset = (RelSubset) input;
        RelSubset newSubset = canonize(subset);
        if (newSubset != subset) {
          rel.replaceInput(i, newSubset);
          if (subset.set != newSubset.set) {
            subset.set.parents.remove(rel);
            newSubset.set.parents.add(rel);
          }
          changeCount++;
        }
      }
    }
    RelMdUtil.clearCache(rel);
    return changeCount > 0;
  }

  private RelSet merge(RelSet set, RelSet set2) {
    assert set != set2 : "pre: set != set2";

    // Find the root of set2's equivalence tree.
    set = equivRoot(set);
    set2 = equivRoot(set2);

    // Looks like set2 was already marked as equivalent to set. Nothing
    // to do.
    if (set2 == set) {
      return set;
    }

    // If necessary, swap the sets, so we're always merging the newer set
    // into the older.
    if (set.id > set2.id) {
      RelSet t = set;
      set = set2;
      set2 = t;
    }

    // Merge.
    set.mergeWith(this, set2);

    // Was the set we merged with the root? If so, the result is the new
    // root.
    if (set2 == getSet(root)) {
      root =
          set.getOrCreateSubset(
              root.getCluster(),
              root.getTraitSet());
      ensureRootConverters();
    }

    return set;
  }

  private static RelSet equivRoot(RelSet s) {
    RelSet p = s; // iterates at twice the rate, to detect cycles
    while (s.equivalentSet != null) {
      p = forward2(s, p);
      s = s.equivalentSet;
    }
    return s;
  }

  /** Moves forward two links, checking for a cycle at each. */
  private static RelSet forward2(RelSet s, RelSet p) {
    p = forward1(s, p);
    p = forward1(s, p);
    return p;
  }

  /** Moves forward one link, checking for a cycle. */
  private static RelSet forward1(RelSet s, RelSet p) {
    if (p != null) {
      p = p.equivalentSet;
      if (p == s) {
        throw new AssertionError("cycle in equivalence tree");
      }
    }
    return p;
  }

  /**
   * Registers a new expression <code>exp</code> and queues up rule matches.
   * If <code>set</code> is not null, makes the expression part of that
   * equivalence set. If an identical expression is already registered, we
   * don't need to register this one and nor should we queue up rule matches.
   *
   * @param rel relational expression to register. Must be either a
   *         {@link RelSubset}, or an unregistered {@link RelNode}
   * @param set set that rel belongs to, or <code>null</code>
   * @return the equivalence-set
   */
  private RelSubset registerImpl(
      RelNode rel,
      RelSet set) {
    if (rel instanceof RelSubset) {
      return registerSubset(set, (RelSubset) rel);
    }

    assert !isRegistered(rel) : "already been registered: " + rel;
    if (rel.getCluster().getPlanner() != this) {
      throw new AssertionError("Relational expression " + rel
          + " belongs to a different planner than is currently being used.");
    }

    // Now is a good time to ensure that the relational expression
    // implements the interface required by its calling convention.
    final RelTraitSet traits = rel.getTraitSet();
    final Convention convention = traits.getTrait(ConventionTraitDef.INSTANCE);
    assert convention != null;
    if (!convention.getInterface().isInstance(rel)
        && !(rel instanceof Converter)) {
      throw new AssertionError("Relational expression " + rel
          + " has calling-convention " + convention
          + " but does not implement the required interface '"
          + convention.getInterface() + "' of that convention");
    }
    if (traits.size() != traitDefs.size()) {
      throw new AssertionError("Relational expression " + rel
          + " does not have the correct number of traits: " + traits.size()
          + " != " + traitDefs.size());
    }

    // Ensure that its sub-expressions are registered.
    rel = rel.onRegister(this);

    // Record its provenance. (Rule call may be null.)
    if (LOGGER.isDebugEnabled()) {
      if (ruleCallStack.isEmpty()) {
        provenanceMap.put(rel, Provenance.EMPTY);
      } else {
        final VolcanoRuleCall ruleCall = ruleCallStack.peek();
        provenanceMap.put(
            rel,
            new RuleProvenance(
                ruleCall.rule,
                ImmutableList.copyOf(ruleCall.rels),
                ruleCall.id));
      }
    }

    // If it is equivalent to an existing expression, return the set that
    // the equivalent expression belongs to.
    String key = rel.getDigest();
    RelNode equivExp = mapDigestToRel.get(key);
    if (equivExp == null) {
      // do nothing
    } else if (equivExp == rel) {
      return getSubset(rel);
    } else {
      assert RelOptUtil.equal(
          "left", equivExp.getRowType(),
          "right", rel.getRowType(),
          Litmus.THROW);
      RelSet equivSet = getSet(equivExp);
      if (equivSet != null) {
        LOGGER.trace(
            "Register: rel#{} is equivalent to {}", rel.getId(), equivExp);
        return registerSubset(set, getSubset(equivExp));
      }
    }

    // Converters are in the same set as their children.
    if (rel instanceof Converter) {
      final RelNode input = ((Converter) rel).getInput();
      final RelSet childSet = getSet(input);
      if ((set != null)
          && (set != childSet)
          && (set.equivalentSet == null)) {
        LOGGER.trace(
            "Register #{} {} (and merge sets, because it is a conversion)",
            rel.getId(), rel.getDigest());
        merge(set, childSet);

        // During the mergers, the child set may have changed, and since
        // we're not registered yet, we won't have been informed. So
        // check whether we are now equivalent to an existing
        // expression.
        if (fixUpInputs(rel)) {
          rel.recomputeDigest();
          key = rel.getDigest();
          RelNode equivRel = mapDigestToRel.get(key);
          if ((equivRel != rel) && (equivRel != null)) {
<<<<<<< HEAD
=======
            assert RelOptUtil.equal(
                "rel rowtype",
                rel.getRowType(),
                "equivRel rowtype",
                equivRel.getRowType(),
                Litmus.THROW);
>>>>>>> 01a1681b6 ([CALCITE-2454] Avoid treating Project(x=1) and Project(x=1) equal in case the type of 1 is int in the first rel and long in the second one)

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

    final int subsetBeforeCount = set.subsets.size();
    RelSubset subset = addRelToSet(rel, set);

    final RelNode xx = mapDigestToRel.put(key, rel);
    assert xx == null || xx == rel : rel.getDigest();

    LOGGER.trace("Register {} in {}", rel, subset);

    // This relational expression may have been registered while we
    // recursively registered its children. If this is the case, we're done.
    if (xx != null) {
      return subset;
    }

    for (RelNode input : rel.getInputs()) {
      RelSubset childSubset = (RelSubset) input;
      childSubset.set.parents.add(rel);
    }

    // Queue up all rules triggered by this relexp's creation.
    fireRules(rel);

    // It's a new subset.
    if (set.subsets.size() > subsetBeforeCount) {
      fireRules(subset);
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
    // not established. So, give the subset another chance to figure out
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
    }
    return subset;
  }

  // implement RelOptPlanner
<<<<<<< HEAD
  @Deprecated // to be removed before 2.0
  @Override public void registerMetadataProviders(List<RelMetadataProvider> list) {
=======
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
