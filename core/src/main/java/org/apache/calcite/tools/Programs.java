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
package org.apache.calcite.tools;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;

>>>>>>> df3383b8b ([CALCITE-2259] Allow Java 8 syntax)
      RelMetadataProvider plannerChain =
          ChainedRelMetadataProvider.of(list);
      rel.getCluster().setMetadataProvider(plannerChain);

      hepPlanner.setRoot(rel);
      return hepPlanner.findBestExp();
    };
  }

  /** Creates a program that invokes heuristic join-order optimization
   * (via {@link org.apache.calcite.rel.rules.JoinToMultiJoinRule},
   * {@link org.apache.calcite.rel.rules.MultiJoin} and
   * {@link org.apache.calcite.rel.rules.LoptOptimizeJoinRule})
   * if there are 6 or more joins (7 or more relations). */
  public static Program heuristicJoinOrder(
      final Iterable<? extends RelOptRule> rules,
      final boolean bushy, final int minJoinCount) {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final int joinCount = RelOptUtil.countJoins(rel);
      final Program program;
      if (joinCount < minJoinCount) {
        program = ofRules(rules);
      } else {
        // Create a program that gathers together joins as a MultiJoin.
        final HepProgram hep = new HepProgramBuilder()
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
            .build();
        final Program program1 =
            of(hep, false, DefaultRelMetadataProvider.INSTANCE);

        // Create a program that contains a rule to expand a MultiJoin
        // into heuristically ordered joins.
        // We use the rule set passed in, but remove JoinCommuteRule and
        // JoinPushThroughJoinRule, because they cause exhaustive search.
        final List<RelOptRule> list = Lists.newArrayList(rules);
        list.removeAll(
            ImmutableList.of(CoreRules.JOIN_COMMUTE,
                CoreRules.JOIN_ASSOCIATE,
                JoinPushThroughJoinRule.LEFT,
                JoinPushThroughJoinRule.RIGHT));
        list.add(bushy
            ? CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY
            : LoptOptimizeJoinRule.INSTANCE);
        final Program program2 = ofRules(list);

        program = sequence(program1, program2);
      }
      return program.run(
          planner, rel, requiredOutputTraits, materializations, lattices);
    };
  }

  public static Program calc(RelMetadataProvider metadataProvider) {
    return hep(RelOptRules.CALC_RULES, true, metadataProvider);
  }

  @Deprecated // to be removed before 2.0
  public static Program subquery(RelMetadataProvider metadataProvider) {
    return subQuery(metadataProvider);
  }

  public static Program subQuery(RelMetadataProvider metadataProvider) {
    return hep(
        ImmutableList.of((RelOptRule) CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
            CoreRules.JOIN_SUB_QUERY_TO_CORRELATE), true, metadataProvider);
  }

  public static Program getProgram() {
    return (planner, rel, requiredOutputTraits, materializations, lattices)
        -> null;
  }

  /** Returns the standard program used by Prepare. */
  public static Program standard() {
    return standard(DefaultRelMetadataProvider.INSTANCE);
  }

  /** Returns the standard program with user metadata provider. */
  public static Program standard(RelMetadataProvider metadataProvider) {
    final Program program1 =
        (planner, rel, requiredOutputTraits, materializations, lattices) -> {
          planner.setRoot(rel);
          for (RelOptMaterialization materialization : materializations) {
            planner.addMaterialization(materialization);
          }
          for (RelOptLattice lattice : lattices) {
            planner.addLattice(lattice);
          }

          final RelNode rootRel2 =
              rel.getTraitSet().equals(requiredOutputTraits)
                  ? rel
                  : planner.changeTraits(rel, requiredOutputTraits);
          assert rootRel2 != null;

          planner.setRoot(rootRel2);
          final RelOptPlanner planner2 = planner.chooseDelegate();
          final RelNode rootRel3 = planner2.findBestExp();
          assert rootRel3 != null : "could not implement exp";
          return rootRel3;
        };

    return sequence(subQuery(metadataProvider),
        new DecorrelateProgram(),
        new TrimFieldsProgram(),
        program1,

        // Second planner pass to do physical "tweaks". This the first time
        // that EnumerableCalcRel is introduced.
        calc(metadataProvider));
  }

  /** Program backed by a {@link RuleSet}. */
  static class RuleSetProgram implements Program {
    final RuleSet ruleSet;

    private RuleSetProgram(RuleSet ruleSet) {
      this.ruleSet = ruleSet;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      planner.clear();
      for (RelOptRule rule : ruleSet) {
        planner.addRule(rule);
      }
      for (RelOptMaterialization materialization : materializations) {
        planner.addMaterialization(materialization);
      }
      for (RelOptLattice lattice : lattices) {
        planner.addLattice(lattice);
      }
      if (!rel.getTraitSet().equals(requiredOutputTraits)) {
        rel = planner.changeTraits(rel, requiredOutputTraits);
      }
      planner.setRoot(rel);
      return planner.findBestExp();

    }
  }

  /** Program that runs sub-programs, sending the output of the previous as
   * input to the next. */
  private static class SequenceProgram implements Program {
    private final ImmutableList<Program> programs;

    SequenceProgram(ImmutableList<Program> programs) {
      this.programs = programs;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      for (Program program : programs) {
        rel = program.run(
            planner, rel, requiredOutputTraits, materializations, lattices);
      }
      return rel;
    }
  }

  /** Program that de-correlates a query.
   *
   * <p>To work around
   * <a href="https://issues.apache.org/jira/browse/CALCITE-842">[CALCITE-842]
   * Decorrelator gets field offsets confused if fields have been trimmed</a>,
   * disable field-trimming in {@link SqlToRelConverter}, and run
   * {@link TrimFieldsProgram} after this program. */
  private static class DecorrelateProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      final CalciteConnectionConfig config =
          planner.getContext().unwrap(CalciteConnectionConfig.class);
      if (config != null && config.forceDecorrelate()) {
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        return RelDecorrelator.decorrelateQuery(rel, relBuilder, false);
      }
      return rel;
    }
  }

  /** Program that trims fields. */
  private static class TrimFieldsProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return new RelFieldTrimmer(null, relBuilder).trim(rel);
    }
  }
}
<<<<<<< HEAD
=======
// End Programs.java
