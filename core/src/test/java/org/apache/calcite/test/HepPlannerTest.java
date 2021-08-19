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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
    checkPlanning(
        CoreRules.PROJECT_REMOVE,
        "select d1.deptno from (select * from dept) d1,"
            + " (select * from dept) d2");
  }

  /** Tests that if two relational expressions are equivalent, the planner
   * notices, and only applies the rule once. */
  @Test void testCommonSubExpression() {
    // In the following,
    //   (select 1 from dept where abs(-1)=20)
    // occurs twice, but it's a common sub-expression, so the rule should only
    // apply once.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);

    final HepTestListener listener = new HepTestListener(0);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.addListener(listener);

    final String sql = "(select 1 from dept where abs(-1)=20)\n"
        + "union all\n"
        + "(select 1 from dept where abs(-1)=20)";
    planner.setRoot(tester.convertSqlToRel(sql).rel);
    RelNode bestRel = planner.findBestExp();

    assertThat(bestRel.getInput(0).equals(bestRel.getInput(1)), is(true));
    assertThat(listener.getApplyTimes() == 1, is(true));
  }

  @Test void testSubprogram() throws Exception {
    // Verify that subprogram gets re-executed until fixpoint.
    // In this case, the first time through we limit it to generate
    // only one calc; the second time through it will generate
    // a second calc, and then merge them.
    HepProgramBuilder subprogramBuilder = HepProgram.builder();
    subprogramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    subprogramBuilder.addMatchLimit(1);
    subprogramBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
<<<<<<< HEAD
    subprogramBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
=======
>>>>>>> 79e6848c7 (DX-34864 Add CoreRules and move static references of these rules to CoreRules)
    subprogramBuilder.addRuleInstance(CoreRules.CALC_MERGE);

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addSubprogram(subprogramBuilder.build());

    final String sql = "select upper(ename) from\n"
        + "(select lower(ename) as ename from emp where empno = 100)";
    sql(sql).with(programBuilder.build()).check();
  }

  @Test void testGroup() throws Exception {
    // Verify simultaneous application of a group of rules.
    // Intentionally add them in the wrong order to make sure
    // that order doesn't matter within the group.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addGroupBegin();
    programBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    programBuilder.addGroupEnd();

    final String sql = "select upper(name) from dept where deptno=20";
    sql(sql).with(programBuilder.build()).check();
  }

  @Test void testGC() throws Exception {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);

    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(
        tester.convertSqlToRel("select upper(name) from dept where deptno=20").rel);
    planner.findBestExp();
    // Reuse of HepPlanner (should trigger GC).
    planner.setRoot(
        tester.convertSqlToRel("select upper(name) from dept where deptno=20").rel);
    planner.findBestExp();
  }

  @Test void testRelNodeCacheWithDigest() {
    HepProgramBuilder programBuilder = HepProgram.builder();
    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());
    String query = "(select n_nationkey from SALES.CUSTOMER) union all\n"
        + "(select n_name from CUSTOMER_MODIFIABLEVIEW)";
    sql(query).withTester(t -> createDynamicTester())
        .withDecorrelation(true)
        .with(programBuilder.build())
        .with(planner)
        .checkUnchanged();
  }

  @Test void testRuleApplyCount() {
    final long applyTimes1 = checkRuleApplyCount(HepMatchOrder.ARBITRARY);
    assertThat(applyTimes1, is(316L));

    final long applyTimes2 = checkRuleApplyCount(HepMatchOrder.DEPTH_FIRST);
    assertThat(applyTimes2, is(87L));
  }

  @Test void testMaterialization() throws Exception {
    HepPlanner planner = new HepPlanner(HepProgram.builder().build());
    RelNode tableRel = tester.convertSqlToRel("select * from dept").rel;
    RelNode queryRel = tableRel;
    RelOptMaterialization mat1 = new RelOptMaterialization(
        tableRel, queryRel, null, ImmutableList.of("default", "mv"));
    planner.addMaterialization(mat1);
    assertEquals(planner.getMaterializations().size(), 1);
    assertEquals(planner.getMaterializations().get(0), mat1);
    planner.clear();
    assertEquals(planner.getMaterializations().size(), 0);
  }

  private long checkRuleApplyCount(HepMatchOrder matchOrder) {
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(matchOrder);
    programBuilder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    programBuilder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final HepTestListener listener = new HepTestListener(0);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.addListener(listener);
    planner.setRoot(tester.convertSqlToRel(COMPLEX_UNION_TREE).rel);
    planner.findBestExp();
    return listener.getApplyTimes();
  }

  /** Listener for HepPlannerTest; counts how many times rules fire. */
  private class HepTestListener implements RelOptListener {
    private long applyTimes;

    HepTestListener(long applyTimes) {
      this.applyTimes = applyTimes;
    }

    long getApplyTimes() {
      return applyTimes;
    }

    @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
    }

    @Override public void ruleAttempted(RuleAttemptedEvent event) {
      if (event.isBefore()) {
        ++applyTimes;
      }
    }

    @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    }

    @Override public void relDiscarded(RelDiscardedEvent event) {
    }

    @Override public void relChosen(RelChosenEvent event) {
    }
  }
}
