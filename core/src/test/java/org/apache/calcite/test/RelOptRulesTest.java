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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.AggregateJoinJoinRemoveRule;
import org.apache.calcite.rel.rules.AggregateJoinRemoveRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.ExchangeRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinJoinRemoveRule;
import org.apache.calcite.rel.rules.ProjectJoinRemoveRule;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortJoinCopyRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexExecutorTest;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Predicate;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit test for rules in {@code org.apache.calcite.rel} and subpackages.
 *
 * <p>As input, the test supplies a SQL statement and a single rule; the SQL is
 * translated into relational algebra and then fed into a
 * {@link org.apache.calcite.plan.hep.HepPlanner}. The planner fires the rule on
 * every
 * pattern match in a depth-first left-to-right pre-order traversal of the tree
 * for as long as the rule continues to succeed in applying its transform. (For
 * rules which call transformTo more than once, only the last result is used.)
 * The plan before and after "optimization" is diffed against a .ref file using
 * {@link DiffRepository}.
 *
 * <p>Procedure for adding a new test case:
 *
 * <ol>
 * <li>Add a new public test method for your rule, following the existing
 * examples. You'll have to come up with an SQL statement to which your rule
 * will apply in a meaningful way. See {@link SqlToRelTestBase} class comments
 * for details on the schema.
 *
 * <li>Run the test. It should fail. Inspect the output in
 * {@code target/surefire/.../RelOptRulesTest.xml}.
 * (If you are running using maven and this file does not exist, add a
 * {@code -X} flag to the maven command line.)
 *
 * <li>Verify that the "planBefore" is the correct
 * translation of your SQL, and that it contains the pattern on which your rule
 * is supposed to fire. If all is well, replace
 * {@code src/test/resources/.../RelOptRulesTest.xml} and
 * with the new {@code target/surefire/.../RelOptRulesTest.xml}.
 *
 * <li>Run the test again. It should fail again, but this time it should contain
 * a "planAfter" entry for your rule. Verify that your rule applied its
 * transformation correctly, and then update the
 * {@code src/test/resources/.../RelOptRulesTest.xml} file again.
 *
 * <li>Run the test one last time; this time it should pass.
 * </ol>
 */
class RelOptRulesTest extends RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  private final PushProjector.ExprCondition skipItem = expr ->
      expr instanceof RexCall
          && "item".equalsIgnoreCase(((RexCall) expr).getOperator().getName());
  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(RelOptRulesTest.class);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6317">
   * [CALCITE-6317] Incorrect constant replacement when group keys are NULL</a>. */
  @Test void testPredicatePull() {
    final String sql = "select deptno, sal "
                 + "from emp "
                + "where deptno = 10 "
                + "group by rollup(sal, deptno)";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceNot() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

    final String sql = "select *\n"
        + "from (select (case when sal > 1000 then null else false end) as caseCol from emp)\n"
        + "where NOT(caseCol)";
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .checkUnchanged();
  }

  /**
   * Ignored until CALCITE-4134 gets cherry-picked.
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6746">[CALCITE-6746]
   * Optimization rule ProjectWindowTranspose is unsound</a>. */
  @Disabled @Test void testConstantWindow() {
    final String sql = "with empsalary(dept, empno, salary, enroll_date) as "
        + "(VALUES ('x', 10, 5200, DATE '2007-08-01'), (NULL, NULL, NULL, NULL))\n"
        + "select sum(salary) "
        + "OVER (order by enroll_date range between INTERVAL 365 DAYS preceding and "
        + "INTERVAL 365 DAYS following),\n"
        + "salary, enroll_date FROM empsalary";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .withRule(CoreRules.PROJECT_WINDOW_TRANSPOSE)
        .checkUnchanged();
  }

  @Test void testReduceNestedCaseWhen() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

    final String sql = "select sal\n"
            + "from emp\n"
            + "where case when (sal = 1000) then\n"
            + "(case when sal = 1000 then null else 1 end is null) else\n"
            + "(case when sal = 2000 then null else 1 end is null) end is true";
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .check();
  }

  @Test void testDigestOfApproximateDistinctAggregateCall() {
    HepProgram preProgram = new HepProgramBuilder()
            .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(AggregateProjectMergeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.AGGREGATE_PROJECT_MERGE);

    final String sql = "select *\n"
            + "from (\n"
            + "select deptno, count(distinct empno) from emp group by deptno\n"
            + "union all\n"
            + "select deptno, approx_count_distinct(empno) from emp group by deptno)";
    sql(sql).withPre(preProgram)
            .with(hepPlanner)
            .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1479">[CALCITE-1479]
   * AssertionError in ReduceExpressionsRule on multi-column IN
   * sub-query</a>. */
  @Test void testReduceCompositeInSubQuery() {
    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "select *\n"
        + "from emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from (\n"
        + "    select empno, deptno\n"
        + "    from emp\n"
        + "    group by empno, deptno))\n"
        + "or deptno < 40 + 60";
    checkSubQuery(sql)
        .with(hepProgram)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2865">[CALCITE-2865]
   * FilterProjectTransposeRule generates wrong traitSet when copyFilter/Project is true</a>. */
  @Test public void testFilterProjectTransposeRule() {
    List<RelOptRule> rules = Arrays.asList(
            CoreRules.FILTER_PROJECT_TRANSPOSE, // default: copyFilter=true, copyProject=true
            new FilterProjectTransposeRule(Filter.class, Project.class,
                false, false, RelFactories.LOGICAL_BUILDER));

    for (RelOptRule rule : rules) {
      RelBuilder b = RelBuilder.create(RelBuilderTest.config().build());
      RelNode in = b
              .scan("EMP")
              .sort(-4) // salary desc
              .project(b.field(3)) // salary
              .filter(b.equals(b.field(0), b.literal(11500))) // salary = 11500
              .build();
      HepProgram program = new HepProgramBuilder()
              .addRuleInstance(rule)
              .build();
      HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(in);
      RelNode result = hepPlanner.findBestExp();

      // Verify LogicalFilter traitSet (must be [3 DESC])
      RelNode filter = result.getInput(0);
      RelCollation collation = filter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
      Assert.assertNotNull(collation);
      List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
      Assert.assertEquals(1, fieldCollations.size());
      RelFieldCollation fieldCollation = fieldCollations.get(0);
      Assert.assertEquals(3, fieldCollation.getFieldIndex());
      Assert.assertEquals(RelFieldCollation.Direction.DESCENDING, fieldCollation.getDirection());
    }
  }

  @Test public void testReduceOrCaseWhen() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

    final String sql = "select sal\n"
        + "from emp\n"
        + "where case when sal = 1000 then null else 1 end is null\n"
        + "OR case when sal = 2000 then null else 1 end is null";
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .check();
  }

  @Test void testReduceNullableCase() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "SELECT CASE WHEN 1=2 "
        + "THEN cast((values(1)) as integer) "
        + "ELSE 2 end from (values(1))";
    sql(sql).with(hepPlanner).checkUnchanged();
  }

  @Test void testReduceNullableCase2() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "SELECT deptno, ename, CASE WHEN 1=2 "
        + "THEN substring(ename, 1, cast(2 as int)) ELSE NULL end from emp"
        + " group by deptno, ename, case when 1=2 then substring(ename,1, cast(2 as int))  else null end";
    sql(sql).with(hepPlanner).checkUnchanged();
  }

  @Test public void testProjectToWindowRuleForMultipleWindows() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    final String sql = "select\n"
        + " count(*) over(partition by empno order by sal) as count1,\n"
        + " count(*) over(partition by deptno order by sal) as count2,\n"
        + " sum(deptno) over(partition by empno order by sal) as sum1,\n"
        + " sum(deptno) over(partition by deptno order by sal) as sum2\n"
        + "from emp";
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .check();
  }

  @Test public void testUnionToDistinctRule() {
    checkPlanning(CoreRules.UNION_TO_DISTINCT,
        "select * from dept union select * from dept");
  }

  @Test public void testExtractJoinFilterRule() {
    checkPlanning(CoreRules.JOIN_EXTRACT_FILTER,
        "select 1 from emp inner join dept on emp.deptno=dept.deptno");
  }

  @Test public void testAddRedundantSemiJoinRule() {
    checkPlanning(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
        "select 1 from emp inner join dept on emp.deptno = dept.deptno");
  }

  @Test void testAddRedundantSemiJoinRule() {
    final String sql = "select 1 from emp inner join dept on emp.deptno = dept.deptno";
    sql(sql).withRule(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN).check();
  }

  @Test void testStrengthenJoinType() {
    // The "Filter(... , right.c IS NOT NULL)" above a left join is pushed into
    // the join, makes it an inner join, and then disappears because c is NOT
    // NULL.
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .build();
    final String sql = "select *\n"
        + "from dept left join emp on dept.deptno = emp.deptno\n"
        + "where emp.deptno is not null and emp.sal > 100";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3170">[CALCITE-3170]
   * ANTI join on conditions push down generates wrong plan</a>. */
  @Test public void testCanNotPushAntiJoinConditionsToLeft() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // not in (select dept.deptno from dept where emp.deptno > 20)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode relNode = relBuilder.push(left)
        .push(right)
        .antiJoin(
            relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
            RexInputRef.of(0, left.getRowType()),
            relBuilder.literal(20)))
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test public void testCanNotPushAntiJoinConditionsToRight() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // not in (select dept.deptno from dept where dept.dname = 'ddd')
    RelNode relNode = relBuilder.scan("EMP")
        .scan("DEPT")
        .antiJoin(
            relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder.equals(relBuilder.field(2, 1, "DNAME"),
                relBuilder.literal("ddd")))
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3171">[CALCITE-3171]
   * SemiJoin on conditions push down throws IndexOutOfBoundsException</a>. */
  @Test public void testPushSemiJoinConditionsToLeft() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // in (select dept.deptno from dept where emp.empno > 20)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode relNode = relBuilder.push(left)
        .push(right)
        .semiJoin(
            relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
                RexInputRef.of(0, left.getRowType()),
                relBuilder.literal(20)))
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test public void testFullOuterJoinSimplificationToLeftOuter() {
    checkPlanning(CoreRules.FILTER_INTO_JOIN,
        "select 1 from sales.dept d full outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie'");
  }

  @Test public void testFullOuterJoinSimplificationToRightOuter() {
    checkPlanning(CoreRules.FILTER_INTO_JOIN,
        "select 1 from sales.dept d full outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where e.sal > 100");
  }

  @Test public void testFullOuterJoinSimplificationToInner() {
    checkPlanning(CoreRules.FILTER_INTO_JOIN,
        "select 1 from sales.dept d full outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie' and e.sal > 100");
  }

  @Test public void testLeftOuterJoinSimplificationToInner() {
    checkPlanning(CoreRules.FILTER_INTO_JOIN,
        "select 1 from sales.dept d left outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where e.sal > 100");
  }

  @Test public void testRightOuterJoinSimplificationToInner() {
    checkPlanning(CoreRules.FILTER_INTO_JOIN,
        "select 1 from sales.dept d right outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3225">[CALCITE-3225]
   * JoinToMultiJoinRule should not match SEMI/ANTI LogicalJoin</a>. */
  @Test public void testJoinToMultiJoinDoesNotMatchSemiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from
    // (select * from emp join dept ON emp.deptno = emp.deptno) t
    // where emp.job in (select job from bonus)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode semiRight = relBuilder.scan("BONUS").build();
    RelNode relNode = relBuilder.push(left)
                                .push(right)
                                .join(
                                    JoinRelType.INNER,
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "DEPTNO"),
                                                    relBuilder.field(2, 1, "DEPTNO")))
                                .push(semiRight)
                                .semiJoin(
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "JOB"),
                                                    relBuilder.field(2, 1, "JOB")))
                                .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3225">[CALCITE-3225]
   * JoinToMultiJoinRule should not match SEMI/ANTI LogicalJoin</a>. */
  @Test public void testJoinToMultiJoinDoesNotMatchAntiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from
    // (select * from emp join dept ON emp.deptno = emp.deptno) t
    // where not exists (select job from bonus where emp.job = bonus.job)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode antiRight = relBuilder.scan("BONUS").build();
    RelNode relNode = relBuilder.push(left)
                                .push(right)
                                .join(
                                    JoinRelType.INNER,
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "DEPTNO"),
                                                    relBuilder.field(2, 1, "DEPTNO")))
                                .push(antiRight)
                                .antiJoin(
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "JOB"),
                                                    relBuilder.field(2, 1, "JOB")))
                                .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test public void testPushFilterPastAgg() {
    checkPlanning(CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        "select dname, c from"
            + " (select name dname, count(*) as c from dept group by name) t"
            + " where dname = 'Charlie'");
  }

  private void basePushFilterPastAggWithGroupingSets(boolean unchanged)
      throws Exception {
    final HepProgram preProgram =
            HepProgram.builder()
                .addRuleInstance(CoreRules.PROJECT_MERGE)
                .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
                .build();
    final HepProgram program =
            HepProgram.builder()
                .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
                .build();
    Sql sql = sql("${sql}").withPre(preProgram)
        .with(program);
    if (unchanged) {
      sql.checkUnchanged();
    } else {
      sql.check();
    }
  }

  @Test void testPushFilterPastAggWithGroupingSets1() throws Exception {
    basePushFilterPastAggWithGroupingSets(true);
  }

  @Test void testPushFilterPastAggWithGroupingSets2() throws Exception {
    basePushFilterPastAggWithGroupingSets(false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * FilterAggregateTransposeRule loses conditions that cannot be pushed</a>. */
  @Test public void testPushFilterPastAggTwo() {
    checkPlanning(CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        "select dept1.c1 from (\n"
            + "  select dept.name as c1, count(*) as c2\n"
            + "  from dept where dept.name > 'b' group by dept.name) dept1\n"
            + "where dept1.c1 > 'c' and (dept1.c2 > 30 or dept1.c1 < 'z')");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-799">[CALCITE-799]
   * Incorrect result for {@code HAVING count(*) > 1}</a>. */
  @Test public void testPushFilterPastAggThree() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
            .build();
    final String sql = "select deptno from emp\n"
        + "group by deptno having count(*) > 1";
    sql(sql).withRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1109">[CALCITE-1109]
   * FilterAggregateTransposeRule pushes down incorrect condition</a>. */
  @Test void testPushFilterPastAggFour() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
            .addRuleInstance(CoreRules.AGGREGATE_FILTER_TRANSPOSE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
            .build();
    final String sql = "select emp.deptno, count(*) from emp where emp.sal > '12'\n"
        + "group by emp.deptno";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-448">[CALCITE-448]
   * FilterIntoJoinRule creates filters containing invalid RexInputRef</a>. */
  @Test void testPushFilterPastProject() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final FilterJoinRule.Predicate predicate =
        (join, joinType, exp) -> joinType != JoinRelType.INNER;
    final FilterJoinRule.JoinConditionPushRule join =
            CoreRules.JOIN_CONDITION_PUSH.config
                    .withPredicate(predicate)
                    .withDescription("FilterJoinRule:no-filter")
                    .as(FilterJoinRule.JoinConditionPushRule.Config.class)
                    .toRule();
    final FilterJoinRule.FilterIntoJoinRule filterOnJoin =
            CoreRules.FILTER_INTO_JOIN.config
                    .withSmart(true)
                    .withPredicate(predicate)
                    .as(FilterJoinRule.FilterIntoJoinRule.Config.class)
                    .toRule();
    final HepProgram program =
        HepProgram.builder()
            .addGroupBegin()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(join)
            .addRuleInstance(filterOnJoin)
            .addGroupEnd()
            .build();
    final String sql = "select a.name\n"
        + "from dept a\n"
        + "left join dept b on b.deptno > 10\n"
        + "right join dept c on b.deptno > 10\n";
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  @Test public void testJoinProjectTranspose1() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .addRuleInstance(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
            .addRuleInstance(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final String sql = "select a.name\n"
        + "from dept a\n"
        + "left join dept b on b.deptno > 10\n"
        + "right join dept c on b.deptno > 10\n";
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1338">[CALCITE-1338]
   * JoinProjectTransposeRule should not pull a literal above the
   * null-generating side of a join</a>. */
  @Test void testJoinProjectTranspose2() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name, 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the left project of right join has literal. */
  @Test void testJoinProjectTranspose3() {
    final String sql = "select *\n"
        + "from (select name, 1 from dept) as a\n"
        + "right join dept b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the right project of left join has not-strong
   * expression {@code y is not null}. */
  @Test void testJoinProjectTranspose4() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select x name, y is not null from\n"
        + "(values (2, cast(null as integer)), (2, 1)) as t(x, y)) b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the right project of left join has not-strong
   * expression {@code 1 + 1}. */
  @Test void testJoinProjectTranspose5() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name, 1 + 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since both the left project and right project have
   * literal. */
  @Test void testJoinProjectTranspose6() {
    final String sql = "select *\n"
        + "from (select name, 1 from dept) a\n"
        + "full join (select name, 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * Should transpose since all expressions in the right project of left join
   * are strong. */
  @Test void testJoinProjectTranspose7() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name from dept) as b\n"
        + " on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .check();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should transpose since all expressions including
   * {@code deptno > 10 and cast(null as boolean)} in the right project of left
   * join are strong. */
  @Test void testJoinProjectTranspose8() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (\n"
        + "  select name, deptno > 10 and cast(null as boolean)\n"
        + "  from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .check();
  }

  @Test void testJoinProjectTransposeWindow() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "join (select rank() over (order by name) as r, 1 + 1 from dept) as b\n"
        + "on a.name = b.r";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3353">[CALCITE-3353]
   * ProjectJoinTransposeRule caused AssertionError when creating a new Join</a>.
   */
  @Test void testProjectJoinTransposeWithMergeJoin() {
    ProjectJoinTransposeRule testRule = new ProjectJoinTransposeRule(
             Project.class, Join.class, expr -> !(expr instanceof RexOver),
             RelFactories.LOGICAL_BUILDER);

    ImmutableList<RelOptRule> commonRules = ImmutableList.of(
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE);
    final RuleSet rules = RuleSets.ofList(ImmutableList.<RelOptRule>builder()
            .addAll(commonRules)
            .add(CoreRules.PROJECT_JOIN_TRANSPOSE)
            .build());
    final RuleSet testRules = RuleSets.ofList(ImmutableList.<RelOptRule>builder()
            .addAll(commonRules)
            .add(testRule).build());

    FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
            .build();

    RelBuilder builder = RelBuilder.create(config);
    RelNode logicalPlan = builder
            .values(new String[]{"id", "name"}, "1", "anna", "2", "bob", "3", "tom")
            .values(new String[]{"name", "age"}, "anna", "14", "bob", "17", "tom", "22")
            .join(JoinRelType.INNER, "name")
            .project(builder.field(3))
            .build();

    RelTraitSet desiredTraits = logicalPlan.getTraitSet()
            .replace(EnumerableConvention.INSTANCE);
    RelOptPlanner planner = logicalPlan.getCluster().getPlanner();
    RelNode enumerablePlan1 = Programs.of(rules).run(planner, logicalPlan,
            desiredTraits, ImmutableList.of(), ImmutableList.of());
    RelNode enumerablePlan2 = Programs.of(testRules).run(planner, logicalPlan,
            desiredTraits, ImmutableList.of(), ImmutableList.of());
    assertEquals(RelOptUtil.toString(enumerablePlan1), RelOptUtil.toString(enumerablePlan2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1338">[CALCITE-1338]
   * JoinProjectTransposeRule should not pull a literal above the
   * null-generating side of a join</a>. */
  @Test public void testJoinProjectTranspose2() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name, 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the left project of right join has literal. */
  @Test public void testJoinProjectTranspose3() {
    final String sql = "select *\n"
        + "from (select name, 1 from dept) as a\n"
        + "right join dept b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the right project of left join has not-strong
   * expression {@code y is not null}. */
  @Test public void testJoinProjectTranspose4() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select x name, y is not null from\n"
        + "(values (2, cast(null as integer)), (2, 1)) as t(x, y)) b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the right project of left join has not-strong
   * expression {@code 1 + 1}. */
  @Test public void testJoinProjectTranspose5() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name, 1 + 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since both the left project and right project have
   * literal. */
  @Test public void testJoinProjectTranspose6() {
    final String sql = "select *\n"
        + "from (select name, 1 from dept) a\n"
        + "full join (select name, 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * Should transpose since all expressions in the right project of left join
   * are strong. */
  @Test public void testJoinProjectTranspose7() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name from dept) as b\n"
        + " on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .check();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should transpose since all expressions including
   * {@code deptno > 10 and cast(null as boolean)} in the right project of left
   * join are strong. */
  @Test public void testJoinProjectTranspose8() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (\n"
        + "  select name, deptno > 10 and cast(null as boolean)\n"
        + "  from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test void testSortUnionTranspose() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
            .addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 10";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test void testSortUnionTranspose2() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
            .addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-987">[CALCITE-987]
   * Push limit 0 will result in an infinite loop</a>. */
  @Test void testSortUnionTranspose3() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
            .addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 0";
    sql(sql).with(program).check();
  }

  @Test public void testSortRemovalAllKeysConstant() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortRemoveConstantKeysRule.INSTANCE)
        .build();
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal\n"
        + "order by deptno desc nulls last";
    sql(sql).with(program).check();
  }

  @Test public void testSortRemovalOneKeyConstant() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortRemoveConstantKeysRule.INSTANCE)
        .build();
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal\n"
        + "order by deptno, sal desc nulls first";
    sql(sql).with(program).check();
  }

  @Test public void testSemiJoinRuleExists() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  @Test void testSemiJoinRule() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();
    final String sql = "select dept.* from dept join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1495">[CALCITE-1495]
   * SemiJoinRule should not apply to RIGHT and FULL JOIN</a>. */
  @Test void testSemiJoinRuleRight() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();
    final String sql = "select dept.* from dept right join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .withDecorrelation(true)
        .withTrim(true)
        .checkUnchanged();
  }

  /** Similar to {@link #testSemiJoinRuleRight()} but FULL. */
  @Test void testSemiJoinRuleFull() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();
    final String sql = "select dept.* from dept full join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .withDecorrelation(true)
        .withTrim(true)
        .checkUnchanged();
  }

  /** Similar to {@link #testSemiJoinRule()} but LEFT. */
  @Test void testSemiJoinRuleLeft() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();
    final String sql = "select name from dept left join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .withDecorrelation(true)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-438">[CALCITE-438]
   * Push predicates through SemiJoin</a>. */
  @Test void testPushFilterThroughSemiJoin() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
            .build();
    final String sql = "select * from (\n"
        + "  select * from dept where dept.deptno in (\n"
        + "    select emp.deptno from emp))R\n"
        + "where R.deptno <=10";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(false)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-571">[CALCITE-571]
   * ReduceExpressionsRule tries to reduce SemiJoin condition to non-equi
   * condition</a>. */
  @Test void testSemiJoinReduceConstants() {
    final HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "select e1.sal\n"
        + "from (select * from emp where deptno = 200) as e1\n"
        + "where e1.deptno in (\n"
        + "  select e2.deptno from emp e2 where e2.sal = 100)";
    sql(sql)
        .withDecorrelation(false)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  @Test void testSemiJoinTrim() throws Exception {
    final DiffRepository diffRepos = getDiffRepos();
    String sql = diffRepos.expand(null, "${sql}");

    TesterImpl t = (TesterImpl) tester;
    final RelDataTypeFactory typeFactory = t.getTypeFactory();
    final Prepare.CatalogReader catalogReader =
        t.createCatalogReader(typeFactory);
    final SqlValidator validator =
        t.createValidator(
            catalogReader, typeFactory);
    SqlToRelConverter converter =
        t.createSqlToRelConverter(
            validator,
            catalogReader,
            typeFactory,
            SqlToRelConverter.Config.DEFAULT);

    final SqlNode sqlQuery = t.parseQuery(sql);
    final SqlNode validatedQuery = validator.validate(sqlQuery);
    RelRoot root =
        converter.convertQuery(validatedQuery, false, true);
    root = root.withRel(converter.decorrelate(sqlQuery, root.rel));

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();

    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root.rel);
    root = root.withRel(planner.findBestExp());

    String planBefore = NL + RelOptUtil.toString(root.rel);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    converter = t.createSqlToRelConverter(validator, catalogReader, typeFactory,
        SqlToRelConverter.configBuilder().withTrimUnusedFields(true).build());
    root = root.withRel(converter.trimUnusedFields(false, root.rel));
    String planAfter = NL + RelOptUtil.toString(root.rel);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testReduceAverage() {
    checkPlanning(CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        "select name, max(name), avg(deptno), min(name)"
            + " from sales.dept group by name");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1621">[CALCITE-1621]
   * Adding a cast around the null literal in aggregate rules</a>. */
  @Test void testCastInAggregateReduceFunctions() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
            .build();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " stddev_samp(deptno),var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountWithoutGroupBy() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select max(deptno), count(distinct ename)\n"
        + "from sales.emp";
    sql(sql).with(program).check();
  }

  @Test public void testDistinctCount1() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct ename)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCount2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct ename), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1293">[CALCITE-1293]
   * Bad code generated when argument to COUNT(DISTINCT) is a # GROUP BY
   * column</a>. */
  @Test void testDistinctCount3() {
    final String sql = "select count(distinct deptno), sum(sal)"
        + " from sales.emp group by deptno";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .build();
    sql(sql).with(program).check();
  }

  /** Tests implementing multiple distinct count the old way, using a join. */
  @Test void testDistinctCountMultipleViaJoin() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct ename), count(distinct job, ename),\n"
        + "count(distinct deptno, job), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  /** Tests implementing multiple distinct count the new way, using GROUPING
   *  SETS. */
  @Test void testDistinctCountMultiple() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct ename), count(distinct job)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMultipleNoGroup() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select count(distinct ename), count(distinct job)\n"
        + "from sales.emp";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMixedJoin() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct ename), count(distinct job, ename),\n"
        + "count(distinct deptno, job), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMixed() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct deptno, job) as cddj, sum(sal) as s\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMixed2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select deptno, count(distinct ename) as cde,\n"
        + "count(distinct job, ename) as cdje,\n"
        + "count(distinct deptno, job) as cddj,\n"
        + "sum(sal) as s\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountGroupingSets1() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select deptno, job, count(distinct ename)\n"
        + "from sales.emp group by rollup(deptno,job)";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountGroupingSets2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select deptno, job, count(distinct ename), sum(sal)\n"
        + "from sales.emp group by rollup(deptno,job)";
    sql(sql).with(program).check();
  }

  @Test void testDistinctNonDistinctAggregates() {
    final String sql = "select emp.empno, count(*), avg(distinct dept.deptno)\n"
        + "from sales.emp emp inner join sales.dept dept\n"
        + "on emp.deptno = dept.deptno\n"
        + "group by emp.empno";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1621">[CALCITE-1621]
   * Adding a cast around the null literal in aggregate rules</a>. */
  @Test void testCastInAggregateExpandDistinctAggregatesRule() {
    final String sql = "select name, sum(distinct cn), sum(distinct sm)\n"
        + "from (\n"
        + "  select name, count(dept.deptno) as cn,sum(dept.deptno) as sm\n"
        + "  from sales.dept group by name)\n"
        + "group by name";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .build();
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1558">[CALCITE-1558]
   * AggregateExpandDistinctAggregatesRule gets field mapping wrong if groupKey
   * is used in aggregate function</a>. */
  @Test void testDistinctNonDistinctAggregatesWithGrouping1() {
    final String sql = "SELECT deptno,\n"
        + "  SUM(deptno), SUM(DISTINCT sal), MAX(deptno), MAX(comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctNonDistinctAggregatesWithGrouping2() {
    final String sql = "SELECT deptno, COUNT(deptno), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctNonDistinctTwoAggregatesWithGrouping() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testRemoveDistinctOnAgg() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "SELECT empno, SUM(distinct sal), MIN(sal), "
        + "MIN(distinct sal), MAX(distinct sal), "
        + "bit_and(distinct sal), bit_or(sal), count(distinct sal) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql).with(program).check();
  }

  @Test void testMultipleDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), AVG(DISTINCT comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithMultipleInputs() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithMultipleInputsAndGroupby() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, deptno, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testPushProjectPastFilter() {
    checkPlanning(CoreRules.PROJECT_FILTER_TRANSPOSE,
        "select empno + deptno from emp where sal = 10 * comm "
            + "and upper(ename) = 'FOO'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1778">[CALCITE-1778]
   * Query with "WHERE CASE" throws AssertionError "Cast for just nullability
   * not allowed"</a>. */
  @Test void testPushProjectPastFilter2() {
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "where case when mgr < 10 then true else false end";
    sql(sql).withRule(CoreRules.PROJECT_FILTER_TRANSPOSE).check();
  }

  @Test public void testPushProjectPastJoin() {
    checkPlanning(CoreRules.PROJECT_JOIN_TRANSPOSE,
        "select e.sal + b.comm from emp e inner join bonus b "
            + "on e.ename = b.ename and e.deptno = 10");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3004">[CALCITE-3004]
   * Should not push over past union but its operands can since setop
   * will affect row count</a>. */
  @Test public void testProjectSetOpTranspose() {
    checkPlanning(CoreRules.PROJECT_SET_OP_TRANSPOSE,
        "select job, sum(sal + 100) over (partition by deptno) from\n"
            + "(select * from emp e1 union all select * from emp e2)");
  }

  @Test public void testProjectCorrelateTransposeDynamic() {
    ProjectCorrelateTransposeRule customPCTrans =
        new ProjectCorrelateTransposeRule(skipItem, RelFactories.LOGICAL_BUILDER);

    HepProgramBuilder programBuilder = HepProgram.builder()
        .addRuleInstance(customPCTrans);

    String query = "select t1.c_nationkey, t2.a as fake_col2 "
        + "from SALES.CUSTOMER as t1, "
        + "unnest(t1.fake_col) as t2(a)";

    checkPlanning(
        createDynamicTester(),
        null,
        new HepPlanner(programBuilder.build()),
        query,
        true);
  }

  @Test public void testProjectCorrelateTransposeRuleLeftCorrelate() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1 "
        + "where exists (select empno, deptno from dept d2 where e1.deptno = d2.deptno)";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .addRuleInstance(ProjectCorrelateTransposeRule.INSTANCE)
        .build();
    sql(sql)
        .withDecorrelation(false)
        .expand(true)
        .with(program)
        .check();
  }

  @Test public void testProjectCorrelateTransposeRuleSemiCorrelate() {
    RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode left = relBuilder
        .values(new String[]{"f", "f2"}, "1", "2").build();

    CorrelationId correlationId = new CorrelationId(0);
    RexNode rexCorrel =
        relBuilder.getRexBuilder().makeCorrel(
            left.getRowType(),
            correlationId);

    RelNode right = relBuilder
        .values(new String[]{"f3", "f4"}, "1", "2")
        .project(relBuilder.field(0),
            relBuilder.getRexBuilder()
                .makeFieldAccess(rexCorrel, 0))
        .build();
    LogicalCorrelate correlate = new LogicalCorrelate(left.getCluster(),
        left.getTraitSet(), left, right, correlationId,
        ImmutableBitSet.of(0), JoinRelType.SEMI);

    relBuilder.push(correlate);
    RelNode relNode = relBuilder.project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectCorrelateTransposeRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test public void testProjectCorrelateTransposeRuleAntiCorrelate() {
    RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode left = relBuilder
        .values(new String[]{"f", "f2"}, "1", "2").build();

    CorrelationId correlationId = new CorrelationId(0);
    RexNode rexCorrel =
        relBuilder.getRexBuilder().makeCorrel(
            left.getRowType(),
            correlationId);

    RelNode right = relBuilder
        .values(new String[]{"f3", "f4"}, "1", "2")
        .project(relBuilder.field(0),
            relBuilder.getRexBuilder().makeFieldAccess(rexCorrel, 0)).build();
    LogicalCorrelate correlate = new LogicalCorrelate(left.getCluster(),
        left.getTraitSet(), left, right, correlationId,
        ImmutableBitSet.of(0), JoinRelType.ANTI);

    relBuilder.push(correlate);
    RelNode relNode = relBuilder.project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectCorrelateTransposeRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test public void testProjectCorrelateTransposeWithExprCond() {
    ProjectCorrelateTransposeRule customPCTrans =
        new ProjectCorrelateTransposeRule(skipItem, RelFactories.LOGICAL_BUILDER);

    checkPlanning(customPCTrans,
        "select t1.name, t2.ename "
            + "from DEPT_NESTED as t1, "
            + "unnest(t1.employees) as t2");
  }

  @Test public void testProjectCorrelateTranspose() {
    ProjectCorrelateTransposeRule customPCTrans =
        new ProjectCorrelateTransposeRule(expr -> true,
            RelFactories.LOGICAL_BUILDER);

    checkPlanning(customPCTrans,
        "select t1.name, t2.ename "
            + "from DEPT_NESTED as t1, "
            + "unnest(t1.employees) as t2");
  }

  /** As {@link #testProjectSetOpTranspose()};
   * should not push over past correlate but its operands can since correlate
   * will affect row count. */
  @Test public void testProjectCorrelateTransposeWithOver() {
    checkPlanning(ProjectCorrelateTransposeRule.INSTANCE,
        "select sum(t1.deptno + 1) over (partition by t1.name),\n"
            + "count(t2.empno) over ()\n"
            + "from DEPT_NESTED as t1, "
            + "unnest(t1.employees) as t2");
  }

  /** Tests that the default instance of {@link FilterProjectTransposeRule}
   * does not push a Filter that contains a correlating variable.
   *
   * @see #testFilterProjectTranspose() */
  @Test public void testFilterProjectTransposePreventedByCorrelation() {
    final String sql = "SELECT e.empno\n"
        + "FROM emp as e\n"
        + "WHERE exists (\n"
        + "  SELECT *\n"
        + "  FROM (\n"
        + "    SELECT deptno * 2 AS twiceDeptno\n"
        + "    FROM dept) AS d\n"
        + "  WHERE e.deptno = d.twiceDeptno)";
    sql(sql)
        .withDecorrelation(false)
        .expand(true)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .checkUnchanged();
  }

  /** Tests a variant of {@link FilterProjectTransposeRule}
   * that pushes a Filter that contains a correlating variable. */
  @Test public void testFilterProjectTranspose() {
    final String sql = "SELECT e.empno\n"
        + "FROM emp as e\n"
        + "WHERE exists (\n"
        + "  SELECT *\n"
        + "  FROM (\n"
        + "    SELECT deptno * 2 AS twiceDeptno\n"
        + "    FROM dept) AS d\n"
        + "  WHERE e.deptno = d.twiceDeptno)";
    final FilterProjectTransposeRule filterProjectTransposeRule =
            CoreRules.FILTER_PROJECT_TRANSPOSE.config
                    .withOperandSupplier(b0 ->
                            b0.operand(Filter.class).predicate(filter -> true)
                                    .oneInput(b1 ->
                                            b1.operand(Project.class).predicate(project -> true)
                                                    .anyInputs()))
                    .as(FilterProjectTransposeRule.Config.class)
                    .withCopyFilter(true)
                    .withCopyProject(true)
                    .toRule();
    sql(sql)
        .withDecorrelation(false)
        .expand(true)
        .withRule(filterProjectTransposeRule)
        .check();
  }

  @Test public void testPushProjectWithOverPastJoin() {
    checkPlanning(CoreRules.PROJECT_JOIN_TRANSPOSE,
        "select e.sal + b.comm, count(e.empno) over() from emp e inner join bonus b "
            + "on e.ename = b.ename and e.deptno = 10");
  }

  private static final String NOT_STRONG_EXPR =
      "case when e.sal < 11 then 11 else -1 * e.sal end";

  private static final String STRONG_EXPR =
      "case when e.sal < 11 then -1 * e.sal else e.sal end";

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1753">[CALCITE-1753]
   * PushProjector should only preserve expressions if the expression is strong
   * when pushing into the nullable-side of outer join</a>. */
  @Test void testPushProjectPastInnerJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastInnerJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastLeftJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e left outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then 11 else -1 * e.sal end";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastLeftJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastLeftJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoinStrong() {
    final String sql = "select count(*),\n"
        + " case when e.sal < 11 then -1 * e.sal else e.sal end\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then -1 * e.sal else e.sal end";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastFullJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastFullJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2343">[CALCITE-2343]
   * Should not push over whose columns are all from left child past join since
   * join will affect row count.</a>. */
  @Test public void testPushProjectWithOverPastJoin1() {
    checkPlanning(CoreRules.PROJECT_JOIN_TRANSPOSE,
        "select e.sal + b.comm,\n"
            + "count(e.empno) over (partition by e.deptno)\n"
            + "from emp e join bonus b\n"
            + "on e.ename = b.ename and e.deptno = 10");
  }

  /** As {@link #testPushProjectWithOverPastJoin1()};
   * should not push over whose columns are all from right child past join since
   * join will affect row count. */
  @Test public void testPushProjectWithOverPastJoin2() {
    checkPlanning(CoreRules.PROJECT_JOIN_TRANSPOSE,
        "select e.sal + b.comm,\n"
            + "count(b.sal) over (partition by b.job)\n"
            + "from emp e join bonus b\n"
            + "on e.ename = b.ename and e.deptno = 10");
  }

  /** As {@link #testPushProjectWithOverPastJoin2()};
   * should not push over past join but should push the operands of over past
   * join. */
  @Test public void testPushProjectWithOverPastJoin3() {
    checkPlanning(CoreRules.PROJECT_JOIN_TRANSPOSE,
        "select e.sal + b.comm,\n"
            + "sum(b.sal + b.sal + 100) over (partition by b.job)\n"
            + "from emp e join bonus b\n"
            + "on e.ename = b.ename and e.deptno = 10");
  }

  @Test public void testPushProjectPastSetOp() {
    checkPlanning(CoreRules.PROJECT_SET_OP_TRANSPOSE,
        "select sal from "
            + "(select * from emp e1 union all select * from emp e2)");
  }

  @Test public void testPushJoinThroughUnionOnLeft() {
    checkPlanning(CoreRules.JOIN_LEFT_UNION_TRANSPOSE,
        "select r1.sal from "
            + "(select * from emp e1 union all select * from emp e2) r1, "
            + "emp r2");
  }

  @Test public void testPushJoinThroughUnionOnRight() {
    checkPlanning(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE,
        "select r1.sal from "
            + "emp r1, "
            + "(select * from emp e1 union all select * from emp e2) r2");
  }

  @Ignore("cycles")
  @Test public void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableScanRule.INSTANCE)
        .addRuleInstance(CoreRules.JOIN_EXTRACT_FILTER)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testPushJoinThroughUnionOnRightDoesNotMatchAntiJoin() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());

    // build a rel equivalent to sql:
    // select r1.sal from
    // emp r1 where r1.deptno not in
    //  (select deptno from dept d1 where deptno < 10
    //  union all
    //  select deptno from dept d2 where deptno > 20)
    RelNode left = builder.scan("EMP").build();
    RelNode right = builder
        .scan("DEPT")
        .filter(
            builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field("DEPTNO"),
                builder.literal(10)))
        .project(builder.field("DEPTNO"))
        .scan("DEPT")
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("DEPTNO"),
                builder.literal(20)))
        .project(builder.field("DEPTNO"))
        .union(true)
        .build();
    RelNode relNode = builder.push(left).push(right)
        .antiJoin(
            builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .project(builder.field("SAL"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_EXTRACT_FILTER)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();

    final String sql = "select d.name as dname,e.ename as ename\n"
        + " from emp e inner join dept d\n"
        + " on e.deptno=d.deptno\n"
        + " where d.name='Propane'";
    sql(sql).with(program).check();
  }

  /** Tests that filters are combined if they are identical. */
  @Test void testMergeFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_MERGE)
        .build();

    final String sql = "select name from (\n"
        + "  select *\n"
        + "  from dept\n"
        + "  where deptno = 10)\n"
        + "where deptno = 10\n";
    sql(sql).with(program).check();
  }

  /** Tests to see if the final branch of union is missed */
  @Test void testUnionMergeRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
            .addRuleInstance(CoreRules.PROJECT_REMOVE)
            .addRuleInstance(CoreRules.UNION_MERGE)
            .build();

    final String sql = "select * from (\n"
        + "select * from (\n"
        + "  select name, deptno from dept\n"
        + "  union all\n"
        + "  select name, deptno from\n"
        + "  (\n"
        + "    select name, deptno, count(1) from dept group by name, deptno\n"
        + "    union all\n"
        + "    select name, deptno, count(1) from dept group by name, deptno\n"
        + "  ) subq\n"
        + ") a\n"
        + "union all\n"
        + "select name, deptno from dept\n"
        + ") aa\n";
    sql(sql).with(program).check();
  }

  @Test void testMinusMergeRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
            .addRuleInstance(CoreRules.PROJECT_REMOVE)
            .addRuleInstance(CoreRules.MINUS_MERGE)
            .build();

    final String sql = "select * from (\n"
        + "select * from (\n"
        + "  select name, deptno from\n"
        + "  (\n"
        + "    select name, deptno, count(1) from dept group by name, deptno\n"
        + "    except all\n"
        + "    select name, deptno, 1 from dept\n"
        + "  ) subq\n"
        + "  except all\n"
        + "  select name, deptno from\n"
        + "  (\n"
        + "    select name, deptno, 1 from dept\n"
        + "    except all\n"
        + "    select name, deptno, count(1) from dept group by name, deptno\n"
        + "  ) subq2\n"
        + ") a\n"
        + "except all\n"
        + "select name, deptno from dept\n"
        + ") aa\n";
    sql(sql).with(program).check();
  }

  /** Tests that a filters is combined are combined if they are identical,
   * even if one of them originates in an ON clause of a JOIN. */
  @Test void testMergeJoinFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_MERGE)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .build();

    final String sql = "select * from (\n"
        + "  select d.deptno, e.ename\n"
        + "  from emp as e\n"
        + "  join dept as d\n"
        + "  on e.deptno = d.deptno\n"
        + "  and d.deptno = 10)\n"
        + "where deptno = 10\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union} operators into
   * a single {@code Union} with 3 inputs. */
  @Test void testMergeUnionAll() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.UNION_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union all\n"
        + "select * from emp where deptno = 20\n"
        + "union all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union}
   * {@code DISTINCT} (not {@code ALL}) operators into a single
   * {@code Union} with 3 inputs. */
  @Test void testMergeUnionDistinct() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.UNION_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union distinct\n"
        + "select * from emp where deptno = 20\n"
        + "union\n" // same as 'union distinct'
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests that {@link UnionMergeRule} does nothing if its arguments have
   * different {@code ALL} settings. */
  @Test void testMergeUnionMixed() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.UNION_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union\n"
        + "select * from emp where deptno = 20\n"
        + "union all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).checkUnchanged();
  }

  /** Tests that {@link UnionMergeRule} converts all inputs to DISTINCT
   * if the top one is DISTINCT.
   * (Since UNION is left-associative, the "top one" is the rightmost.) */
  @Test void testMergeUnionMixed2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.UNION_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union all\n"
        + "select * from emp where deptno = 20\n"
        + "union\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests that {@link UnionMergeRule} does nothing if its arguments have
   * are different set operators, {@link Union} and {@link Intersect}. */
  @Test void testMergeSetOpMixed() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.UNION_MERGE)
        .addRuleInstance(CoreRules.INTERSECT_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).checkUnchanged();
  }

  /** Tests {@link CoreRules#INTERSECT_MERGE}, which merges 2
   * {@link Intersect} operators into a single {@code Intersect} with 3
   * inputs. */
  @Test void testMergeIntersect() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.INTERSECT_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link org.apache.calcite.rel.rules.IntersectToDistinctRule},
   * which rewrites an {@link Intersect} operator with 3 inputs. */
  @Test void testIntersectToDistinct() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.INTERSECT_MERGE)
        .addRuleInstance(CoreRules.INTERSECT_TO_DISTINCT)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests that {@link org.apache.calcite.rel.rules.IntersectToDistinctRule}
   * correctly ignores an {@code INTERSECT ALL}. It can only handle
   * {@code INTERSECT DISTINCT}. */
  @Test void testIntersectToDistinctAll() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.INTERSECT_MERGE)
        .addRuleInstance(CoreRules.INTERSECT_TO_DISTINCT)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link CoreRules#MINUS_MERGE}, which merges 2
   * {@link Minus} operators into a single {@code Minus} with 3
   * inputs. */
  @Test void testMergeMinus() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.MINUS_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "except\n"
        + "select * from emp where deptno = 20\n"
        + "except\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link CoreRules#MINUS_MERGE}
   * does not merge {@code Minus(a, Minus(b, c))}
   * into {@code Minus(a, b, c)}, which would be incorrect. */
  @Test void testMergeMinusRightDeep() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.MINUS_MERGE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "except\n"
        + "select * from (\n"
        + "  select * from emp where deptno = 20\n"
        + "  except\n"
        + "  select * from emp where deptno = 30)";
    sql(sql).with(program).checkUnchanged();
  }

  @Test void testHeterogeneousConversion() throws Exception {
    // This one tests the planner's ability to correctly
    // apply different converters on top of a common
    // sub-expression.  The common sub-expression is the
    // reference to the table sales.emps.  On top of that
    // are two projections, unioned at the top.  For one
    // of the projections, transfer it to calc, for the other,
    // keep it unchanged.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableScanRule.INSTANCE)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)

            // Control the calc conversion.
        .addMatchLimit(1)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)

        .build();

    final String sql = "select upper(ename) from emp union all\n"
        + "select lower(ename) from emp";
    sql(sql).with(program).check();
  }

  @Test void testPushSemiJoinPastJoinRuleLeft() throws Exception {
    // tests the case where the semijoin is pushed to the left
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and e1.empno = e2.empno");
  }

  @Test void testPushSemiJoinPastJoinRuleRight() throws Exception {
    // tests the case where the semijoin is pushed to the right
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testPushSemiJoinPastFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
            + "where e.deptno = d.deptno and e.ename = 'foo'");
  }

  @Test void testConvertMultiJoinRule() throws Exception {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    sql(sql).with(program).check();
  }


  @Test public void testConvertMultiFullOuterJoinRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1 full outer join dept d on e1.deptno = d.deptno "
            + "full outer join emp e2 on d.deptno = e2.deptno");
  }

  @Test public void testConvertMultiFullOuterJoinRuleLoptOptimizeJoinRule() throws Exception {
    HepProgram preProgram = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(LoptOptimizeJoinRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram, new HepPlanner(program),
        "select e1.ename from emp e1 full outer join dept d on e1.deptno = d.deptno "
            + "full outer join emp e2 on d.deptno = e2.deptno");
  }

  @Test public void testConvertMultiJoinRuleLoptOptimizeJoinRuleNonOptimized() throws Exception {
    HepProgram preProgram = new HepProgramBuilder()
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
            .build();
    HepProgram program = new HepProgramBuilder()
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(LoptOptimizeJoinRule.UNOPTIMIZED)
            .build();
    checkPlanning(tester, preProgram, new HepPlanner(program),
            "select e1.ename from emp e1 full outer join dept d on e1.deptno = d.deptno "
                    + "full outer join emp e2 on d.deptno = e2.deptno");
  }

  @Test public void testConvertMultiFullOuterJoinRuleShouldNotCollapse() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1 full outer join dept d on e1.deptno = d.deptno "
            + "inner join emp e2 on d.deptno = e2.deptno");
  }

  @Test public void testConvertMultiLeftOuterJoinRuleShouldNotCollapse() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1 left outer join dept d on e1.deptno = d.deptno "
            + "full outer join emp e2 on d.deptno = e2.deptno");
  }

  @Test public void testConvertMultiFullOuterJoinRuleShouldNotCollapseLopOptimizeJoinRule()
      throws Exception {
    HepProgram preProgram = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(LoptOptimizeJoinRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram, new HepPlanner(program),
        "select e1.ename from emp e1 full outer join dept d on e1.deptno = d.deptno "
            + "inner join emp e2 on d.deptno = e2.deptno");
  }

  @Test public void testManyFiltersOnTopOfMultiJoinShouldCollapse() throws Exception {
    HepProgram program = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .addRuleCollection(
            Arrays.asList(CoreRules.FILTER_MULTI_JOIN_MERGE, CoreRules.PROJECT_MULTI_JOIN_MERGE))
        .build();
    checkPlanning(program,
        "select * from (select * from emp e1 left outer join dept d on e1.deptno = d.deptno "
          + "where d.deptno > 3) where ename LIKE 'bar'");
  }

  @Test public void testReduceConstants() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    // NOTE jvs 27-May-2006: among other things, this verifies
    // intentionally different treatment for identical coalesce expression
    // in select and where.

    // There is "CAST(2 AS INTEGER)" in the plan because 2 has type "INTEGER NOT
    // NULL" and we need "INTEGER".
    final String sql = "select"
        + " 1+2, d.deptno+(3+4), (5+6)+d.deptno, cast(null as integer),"
        + " coalesce(2,null), row(7+8)"
        + " from dept d inner join emp e"
        + " on d.deptno = e.deptno + (5-5)"
        + " where d.deptno=(7+8) and d.deptno=(8+7) and d.deptno=coalesce(2,null)";
    sql(sql).with(program)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-570">[CALCITE-570]
   * ReduceExpressionsRule throws "duplicate key" exception</a>. */
  @Test public void testReduceConstantsDup() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select d.deptno"
        + " from dept d"
        + " where d.deptno=7 and d.deptno=8";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-935">[CALCITE-935]
   * Improve how ReduceExpressionsRule handles duplicate constraints</a>. */
  @Test public void testReduceConstantsDup2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7 and deptno=8\n"
        + "and empno = 10 and mgr is null and empno = 10";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test public void testPullNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDup3Null() throws Exception {
    final String sql = "select e.empno"
        + " from emp e"
        + " where e.mgr<>7 or e.mgr<>8";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot() throws Exception {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where not(d.deptno=7 and d.deptno=8)";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNotNull() throws Exception {
    final String sql = "select e.empno"
        + " from emp e"
        + " where not(e.mgr=7 and e.mgr=8)";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot2() throws Exception {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where not(d.deptno=7 and d.name='foo' and d.deptno=8)";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot2Null() throws Exception {
    final String sql = "select e.empno"
        + " from emp e"
        + " where not(e.mgr=7 and e.deptno=8 and e.mgr=8)";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test void testPullNull() throws Exception {
    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7\n"
        + "and empno = 10 and mgr is null and empno = 10";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test public void testOrAlwaysTrue() {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
            .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
            .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "select * from EMPNULLABLES_20\n"
        + "where sal is null or sal is not null";
    checkPlanning(program, sql);
  }

  @Test public void testOrAlwaysTrue2() {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
            .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
            .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "select * from EMPNULLABLES_20\n"
        + "where sal is not null or sal is null";
    checkPlanning(program, sql);
  }

  @Test public void testReduceConstants2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    checkPlanUnchanged(new HepPlanner(program),
        "select p1 is not distinct from p0 from (values (2, cast(null as integer))) as t(p0, p1)");
  }

  @Test public void testReduceConstants3() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select e.mgr is not distinct from f.mgr "
        + "from emp e join emp f on (e.mgr=f.mgr) where e.mgr is null";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-902">[CALCITE-902]
   * Match nullability when reducing expressions in a Project</a>. */
  @Test public void testReduceConstantsProjectNullable() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    checkPlanning(program, "select mgr from emp where mgr=10");
  }

  // see HIVE-9645
  @Test public void testReduceConstantsNullEqualsOne() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    checkPlanning(program,
        "select count(1) from emp where cast(null as integer) = 1");
  }

  // see HIVE-9644
  @Test public void testReduceConstantsCaseEquals() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    // Equivalent to 'deptno = 10'
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test public void testReduceConstantsCaseEquals2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    // Equivalent to 'case when deptno = 20 then false
    //                     when deptno = 10 then true
    //                     else null end'
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test public void testReduceConstantsCaseEquals3() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    // Equivalent to 'deptno = 30 or deptno = 10'
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test
  public void testSkipReduceConstantsCaseEquals() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN_DUMB)
        .build();

    checkPlanning(program,
        "select * from emp e1, emp e2\n"
            + "where coalesce(e1.mgr, -1) = coalesce(e2.mgr, -1)");
  }

  @Test public void testReduceConstantsEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    // WHERE NULL is the same as WHERE FALSE, so get empty result
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1860">[CALCITE-1860]
   * Duplicate null predicates cause NullPointerException in RexUtil</a>. */
  @Test public void testReduceConstantsNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "select * from (\n"
        + "  select *\n"
        + "  from (\n"
        + "    select cast(null as integer) as n\n"
        + "    from emp)\n"
        + "  where n is null and n is null)\n"
        + "where n is null";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-566">[CALCITE-566]
   * ReduceExpressionsRule requires planner to have an Executor</a>. */
  @Test public void testReduceConstantsRequiresExecutor() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    // Remove the executor
    tester.convertSqlToRel("values 1").rel.getCluster().getPlanner()
        .setExecutor(null);

    // Rule should not fire, but there should be no NPE
    final String sql =
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)";
    checkPlanning(new HepPlanner(program), sql);
  }

  @Test public void testAlreadyFalseEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test void testReduceConstantsCalc() throws Exception {
    // This reduction does not work using
    // CoreRules.PROJECT_REDUCE_EXPRESSIONS or FILTER_INSTANCE,
    // only CALC_INSTANCE, because we need to pull the project expression
    //    upper('table')
    // into the condition
    //    upper('table') = 'TABLE'
    // and reduce it to TRUE. Only in the Calc are projects and conditions
    // combined.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)

            // the hard part is done... a few more rule calls to clean up
        .addRuleInstance(PruneEmptyRules.UNION_INSTANCE)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)
        .build();

    // Result should be same as typing
    //  SELECT * FROM (VALUES ('TABLE        ', 'T')) AS T(U, S)
    final String sql = "select * from (\n"
        + "  select upper(substring(x FROM 1 FOR 2) || substring(x FROM 3)) as u,\n"
        + "      substring(x FROM 1 FOR 1) as s\n"
        + "  from (\n"
        + "    select 'table' as x from (values (true))\n"
        + "    union\n"
        + "    select 'view' from (values (true))\n"
        + "    union\n"
        + "    select 'foreign table' from (values (true))\n"
        + "  )\n"
        + ") where u = 'TABLE'";
    sql(sql).with(program).check();
  }

  @Test public void testRemoveSemiJoin() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_REMOVE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
            + "where e.deptno = d.deptno");
  }

  @Test public void testRemoveSemiJoinWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE)
        .addRuleInstance(CoreRules.SEMI_JOIN_REMOVE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
            + "where e.deptno = d.deptno and e.ename = 'foo'");
  }

  @Test public void testRemoveSemiJoinRight() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .addRuleInstance(CoreRules.SEMI_JOIN_REMOVE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testRemoveSemiJoinRightWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .addRuleInstance(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE)
        .addRuleInstance(CoreRules.SEMI_JOIN_REMOVE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno "
            + "and d.name = 'foo'");
  }

  private void checkPlanning(String query) throws Exception {
    final Tester tester1 = tester.withCatalogReaderFactory(
        (typeFactory, caseSensitive) -> new MockCatalogReader(typeFactory, caseSensitive) {

          @Override public MockCatalogReader init() {
            // CREATE SCHEMA abc;
            // CREATE TABLE a(a INT);
            // ...
            // CREATE TABLE j(j INT);
            MockSchema schema = new MockSchema("SALES");
            registerSchema(schema);
            final RelDataType intType =
                typeFactory.createSqlType(SqlTypeName.INTEGER);
            for (int i = 0; i < 10; i++) {
              String t = String.valueOf((char) ('A' + i));
              MockTable table = MockTable.create(this, schema, t, false, 100);
              table.addColumn(t, intType);
              registerTable(table);
            }
            return this;
          }
          // CHECKSTYLE: IGNORE 1
        });

    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.PROJECT_REMOVE)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    sql(query).withTester(t -> tester1)
        .with(program)
        .check();
  }

  @Test void testConvertMultiJoinRuleOuterJoins() throws Exception {
    checkPlanning("select * from "
        + "    (select * from "
        + "        (select * from "
        + "            (select * from A right outer join B on a = b) "
        + "            left outer join "
        + "            (select * from C full outer join D on c = d)"
        + "            on a = c and b = d) "
        + "        right outer join "
        + "        (select * from "
        + "            (select * from E full outer join F on e = f) "
        + "            right outer join "
        + "            (select * from G left outer join H on g = h) "
        + "            on e = g and f = h) "
        + "        on a = e and b = f and c = g and d = h) "
        + "    inner join "
        + "    (select * from I inner join J on i = j) "
        + "    on a = i and h = j");
  }

  @Test void testConvertMultiJoinRuleOuterJoins2() throws Exception {
    // in (A right join B) join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    checkPlanning("select * from A right join B on a = b join C on b = c");
  }

  @Test void testConvertMultiJoinRuleOuterJoins3() throws Exception {
    // in (A join B) left join C, pushing C is allowed;
    // therefore there should be 1 MultiJoin
    checkPlanning("select * from A join B on a = b left join C on b = c");
  }

  @Test void testConvertMultiJoinRuleOuterJoins4() throws Exception {
    // in (A join B) right join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    checkPlanning("select * from A join B on a = b right join C on b = c");
  }

  @Test public void testPushSemiJoinPastProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .addRuleInstance(CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE)
        .build();
    checkPlanning(program,
        "select e.* from "
            + "(select ename, trim(job), sal * 2, deptno from emp) e, dept d "
            + "where e.deptno = d.deptno");
  }

  @Test public void testReduceValuesUnderFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_VALUES_MERGE)
        .build();

    // Plan should be same as for
    // select a, b from (values (10,'x')) as t(a, b)");
    final String sql = "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15";
    sql(sql).withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_VALUES_MERGE)
        .check();
  }

  @Test public void testReduceValuesUnderProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_VALUES_MERGE)
        .build();

    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    final String sql = "select a + b from (values (10, 1), (20, 3)) as t(a, b)";
    sql(sql).withRule(CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_VALUES_MERGE)
        .check();
  }

  @Test public void testReduceValuesUnderProjectFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    final String sql = "select a + b as x, b, a\n"
        + "from (values (10, 1), (30, 7), (20, 3)) as t(a, b)\n"
        + "where a - b < 21";
    sql(sql).withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.PROJECT_MERGE,
        CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1439">[CALCITE-1439]
   * Handling errors during constant reduction</a>. */
  @Test public void testReduceCase() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select\n"
        + "  case when false then cast(2.1 as float)\n"
        + "   else cast(1 as integer) end as newcol\n"
        + "from emp";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  private void checkReduceNullableToNotNull(ReduceExpressionsRule rule) {
    final String sql = "select\n"
        + "  empno + case when 'a' = 'a' then 1 else null end as newcol\n"
        + "from emp";
    sql(sql).withRule(rule)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal that
   *  is cast to nullable. */
  @Test public void testReduceNullableToNotNull() throws Exception {
    checkReduceNullableToNotNull(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal. */
  @Test void testReduceNullableToNotNull2() throws Exception {
    final ReduceExpressionsRule.ProjectReduceExpressionsRule rule =
        new ReduceExpressionsRule.ProjectReduceExpressionsRule(
            LogicalProject.class, false,
            RelFactories.LOGICAL_BUILDER);
    checkReduceNullableToNotNull(rule);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2736">[CALCITE-2736]
   * ReduceExpressionsRule never reduces dynamic expressions but this should be
   * configurable</a>. Tests that a dynamic function (USER) is reduced if and
   * only if {@link ReduceExpressionsRule.Config#treatDynamicCallsAsConstant()}
   * is true. */
  @Test public void testReduceDynamic() {
    checkDynamicFunctions(true).get();
  }

  /** As {@link #testReduceDynamic()}. */
  @Test public void testNoReduceDynamic() {
    checkDynamicFunctions(false).get();
  }

  private Supplier<Void>  checkDynamicFunctions(boolean treatDynamicCallsAsConstant) {
    HepProgramBuilder builder = new HepProgramBuilder();

    final ReduceExpressionsRule<?> rule =
          CoreRules.PROJECT_REDUCE_EXPRESSIONS.config
                  .withOperandFor(LogicalProject.class)
                  .withTreatDynamicCallsAsConstant(treatDynamicCallsAsConstant)
                  .as(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.class)
                  .toRule();
    builder.addRuleInstance(rule);
    final HepPlanner hepPlanner = new HepPlanner(builder.build());
    RelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final DataContext dataContext = new RexExecutorTest.SingleValueDataContext(
          DataContext.Variable.USER.camelName, "happyCalciteUser");
    final RexExecutor executor = new RexExecutorImpl(dataContext);
    hepPlanner.setExecutor(executor);
    planner.setExecutor(executor);
    Tester tester = new TesterImpl(getDiffRepos())
            .withPlannerFactory(context -> planner);
    final boolean unchanged = !treatDynamicCallsAsConstant;
    final String sql = "select USER from emp";
    return () -> {
      checkPlanning(tester, null, hepPlanner, sql, unchanged);
      return null;
    };
  }

  @Test public void testReduceConstantsIsNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    checkPlanning(program,
        "select empno from emp where empno=10 and empno is null");
  }

  @Test public void testReduceConstantsIsNotNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

  private Supplier<Void>  checkDynamicFunctions(boolean treatDynamicCallsAsConstant) {
    HepProgramBuilder builder = new HepProgramBuilder();

    final ReduceExpressionsRule<?> rule =
          CoreRules.PROJECT_REDUCE_EXPRESSIONS.config
                  .withOperandFor(LogicalProject.class)
                  .withTreatDynamicCallsAsConstant(treatDynamicCallsAsConstant)
                  .as(ProjectReduceExpressionsRule.Config.class)
                  .toRule();
    builder.addRuleInstance(rule);
    final HepPlanner hepPlanner = new HepPlanner(builder.build());
    RelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final DataContext dataContext = new RexExecutorTest.SingleValueDataContext(
          DataContext.Variable.USER.camelName, "happyCalciteUser");
    final RexExecutor executor = new RexExecutorImpl(dataContext);
    hepPlanner.setExecutor(executor);
    planner.setExecutor(executor);
    Tester tester = new TesterImpl(getDiffRepos())
            .withPlannerFactory(context -> planner);
    final boolean unchanged = !treatDynamicCallsAsConstant;
    final String sql = "select USER from emp";
    return () -> {
      checkPlanning(tester, null, hepPlanner, sql, unchanged);
      return null;
    };
  }

  @Test void testReduceConstantsIsNull() throws Exception {
    final String sql = "select empno from emp where empno=10 and empno is null";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test void testReduceConstantsIsNotNull() throws Exception {
    final String sql = "select empno from emp\n"
        + "where empno=10 and empno is not null";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test public void testReduceConstantsNegated() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select empno from emp\n"
        + "where empno=10 and not(empno=10)";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test public void testReduceConstantsNegatedInverted() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2638">[CALCITE-2638]
   * Constant reducer must not duplicate calls to non-deterministic
   * functions</a>. */
  @Test public void testReduceConstantsNonDeterministicFunction() {
    final DiffRepository diffRepos = getDiffRepos();

    final SqlOperator nonDeterministicOp =
        new SqlSpecialOperator("NDC", SqlKind.OTHER_FUNCTION, 0, false,
            ReturnTypes.INTEGER, null, null) {
          @Override public boolean isDeterministic() {
            return false;
          }
        };

    // Build a tree equivalent to the SQL
    //  SELECT sal, n
    //  FROM (SELECT sal, NDC() AS n FROM emp)
    //  WHERE n > 10
    final RelBuilder builder =
        RelBuilder.create(RelBuilderTest.config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("SAL"),
                builder.alias(builder.call(nonDeterministicOp), "N"))
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field("N"), builder.literal(10)))
            .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /** Checks that constant reducer duplicates calls to dynamic functions, if
   * appropriate. CURRENT_TIMESTAMP is a dynamic function. */
  @Test public void testReduceConstantsDynamicFunction() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select sal, t\n"
        + "from (select sal, current_timestamp t from emp)\n"
        + "where t > TIMESTAMP '2018-01-01 00:00:00'";
    sql(sql).with(program).checkUnchanged();
  }

  @Test public void testCasePushIsAlwaysWorking() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select empno from emp"
        + " where case when sal > 1000 then empno else sal end = 1";
    checkPlanning(program, sql);
  }

  @Ignore // Calcite does not support INSERT yet
  @Test public void testReduceValuesNull() throws Exception {
    // The NULL literal presents pitfalls for value-reduction. Only
    // an INSERT statement contains un-CASTed NULL values.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_VALUES_MERGE)
        .build();
    checkPlanning(program,
        "insert into sales.depts(deptno,name) values (NULL, 'null')");
  }

  @Test void testReduceValuesToEmpty() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    final String sql = "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)\n"
        + "where a - b < 0";
    sql(sql).with(program).check();
  }

  @Test public void testReduceConstantsWindow() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_WINDOW_TRANSPOSE)
        .addRuleInstance(CoreRules.WINDOW_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "select col1, col2, col3\n"
        + "from (\n"
        + "  select empno,\n"
        + "    sum(100) over (partition by deptno, sal order by sal) as col1,\n"
        + "    sum(100) over (partition by sal order by deptno) as col2,\n"
        + "    sum(sal) over (partition by deptno order by sal) as col3\n"
        + "  from emp where sal = 5000)";

    checkPlanning(program, sql);
  }

  @Test public void testEmptyFilterProjectUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.UNION_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (30, 3)) as t(x, y)");
    final String sql = "select * from (\n"
        + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "union all\n"
        + "select * from (values (20, 2))\n"
        + ")\n"
        + "where x + y > 30";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1488">[CALCITE-1488]
   * ValuesReduceRule should ignore empty Values</a>. */
  @Test void testEmptyProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .addRuleInstance(CoreRules.FILTER_VALUES_MERGE)
        .addRuleInstance(CoreRules.PROJECT_VALUES_MERGE)
        .build();

    final String sql = "select z + x from (\n"
        + "  select x + y as z, x from (\n"
        + "    select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "    where x + y > 50))";
    sql(sql).with(program).check();
  }

  /** Same query as {@link #testEmptyProject()}, and {@link PruneEmptyRules}
   * is able to do the job that {@link ValuesReduceRule} cannot do. */
  @Test void testEmptyProject2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_VALUES_MERGE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();

    final String sql = "select z + x from (\n"
        + "  select x + y as z, x from (\n"
        + "    select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "    where x + y > 50))";
    sql(sql).with(program).check();
  }

  @Test void testEmptyIntersect() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.INTERSECT_INSTANCE)
        .build();

    final String sql = "select * from (values (30, 3))"
        + "intersect\n"
        + "select *\nfrom (values (10, 1), (30, 3)) as t (x, y) where x > 50\n"
        + "intersect\n"
        + "select * from (values (30, 3))";
    sql(sql).with(program).check();
  }

  @Test void testEmptyMinus() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.MINUS_INSTANCE)
        .build();

    // First input is empty; therefore whole expression is empty
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "where x > 30\n"
        + "except\n"
        + "select * from (values (20, 2))\n"
        + "except\n"
        + "select * from (values (40, 4))";
    sql(sql).with(program).check();
  }

  @Test void testEmptyMinus2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.MINUS_INSTANCE)
        .build();

    // Second and fourth inputs are empty; they are removed
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "except\n"
        + "select * from (values (20, 2)) as t (x, y) where x > 30\n"
        + "except\n"
        + "select * from (values (40, 4))\n"
        + "except\n"
        + "select * from (values (50, 5)) as t (x, y) where x > 50";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyInnerJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    final String sql = "select * from (\n"
        + "select * from emp where false) as e\n"
        + "join dept as d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyLeftJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "left join dept d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyRightJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp right join dept".
    // Cannot optimize away the join because of RIGHT.
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "right join dept d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyFullJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.SORT_INSTANCE)
        .build();

    final String sql = "select * from emp where false order by deptno";
    sql(sql).with(program).check();
  }

  @Test void testEmptySort2() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("DEPT").empty()
        .sort(
            relBuilder.field("DNAME"),
            relBuilder.field("DEPTNO"))
        .build();

    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PruneEmptyRules.SORT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testEmptySortLimitZero() {
    final String sql = "select * from emp order by deptno limit 0";
    sql(sql).withRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE).check();
  }

  @Test void testEmptyAggregate() {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.AGGREGATE_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();

    final String sql = "select sum(empno) from emp where false group by deptno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test void testEmptyAggregateEmptyKey() {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PruneEmptyRules.AGGREGATE_INSTANCE)
        .build();

    final String sql = "select sum(empno) from emp where false";
    sql(sql).withPre(preProgram)
        .with(program)
        .checkUnchanged();
  }

  @Test void testEmptyAggregateEmptyKeyWithAggregateValuesRule() {
    HepProgram preProgram = HepProgram
        .builder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_VALUES)
        .build();

    final String sql = "select count(*), sum(empno) from emp where false";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test void testReduceCasts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    // The resulting plan should have no cast expressions
    checkPlanUnchanged(new HepPlanner(program),
        "select cast(d.name as varchar(128)), cast(e.empno as integer) "
            + "from dept as d inner join emp as e "
            + "on cast(d.deptno as integer) = cast(e.deptno as integer) "
            + "where cast(e.job as varchar(1)) = 'Manager'");
  }

  /** Tests that a cast from a TIME to a TIMESTAMP is not reduced. It is not
   * constant because the result depends upon the current date. */
  @Test void testReduceCastTimeUnchanged() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .build();

    sql("select cast(time '12:34:56' as timestamp) from emp as e")
        .with(program)
        .checkUnchanged();
  }

  @Test public void testReduceCastAndConsts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    // Make sure constant expressions inside the cast can be reduced
    // in addition to the casts.
    final String sql = "select * from emp\n"
        + "where cast((empno + (10/2)) as int) = 13";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test public <T> void testReduceCaseNullabilityChange() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    try (Hook.Closeable a = Hook.REL_BUILDER_SIMPLIFY.add(Hook.propertyJ(false))) {
      checkPlanning(program,
          "select case when empno = 1 then 1 when 1 IS NOT NULL then 2 else null end as qx "
              + "from emp");
    }
  }

  @Ignore // Calcite does not support INSERT yet
  @Test public void testReduceCastsNullable() throws Exception {
    HepProgram program = new HepProgramBuilder()

        // Simulate the way INSERT will insert casts to the target types
        .addRuleInstance(
            CoerceInputsRule.Config.DEFAULT
                        .withCoerceNames(false)
                        .withConsumerRelClass(LogicalTableModify.class)
                        .toRule())
            // Convert projects to calcs, merge two calcs, and then
            // reduce redundant casts in merged calc.
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)
=======
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "insert into sales.dept(deptno, name)\n"
        + "select empno, cast(job as varchar(128)) from sales.empnullables";
    sql(sql).with(program).check();
  }

  @Test void testReduceCaseWhenWithCast() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RelNode left = relBuilder
        .values(new String[]{"x", "y"}, 1, 2).build();
    RexNode ref = rexBuilder.makeInputRef(left, 0);
    RexNode literal1 = rexBuilder.makeLiteral(1, type, false);
    RexNode literal2 = rexBuilder.makeLiteral(2, type, false);
    RexNode literal3 = rexBuilder.makeLiteral(3, type, false);

    // CASE WHEN x % 2 = 1 THEN x < 2
    //      WHEN x % 3 = 2 THEN x < 1
    //      ELSE x < 3
    final RexNode caseRexNode = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeCall(SqlStdOperatorTable.MOD, ref, literal2), literal1),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal2),
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeCall(SqlStdOperatorTable.MOD, ref, literal3), literal2),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal1),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal3));

    final RexNode castNode = rexBuilder.makeCast(rexBuilder.getTypeFactory().
        createTypeWithNullability(caseRexNode.getType(), true), caseRexNode);
    final RelNode root = relBuilder
        .push(left)
        .project(castNode)
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);

    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
    hepPlanner.setRoot(root);

    RelNode output = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  private void basePushAggThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(AggregateUnionTransposeRule.INSTANCE)
        .build();
    sql("${sql}").with(program).check();
  }

  @Test void testPushSumConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableNOGBYThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMaxNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMinThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushAvgThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumConstantGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullConstantGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountStarGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMaxNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMinGroupingSetsThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushAvgGroupingSetsThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumCountStarGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountFilterThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPullFilterThroughAggregate() throws Exception {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .build();
    HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_FILTER_TRANSPOSE)
        .build();
    final String sql = "select ename, sal, deptno from ("
        + "  select ename, sal, deptno"
        + "  from emp"
        + "  where sal > 5000)"
        + "group by ename, sal, deptno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test void testPullFilterThroughAggregateGroupingSets()
      throws Exception {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .build();
    HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.AGGREGATE_FILTER_TRANSPOSE)
        .build();
    final String sql = "select ename, sal, deptno from ("
        + "  select ename, sal, deptno"
        + "  from emp"
        + "  where sal > 5000)"
        + "group by rollup(ename, sal, deptno)";
    sql(sql).withPre(preProgram).with(program).check();
  }

  private void basePullConstantTroughAggregate() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    sql("${sql}").with(program).check();
  }

  @Test void testPullConstantThroughConstLast() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateSimpleNonNullable() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermuted() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermutedConstFirst() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermutedConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateAllConst()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateAllLiterals()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughUnion()
      throws Exception {
    HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.UNION_PULL_UP_CONSTANTS)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 2, deptno, job from emp as e2";
    sql(sql)
        .withTrim(true)
        .with(program)
        .check();
  }

  @Test void testPullConstantThroughUnion2()
      throws Exception {
    // Negative test: constants should not be pulled up
    HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.UNION_PULL_UP_CONSTANTS)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 1, deptno, job from emp as e2";
    sql(sql).with(program).checkUnchanged();
  }

  @Test void testPullConstantThroughUnion3()
      throws Exception {
    // We should leave at least a single column in each Union input
    HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.UNION_PULL_UP_CONSTANTS)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select 2, 3 from emp as e1\n"
        + "union all\n"
        + "select 2, 3 from emp as e2";
    sql(sql)
        .withTrim(true)
        .with(program)
        .check();
  }

  @Test public void testAggregateProjectMerge() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
            + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
            + "  from emp)\n"
            + "group by x, y");
  }

  @Test public void testAggregateGroupingSetsProjectMerge() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
            + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
            + "  from emp)\n"
            + "group by rollup(x, y)");
  }

  @Test void testAggregateExtractProjectRule() {
    final String sql = "select sum(sal)\n"
        + "from emp";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(Aggregate.class, LogicalTableScan.class,
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).check();
  }

  @Test void testAggregateExtractProjectRuleWithGroupingSets() {
    final String sql = "select empno, deptno, sum(sal)\n"
        + "from emp\n"
        + "group by grouping sets ((empno, deptno),(deptno),(empno))";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(Aggregate.class, LogicalTableScan.class,
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).check();
  }


  /** Test with column used in both grouping set and argument to aggregate
   * function. */
  @Test void testAggregateExtractProjectRuleWithGroupingSets2() {
    final String sql = "select empno, deptno, sum(empno)\n"
        + "from emp\n"
        + "group by grouping sets ((empno, deptno),(deptno),(empno))";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(Aggregate.class, LogicalTableScan.class,
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).check();
  }

  @Test void testAggregateExtractProjectRuleWithFilter() {
    final String sql = "select sum(sal) filter (where empno = 40)\n"
        + "from emp";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    // AggregateProjectMergeRule does not merges Project with Filter.
    // Force match Aggregate on top of Project once explicitly in unit test.
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(
            operand(Aggregate.class,
                operandJ(Project.class, null,
                    new Predicate<Project>() {
                      int matchCount = 0;

                      public boolean test(Project project) {
                        return matchCount++ == 0;
                      }
                    },
                    none())),
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).checkUnchanged();
  }

  @Test public void testAggregateCaseToFilter() {
    final String sql = "select\n"
        + " sum(sal) as sum_sal,\n"
        + " count(distinct case\n"
        + "       when job = 'CLERK'\n"
        + "       then deptno else null end) as count_distinct_clerk,\n"
        + " sum(case when deptno = 10 then sal end) as sum_sal_d10,\n"
        + " sum(case when deptno = 20 then sal else 0 end) as sum_sal_d20,\n"
        + " sum(case when deptno = 30 then 1 else 0 end) as count_d30,\n"
        + " count(case when deptno = 40 then 'x' end) as count_d40,\n"
        + " count(case when deptno = 20 then 1 end) as count_d20\n"
        + "from emp";
    sql(sql).withRule(AggregateCaseToFilterRule.INSTANCE).check();
  }

  @Test public void testPullAggregateThroughUnion() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_UNION_AGGREGATE)
        .build();

    final String sql = "select deptno, job from"
        + " (select deptno, job from emp as e1"
        + " group by deptno,job"
        + "  union all"
        + " select deptno, job from emp as e2"
        + " group by deptno,job)"
        + " group by deptno,job";
    sql(sql).with(program).check();
  }

  @Test void testPullAggregateThroughUnion2() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND)
        .addRuleInstance(CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST)
        .build();

    final String sql = "select deptno, job from"
        + " (select deptno, job from emp as e1"
        + " group by deptno,job"
        + "  union all"
        + " select deptno, job from emp as e2"
        + " group by deptno,job)"
        + " group by deptno,job";
    sql(sql).with(program).check();
  }

  /**
   * Once the bottom aggregate pulled through union, we need to add a Project
   * if the new input contains a different type from the union.
   */
  @Test public void testPullAggregateThroughUnionAndAddProjects() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_UNION_AGGREGATE)
        .build();
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select e.deptno, d.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno, d.deptno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2200">[CALCITE-2200]
   * Infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test public void testJoinPushTransitivePredicatesRule() {
    HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
        .addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
        .build();

    final HepPlanner hepPlanner =
        new HepPlanner(new HepProgramBuilder().build());

    final String sql = "select d.deptno from sales.emp d where d.deptno\n"
        + "IN (select e.deptno from sales.emp e "
        + "where e.deptno = d.deptno or e.deptno = 4)";
    sql(sql).withPre(preProgram).with(hepPlanner).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2205">[CALCITE-2205]
   * One more infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test public void testJoinPushTransitivePredicatesRule2() {
    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
        .addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);

    final String sql = "select n1.SAL\n"
        + "from EMPNULLABLES_20 n1\n"
        + "where n1.SAL IN (\n"
        + "  select n2.SAL\n"
        + "  from EMPNULLABLES_20 n2\n"
        + "  where n1.SAL = n2.SAL or n1.SAL = 4)";
    sql(sql)
        .withDecorrelation(true)
        .with(hepPlanner)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2195">[CALCITE-2195]
   * AggregateJoinTransposeRule fails to aggregate over unique column</a>. */
  @Test public void testPushAggregateThroughJoin6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select sum(B.sal)\n"
        + "from sales.emp as A\n"
        + "join (select distinct sal from sales.emp) as B\n"
        + "on A.sal=B.sal\n";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2278">[CALCITE-2278]
   * AggregateJoinTransposeRule fails to split aggregate call if input contains
   * an aggregate call and has distinct rows</a>. */
  @Test public void testPushAggregateThroughJoinWithUniqueInput() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select A.job, B.mgr, A.deptno,\n"
        + "max(B.hiredate1) as hiredate1, sum(B.comm1) as comm1\n"
        + "from sales.emp as A\n"
        + "join (select mgr, sal, max(hiredate) as hiredate1,\n"
        + "    sum(comm) as comm1 from sales.emp group by mgr, sal) as B\n"
        + "on A.sal=B.sal\n"
        + "group by A.job, B.mgr, A.deptno";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** SUM is the easiest aggregate function to split. */
  @Test public void testPushAggregateSumThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select e.job,sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2105">[CALCITE-2105]
   * AggregateJoinTransposeRule incorrectly makes a SUM NOT NULL when Aggregate
   * has no group keys</a>. */
  @Test public void testPushAggregateSumWithoutGroupKeyThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2108">[CALCITE-2108]
   * AggregateJoinTransposeRule incorrectly splits a SUM0 call when Aggregate
   * has no group keys</a>.
   *
   * <p>Similar to {@link #testPushAggregateSumThroughJoin()},
   * but also uses {@link AggregateReduceFunctionsRule}. */
  @Test public void testPushAggregateSumThroughJoinAfterAggregateReduce() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Push a variety of aggregate functions. */
  @Test public void testPushAggregateFunctionsThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select e.job,\n"
        + "  min(sal) as min_sal, min(e.deptno) as min_deptno,\n"
        + "  sum(sal) + 1 as sum_sal_plus, max(sal) as max_sal,\n"
        + "  sum(sal) as sum_sal_2, count(sal) as count_sal,\n"
        + "  count(mgr) as count_mgr\n"
        + "from sales.emp as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Push a aggregate functions into a relation that is unique on the join
   * key. */
  @Test public void testPushAggregateThroughJoinDistinct() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql = "select d.name,\n"
        + "  sum(sal) as sum_sal, count(*) as c\n"
        + "from sales.emp as e\n"
        + "join (select distinct name from sales.dept) as d\n"
        + "  on e.job = d.name\n"
        + "group by d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Push count(*) through join, no GROUP BY. */
  @Test public void testPushAggregateSumNoGroup() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();
    final String sql =
        "select count(*) from sales.emp join sales.dept on job = name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3076">[CALCITE-3076]
   * AggregateJoinTransposeRule throws error for unique under aggregate keys when
   * generating merged calls</a>.*/
  @Test public void testPushAggregateThroughJoinOnEmptyLogicalValues() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .build();

    final String sql =
        "select count(*) volume, sum(C1.sal) C1_sum_sal "
            + "from (select sal, ename from sales.emp where 1=2) C1 "
            + "inner join (select ename from sales.emp) C2   "
            + "on C1.ename = C2.ename ";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2249">[CALCITE-2249]
   * AggregateJoinTransposeRule generates inequivalent nodes if Aggregate relNode contains
   * distinct aggregate function.</a>. */
  @Test public void testPushDistinctAggregateIntoJoin() throws Exception {
    final HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
            .build();
    final String sql =
            "select count(distinct sal) from sales.emp join sales.dept on job = name";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate.
   */
  @Test public void testAggregateMerge1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select deptno c, min(y), max(z) z,\n"
        + "sum(r), sum(m) n, sum(x) sal from (\n"
        + "   select deptno, ename, sum(sal) x, max(sal) z,\n"
        + "      min(sal) y, count(hiredate) m, count(mgr) r\n"
        + "   from sales.emp group by deptno, ename) t\n"
        + "group by deptno";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate, top aggregate is not simple aggregate.
   */
  @Test public void testAggregateMerge2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select deptno, empno, sum(x), sum(y)\n"
        + "from (\n"
        + "  select ename, empno, deptno, sum(sal) x, count(mgr) y\n"
        + "    from sales.emp\n"
        + "  group by deptno, ename, empno) t\n"
        + "group by grouping sets(deptno, empno)";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since lower aggregate is not simple aggregate.
   */
  @Test public void testAggregateMerge3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select deptno, sum(x) from (\n"
        + " select ename, deptno, sum(sal) x from\n"
        + "   sales.emp group by cube(deptno, ename)) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since it contains distinct aggregate
   * function.
   */
  @Test public void testAggregateMerge4() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select deptno, sum(x) from (\n"
        + "  select ename, deptno, count(distinct sal) x\n"
        + "    from sales.emp group by deptno, ename) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since AVG doesn't support splitting.
   */
  @Test public void testAggregateMerge5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select deptno, avg(x) from (\n"
        + "  select mgr, deptno, avg(sal) x from\n"
        + "    sales.emp group by deptno, mgr) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since top agg has no group key, and
   * lower agg function is COUNT.
   */
  @Test public void testAggregateMerge6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select sum(x) from (\n"
        + "select mgr, deptno, count(sal) x from\n"
        + "sales.emp group by deptno, mgr) t";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since top agg contains empty grouping set,
   * and lower agg function is COUNT.
   */
  @Test public void testAggregateMerge7() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select mgr, deptno, sum(x) from (\n"
        + "  select mgr, deptno, count(sal) x from\n"
        + "    sales.emp group by deptno, mgr) t\n"
        + "group by cube(mgr, deptno)";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate, since both top and bottom aggregates
   * contains empty grouping set and they are mergable.
   */
  @Test public void testAggregateMerge8() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(CoreRules.AGGREGATE_MERGE)
        .build();
    final String sql = "select sum(x) x, min(y) z from (\n"
        + "  select sum(sal) x, min(sal) y from sales.emp)";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and all aggregate functions are splittable.
   */
  @Test public void testAggregateRemove1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select empno, sum(sal), min(sal), max(sal), "
        + "bit_and(distinct sal), bit_or(sal), count(distinct sal) "
        + "from sales.emp group by empno, deptno\n";
    checkPlanning(program, sql);
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and there are no aggregate functions.
   */
  @Test public void testAggregateRemove2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select distinct empno, deptno from sales.emp\n";
    checkPlanning(program, sql);
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and all aggregate functions are splittable. Count
   * aggregate function should be transformed to CASE function call
   * because mgr is nullable.
   */
  @Test public void testAggregateRemove3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select empno, count(mgr) "
        + "from sales.emp group by empno, deptno\n";
    checkPlanning(program, sql);
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove aggregate because avg is not splittable.
   */
  @Test public void testAggregateRemove4() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select empno, max(sal), avg(sal) "
        + "from sales.emp group by empno, deptno\n";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove non-simple aggregates.
   */
  @Test public void testAggregateRemove5() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select empno, deptno, sum(sal) "
        + "from sales.emp group by cube(empno, deptno)\n";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove aggregate because deptno is not unique.
   */
  @Test public void testAggregateRemove6() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select deptno, max(sal) "
        + "from sales.emp group by deptno\n";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /**
   * The top Aggregate should be removed -- given "deptno=100",
   * the input of top Aggregate must be already distinct by "mgr"
   */
  @Test public void testAggregateRemove7() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = ""
        + "select mgr, sum(sum_sal)\n"
        + "from\n"
        + "(select mgr, deptno, sum(sal) sum_sal\n"
        + " from sales.emp\n"
        + " group by mgr, deptno)\n"
        + "where deptno=100\n"
        + "group by mgr";
    sql(sql)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2712">[CALCITE-2712]
   * Should remove the left join since the aggregate has no call and
   * only uses column in the left input of the bottom join as group key.</a>. */
  @Test public void testAggregateJoinRemove1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select distinct e.deptno from sales.emp e\n"
            + "left outer join sales.dept d on e.deptno = d.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but has aggregate
   * call with distinct. */
  @Test public void testAggregateJoinRemove2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select e.deptno, count(distinct e.job) from sales.emp e\n"
            + "left outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by e.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but should not
   * remove the left join since the aggregate uses column in the right
   * input of the bottom join. */
  @Test public void testAggregateJoinRemove3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select e.deptno, count(distinct d.name) from sales.emp e\n"
            + "left outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by e.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but right join. */
  @Test public void testAggregateJoinRemove4() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select distinct d.deptno from sales.emp e\n"
            + "right outer join sales.dept d on e.deptno = d.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove2()} but right join. */
  @Test public void testAggregateJoinRemove5() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select d.deptno, count(distinct d.name) from sales.emp e\n"
            + "right outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by d.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove3()} but right join. */
  @Test public void testAggregateJoinRemove6() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select d.deptno, count(distinct e.job) from sales.emp e\n"
            + "right outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by d.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the bottom join since the aggregate has no aggregate
   * call. */
  @Test public void testAggregateJoinRemove7() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT distinct e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }


  /** Similar to {@link #testAggregateJoinRemove7()} but has aggregate
   * call. */
  @Test public void testAggregateJoinRemove8() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, COUNT(DISTINCT d2.name)\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno\n"
        + "GROUP BY e.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove7()} but use columns in
   * the right input of the top join. */
  @Test public void testAggregateJoinRemove9() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT distinct e.deptno, d2.name\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the aggregate uses column in the
   * right input of bottom join. */
  @Test public void testAggregateJoinRemove10() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, COUNT(DISTINCT d1.name, d2.name)\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno\n"
        + "GROUP BY e.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the bottom join since the project uses column in the
   * right input of bottom join. */
  @Test public void testProjectJoinRemove1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, d2.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the project uses column in the
   * left input of bottom join. */
  @Test public void testProjectJoinRemove2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, d1.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the right join keys of bottom
   * join are not unique. */
  @Test public void testProjectJoinRemove3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e1.deptno, d.deptno\n"
        + "FROM sales.emp e1\n"
        + "LEFT JOIN sales.emp e2 ON e1.deptno = e2.deptno\n"
        + "LEFT JOIN sales.dept d ON e1.deptno = d.deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the left join since the join key of the right input is
   * unique. */
  @Test public void testProjectJoinRemove4() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the left join since the join key of the right input is
   * not unique. */
  @Test public void testProjectJoinRemove5() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e1.deptno\n"
        + "FROM sales.emp e1\n"
        + "LEFT JOIN sales.emp e2 ON e1.deptno = e2.deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the left join since the project use columns in the right
   * input of the join. */
  @Test public void testProjectJoinRemove6() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, d.name\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the right join since the join key of the left input is
   * unique. */
  @Test public void testProjectJoinRemove7() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the right join since the join key of the left input is
   * not unique. */
  @Test public void testProjectJoinRemove8() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e2.deptno\n"
        + "FROM sales.emp e1\n"
        + "RIGHT JOIN sales.emp e2 ON e1.deptno = e2.deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the right join since the project uses columns in the
   * left input of the join. */
  @Test public void testProjectJoinRemove9() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, d.name\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  @Test public void testSwapOuterJoin() {
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(CoreRules.JOIN_COMMUTE_OUTER)
        .build();
    checkPlanning(program,
        "select 1 from sales.dept d left outer join sales.emp e"
            + " on d.deptno = e.deptno");
  }

  @Test public void testPushJoinCondDownToProject() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS)
        .build();
    checkPlanning(program,
        "select d.deptno, e.deptno from sales.dept d, sales.emp e"
            + " where d.deptno + 10 = e.deptno * 2");
  }

  @Test public void testSortJoinTranspose1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();

    final String sql = "select * from sales.emp e left join (\n"
            + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
            + "order by sal limit 10";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4616">[CALCITE-4616]
   * AggregateUnionTransposeRule causes row type mismatch when some inputs have
   * unique grouping key</a>. */
  @Test public void testAggregateUnionTransposeWithOneInputUnique() {
    final String sql = "select deptno, SUM(t) from (\n"
        + "select deptno, 1 as t from sales.emp e1\n"
        + "union all\n"
        + "select distinct deptno, 2 as t from sales.emp e2)\n"
        + "group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .check();
  }

  /** If all inputs to UNION are already unique, AggregateUnionTransposeRule is
   * a no-op. */
  @Test public void testAggregateUnionTransposeWithAllInputsUnique() {
    final String sql = "select deptno, SUM(t) from (\n"
        + "select distinct deptno, 1 as t from sales.emp e1\n"
        + "union all\n"
        + "select distinct deptno, 2 as t from sales.emp e2)\n"
        + "group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .checkUnchanged();
  }

  @Test public void testSortJoinTranspose2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinTranspose3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();
    // This one cannot be pushed down
    final String sql = "select * from sales.emp e left join (\n"
        + "  select * from sales.dept) d on e.deptno = d.deptno\n"
        + "order by sal, name limit 10";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql, true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-931">[CALCITE-931]
   * Wrong collation trait in SortJoinTransposeRule for right joins</a>. */
  @Test public void testSortJoinTranspose4() {
    // Create a customized test with RelCollation trait in the test cluster.
    Tester tester = new TesterImpl(getDiffRepos(), true, true, false, false,
        true, null, null) {
      @Override public RelOptPlanner createPlanner() {
        return new MockRelOptPlanner(Contexts.empty()) {
          @Override public List<RelTraitDef> getRelTraitDefs() {
            return ImmutableList.of(RelCollationTraitDef.INSTANCE);
          }
          @Override public RelTraitSet emptyTraitSet() {
            return RelTraitSet.createEmpty().plus(
                RelCollationTraitDef.INSTANCE.getDefault());
          }
        };
      }
    };

    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    sql(sql).withPre(preProgram)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .check();
  }

  @Test void testSortJoinTranspose3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    sql(sql).withTester(t -> tester)
        .withPre(preProgram)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1498">[CALCITE-1498]
   * Avoid LIMIT with trivial ORDER BY being pushed through JOIN endlessly</a>. */
  @Test void testSortJoinTranspose5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();
    // SortJoinTransposeRule should not be fired again.
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "limit 10";
    sql(sql).withPre(preProgram)
        .with(program)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test void testSortJoinTranspose6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_JOIN_TRANSPOSE)
        .build();
    // This one cannot be pushed down
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "left join sales.emp e using (deptno) order by d.deptno offset 1";
    sql(sql)
        .withPre(preProgram)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  @Test public void testSortProjectTranspose1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as integer) offset 1";
    sql(sql).withRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .check();
  }

  @Test public void testSortProjectTranspose2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as double) offset 1";
    sql(sql).withRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .check();
  }

  @Test public void testSortProjectTranspose3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    // This one cannot be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as varchar(10)) offset 1";
    sql(sql).withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1023">[CALCITE-1023]
   * Planner rule that removes Aggregate keys that are constant</a>. */
  @Test void testAggregateConstantKeyRule() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .check();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where reduction is not
   * possible because "deptno" is the only key. */
  @Test void testAggregateConstantKeyRule2() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .checkUnchanged();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where both keys are
   * constants but only one can be removed. */
  @Test void testAggregateConstantKeyRule3() {
    final String sql = "select job\n"
        + "from sales.emp\n"
        + "where sal is null and job = 'Clerk'\n"
        + "group by sal, job\n"
        + "having count(*) > 3";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .check();
  }

  @Test public void testReduceExpressionsNot() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();
    checkPlanUnchanged(new HepPlanner(program),
        "select * from (values (false),(true)) as q (col1) where not(col1)");
  }

  private Sql checkSubQuery(String sql) {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .build();
    return sql(sql).with(new HepPlanner(program)).expand(false);
  }

  /** Tests expanding a sub-query, specifically an uncorrelated scalar
   * sub-query in a project (SELECT clause). */
  @Test void testExpandProjectScalar() throws Exception {
    final String sql = "select empno,\n"
        + "  (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql).check();
  }

  @Test public void testSelectNotInCorrelated() {
    final String sql = "select sal,\n"
        + " empno NOT IN (\n"
        + " select deptno from dept\n"
        + "   where emp.job=dept.name)\n"
        + " from emp";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1493">[CALCITE-1493]
   * Wrong plan for NOT IN correlated queries</a>. */
  @Test void testWhereNotInCorrelated() {
    final String sql = "select sal from emp\n"
        + "where empno NOT IN (\n"
        + "  select deptno from dept\n"
        + "  where emp.job = dept.name)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereNotInCorrelated2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno NOT IN\n"
        + "   (select empno from (select ename, empno, sal as r from emp) e2\n"
        + "    where r > 2 and e1.ename= e2.ename)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testAll() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > ALL (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testSome() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > SOME (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for testing type created by SubQueryRemoveRule: an
   * ANY sub-query is non-nullable therefore plan should have cast. */
  @Test public void testAnyInProjectNonNullable() {
    final String sql = "select name, deptno > ANY (\n"
        + "  select deptno from emp)\n"
        + "from dept";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for testing type created by SubQueryRemoveRule; an
   * ANY sub-query is nullable therefore plan should not have cast. */
  @Test public void testAnyInProjectNullable() {
    final String sql = "select deptno, name = ANY (\n"
        + "  select mgr from emp)\n"
        + "from dept";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testSelectAnyCorrelated() {
    final String sql = "select empno > ANY (\n"
        + "  select deptno from dept where emp.job = dept.name)\n"
        + "from emp\n";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testWhereAnyCorrelatedInSelect() {
    final String sql =
        "select * from emp where empno > ANY (\n"
            + "  select deptno from dept where emp.job = dept.name)\n";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testSomeWithEquality() {
    final String sql = "select * from emp e1\n"
        + "  where e1.deptno = SOME (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testSomeWithEquality2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.ename= SOME (select name from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1546">[CALCITE-1546]
   * Sub-queries connected by OR</a>. */
  @Test void testWhereOrSubQuery() {
    final String sql = "select * from emp\n"
        + "where sal = 4\n"
        + "or empno NOT IN (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testExpandProjectIn() throws Exception {
    final String sql = "select empno,\n"
        + "  deptno in (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandProjectInNullable() throws Exception {
    final String sql = "with e2 as (\n"
        + "  select empno, case when true then deptno else null end as deptno\n"
        + "  from sales.emp)\n"
        + "select empno,\n"
        + "  deptno in (select deptno from e2 where empno < 20) as d\n"
        + "from e2";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandProjectInComposite() throws Exception {
    final String sql = "select empno, (empno, deptno) in (\n"
        + "    select empno, deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandProjectExists() throws Exception {
    final String sql = "select empno,\n"
        + "  exists (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandFilterScalar() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test void testExpandFilterIn() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where deptno in (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test void testExpandFilterInComposite() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  /** An IN filter that requires full 3-value logic (true, false, unknown). */
  @Test void testExpandFilterIn3Value() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where empno\n"
        + " < case deptno in (select case when true then deptno else null end\n"
        + "                   from sales.emp where empno < 20)\n"
        + "   when true then 10\n"
        + "   when false then 20\n"
        + "   else 30\n"
        + "   end";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** An EXISTS filter that can be converted into true/false. */
  @Test void testExpandFilterExists() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test void testExpandFilterExistsSimple() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test void testExpandFilterExistsSimpleAnd() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "and emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test void testExpandJoinScalar() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)";
    checkSubQuery(sql).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3121">[CALCITE-3121]
   * VolcanoPlanner hangs due to subquery with dynamic star</a>. */
=======
   * VolcanoPlanner hangs due to sub-query with dynamic star</a>. */
  @Test public void testSubQueryWithDynamicStarHang() {
    String sql = "select n.n_regionkey from (select * from "
        + "(select * from sales.customer) t) n where n.n_nationkey >1";

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    PushProjector.ExprCondition exprCondition = expr -> {
      if (expr instanceof RexCall) {
        RexCall call = (RexCall) expr;
        return "item".equals(call.getOperator().getName().toLowerCase(Locale.ROOT));
      }
      return false;
    };
    RuleSet ruleSet =
        RuleSets.ofList(
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_MERGE,
            CoreRules.PROJECT_MERGE,
            ProjectFilterTransposeRule.Config.DEFAULT
                .withOperandFor(Project.class, Filter.class)
                .withPreserveExprCondition(exprCondition)
                .toRule(),
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3188">[CALCITE-3188]
   * IndexOutOfBoundsException in ProjectFilterTransposeRule when executing SELECT COUNT(*)</a>. */
  @Test public void testProjectFilterTransposeRuleOnEmptyRowType() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select `empty` from emp
    // where emp.deptno = 20
    RelNode relNode = relBuilder.scan("EMP")
        .filter(relBuilder
            .equals(
                relBuilder.field(1, 0, "DEPTNO"),
                relBuilder.literal(20)))
        .project(ImmutableList.of())
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Ignore("[CALCITE-1045]")
  @Test public void testExpandJoinIn() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on emp.deptno in (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Disabled("[CALCITE-1045]")
  @Test void testExpandJoinInComposite() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (emp.empno, dept.deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Test void testExpandJoinExists() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on exists (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Test void testDecorrelateExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1511">[CALCITE-1511]
   * AssertionError while decorrelating query with two EXISTS
   * sub-queries</a>. */
  @Test void testDecorrelateTwoExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)\n"
        + "AND NOT EXISTS (\n"
        + "  select * from emp ee where ee.job = emp.job AND ee.sal=34)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2028">[CALCITE-2028]
   * Un-correlated IN sub-query should be converted into a Join,
   * rather than a Correlate without correlation variables </a>. */
  @Test void testDecorrelateUncorrelatedInAndCorrelatedExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "WHERE job in (\n"
        + "  select job from emp ee where ee.sal=34)"
        + "AND EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)\n";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1537">[CALCITE-1537]
   * Unnecessary project expression in multi-sub-query plan</a>. */
  @Test void testDecorrelateTwoIn() throws Exception {
    final String sql = "select sal\n"
        + "from sales.emp\n"
        + "where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)\n"
        + "AND empno IN (\n"
        + "  select empno from emp e where emp.ename = e.ename)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1045">[CALCITE-1045]
   * Decorrelate sub-queries in Project and Join</a>, with the added
   * complication that there are two sub-queries. */
  @Disabled("[CALCITE-1045]")
  @Test void testDecorrelateTwoScalar() throws Exception {
    final String sql = "select deptno,\n"
        + "  (select min(1) from emp where empno > d.deptno) as i0,\n"
        + "  (select min(0) from emp\n"
        + "    where deptno = d.deptno and ename = 'SMITH') as i1\n"
        + "from dept as d";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereInJoinCorrelated() {
    final String sql = "select empno from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where e.sal in (\n"
        + "  select e2.sal from emp as e2 where e2.deptno > e.deptno)";
    checkSubQuery(sql).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1494">[CALCITE-1494]
   * Inefficient plan for correlated sub-queries</a>. In "planAfter", there
   * must be only one scan each of emp and dept. We don't need a separate
   * value-generator for emp.job. */
  @Test void testWhereInCorrelated() {
    final String sql = "select sal from emp where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereExpressionInCorrelated() {
    final String sql = "select ename from (\n"
        + "  select ename, deptno, sal + 1 as salPlus from emp) as e\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = e.salPlus)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereExpressionInCorrelated2() {
    final String sql = "select name from (\n"
        + "  select name, deptno, deptno - 10 as deptnoMinus from dept) as d\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = d.deptnoMinus)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testExpandWhereComparisonCorrelated() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp as e\n"
        + "where sal = (\n"
        + "  select max(sal) from sales.emp e2 where e2.empno = e.empno)";
    checkSubQuery(sql).check();
  }

  @Test void testCustomColumnResolvingInNonCorrelatedSubQuery() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 in (\n"
        + "  select f1.c0 from struct.t t2)";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .build();
    sql(sql)
        .withTrim(true)
        .expand(false)
        .with(program)
        .check();
  }

  @Test void testCustomColumnResolvingInCorrelatedSubQuery() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 = (\n"
        + "  select max(f1.c0) from struct.t t2 where t1.k0 = t2.k0)";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .build();
    sql(sql)
        .withTrim(true)
        .expand(false)
        .with(program)
        .check();
  }

  @Test void testCustomColumnResolvingInCorrelatedSubQuery2() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 in (\n"
        + "  select f1.c0 from struct.t t2 where t1.c2 = t2.c2)";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .build();
    sql(sql)
        .withTrim(true)
        .expand(false)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2744">[CALCITE-2744]
   * RelDecorrelator use wrong output map for LogicalAggregate decorrelate</a>. */
  @Test public void testDecorrelateAggWithConstantGroupKey() {
    final String sql = "SELECT * FROM emp A where sal in \n"
        + "(SELECT max(sal) FROM emp B where A.mgr = B.empno group by deptno, 'abc')";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /** Test case for CALCITE-2744 for aggregate decorrelate with multi-param agg call
   * but without group key. */
<<<<<<< HEAD
  @Disabled
  @Test void testDecorrelateAggWithMultiParamsAggCall() {
=======
  @Ignore
  @Test public void testDecorrelateAggWithMultiParamsAggCall() {
    final String sql = "SELECT * FROM (SELECT MY_AVG(sal, 1) AS c FROM emp) as m,\n"
                       + " LATERAL TABLE(ramp(m.c)) AS T(s)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .checkUnchanged();
  }

  /** Same as {@link #testDecorrelateAggWithMultiParamsAggCall}
   * but with constant grouping key. */
  @Ignore
  @Test public void testDecorrelateAggWithMultiParamsAggCall2() {
    final String sql = "SELECT * FROM "
        + "(SELECT MY_AVG(sal, 1) AS c FROM emp group by empno, 'abc') as m,\n"
        + " LATERAL TABLE(ramp(m.c)) AS T(s)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * Converting predicates on date dimension columns into date ranges</a>,
   * specifically a rule that converts {@code EXTRACT(YEAR FROM ...) = constant}
   * to a range. */
  @Test void testExtractYearToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014";
    final Context context =
        Contexts.of(new CalciteConnectionConfigImpl(new Properties()));
    sql(sql).withRule(DateRangeRules.FILTER_INSTANCE)
        .withContext(context)
        .check();
  }

  @Test void testExtractYearMonthToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014"
        + "and extract(month from birthdate) = 4";
    final Context context =
        Contexts.of(new CalciteConnectionConfigImpl(new Properties()));
    sql(sql).withRule(DateRangeRules.FILTER_INSTANCE)
        .withContext(context)
        .check();
  }

  /**
   * Rule to transform {@link LogicalFilter} into
   * custom MyFilter
   */

  /**
   * Custom implementation of {@link SetOp} for use
   * in test case to verify that {@link FilterSetOpTransposeRule}
   * can be created with any {@link SetOp} and not limited to
   * {@link org.apache.calcite.rel.core.SetOp}
   */
  private static class MySetOp extends SetOp {

    MySetOp(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        SqlKind k,
        boolean all) {
      super(cluster, traitSet, inputs, k, all);
    }

    @Override public MySetOp copy(RelTraitSet traitSet, List<RelNode> inputs,
                        boolean all) {
      return new MySetOp(getCluster(), traitSet, inputs, SqlKind.UNION, all);
    }
  }

  /**
   * Rule to transform {@link SetOp} into custom
   * MySetOp
   */
  private static class MySetOpRule extends RelOptRule {
    static final MySetOpRule INSTANCE =
        new MySetOpRule(SetOp.class, RelFactories.LOGICAL_BUILDER);

    private MySetOpRule(Class<? extends SetOp> clazz,
                          RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final SetOp setOp = call.rel(0);
      final List<RelNode> inputs = setOp.getInputs();
      final MySetOp newMySetOp =
          new MySetOp(inputs.get(0).getCluster(), setOp.getTraitSet(), inputs, SqlKind.UNION, true);
      call.transformTo(newMySetOp);
    }
  }

  /**
   * Test case to push custom Filter past custom union.
   */
  @Disabled
  @Test void testFilterSetOpTranspose() {

    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleCollection(Arrays.asList(MyFilterRule.INSTANCE, MySetOpRule.INSTANCE))
        .build();

    List<RelOptRule> rules = Arrays.asList(
        CoreRules.FILTER_SET_OP_TRANSPOSE, // default
        FilterSetOpTransposeRule.Config.DEFAULT
                .withOperandSupplier(b0 ->
                        b0.operand(MyFilter.class).oneInput(b1 ->
                                b1.operand(MySetOp.class).anyInputs()))
                .as(FilterSetOpTransposeRule.Config.class)
                .toRule());

    HepProgram program = new HepProgramBuilder()
        .addRuleCollection(rules)
        .build();

    final String sql = "SELECT e3.empno AS \"$C0\",\n"
        + "MIN(e3.sal) AS \"$C1\",\n"
        + "e3.slacker AS \"$C2\"\n"
        + "FROM ("
        + "   SELECT empno,\n"
        + "          sal,\n"
        + "          slacker\n"
        + "     FROM emp as e1\n"
        + "   UNION ALL\n"
        + "     (SELECT empno, deptno, slacker\n"
        + "          FROM emp as e2)) AS e3\n"
        + "WHERE empno > 40\n"
        + "GROUP BY empno, slacker\n";

    sql(sql).withPre(preProgram).with(program).check();
  }
  @Test void testPartialExpressionReduction() {
    final String sql = "select "
        + "EXTRACT(SECOND FROM CAST(CASE WHEN TRUE THEN {ts '2018-01-01 01:23:45'} ELSE NULL END AS TIMESTAMP)) = "
        + "EXTRACT(SECOND FROM d) "
        + "FROM (VALUES({ts '2018-01-01 01:23:45'})) tbl(d)";

    checkPlanning(CoreRules.PROJECT_REDUCE_EXPRESSIONS, sql);
  }

  @Test void testRangeSimplification() {
    final String sql = "select *"
        + "FROM (VALUES({ts '2018-01-01 01:23:45'})) tbl(d) "
        + "WHERE d = '2018-01-01 01:23:45' AND d > '2018-01-01 01:23:45'";


    checkPlanning(CoreRules.FILTER_REDUCE_EXPRESSIONS, sql);
  }

  @Test void testDecorrelateUnion0() {
    final String sql = ""
        + "SELECT deptno FROM dept where exists\n"
        + "(SELECT 1 FROM emp where sal < 100 and emp.deptno=dept.deptno\n"
        + "UNION ALL\n"
        + "SELECT 1 FROM bonus where bonus.comm=dept.deptno)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Input frames with the same correlated variables and old-new output mappings,
   * but correlated variables referenced by different columns.
   */
  @Test void testDecorrelateUnion1() {
    final String sql = "SELECT * FROM emp where exists \n"
        + "(SELECT 1 FROM dept where deptno = emp.deptno\n"
        + "UNION ALL\n"
        + "SELECT 1 FROM dept where deptno + 1 = emp.deptno)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Input frames with different correlated variables
   * and referenced by different columns;
   */
  @Test void testDecorrelateUnion2() {
    final String sql = "SELECT * FROM emp where exists \n"
        + "(SELECT 1 FROM dept where deptno = emp.deptno\n"
        + "UNION ALL\n"
        + "SELECT 1 FROM dept where name = emp.ename)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Input frames with the same correlated variables,
   * but different old-new output mappings.
   */
  @Test void testDecorrelateUnion3() {
    final String sql = "SELECT * FROM emp A where (deptno, sal) in \n"
        + "(SELECT deptno, max(sal) FROM emp B where A.mgr = B.empno group by deptno\n"
        + "UNION ALL\n"
        + "SELECT deptno, sal FROM emp B where A.mgr = B.empno)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4317">[CALCITE-4317]
   * RelFieldTrimmer after trimming all the fields in an aggregate
   * should not return a zero field Aggregate</a>.
   */
  @Test void testProjectJoinTransposeRuleOnAggWithNoFieldsWithTrimmer() {
    final RelBuilder relBuilder =
        RelBuilderTest.createBuilder(c -> c.withPruneInputOfAggregate(false));
    // Build a rel equivalent to sql:
    // SELECT name FROM (SELECT count(*) cnt_star, count(empno) cnt_en FROM sales.emp)
    // cross join sales.dept
    // limit 10

    RelNode left = relBuilder.scan("DEPT").build();

    // Create a fresh RelBuilder for the aggregate operation to ensure
    // configuration is applied
    final RelBuilder rightBuilder =
        RelBuilderTest.createBuilder(c -> c.withPruneInputOfAggregate(false));

    RelNode right = rightBuilder.scan("EMP")
        .project(
          ImmutableList.of(rightBuilder.getRexBuilder().makeExactLiteral(BigDecimal.ZERO)),
          ImmutableList.of("DUMMY"))
        .aggregate(
          rightBuilder.groupKey(),
          rightBuilder.count(false, "DUMMY_COUNT", rightBuilder.field(0)))
        .build();

    RelNode plan = relBuilder.push(left)
        .push(right)
        .join(JoinRelType.INNER,
          relBuilder.getRexBuilder().makeLiteral(true))
        .project(relBuilder.field("DEPTNO"))
        .build();

    final String planBeforeTrimming = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planBeforeTrimming",
        "${planBeforeTrimming}", planBeforeTrimming);

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

    // Create a RelBuilderFactory that disables pruneInputOfAggregate
    final RelBuilderFactory relBuilderFactory =
        RelBuilder.proto(Contexts.of(RelBuilder.Config.DEFAULT.withPruneInputOfAggregate(false)));

    Tester tester = new TesterImpl(getDiffRepos(), false, false, false,
            false, false, null, null)
            .withClusterFactory(
              relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()))
            .withConfig(SqlToRelConverter.configBuilder()
                .withRelBuilderFactory(relBuilderFactory)
                .withTrimUnusedFields(true)
                .build())
            .withContext(Contexts.of(RelBuilder.Config.DEFAULT.withPruneInputOfAggregate(false)));

    plan = tester.trimRelNode(plan);

    final String planAfterTrimming = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planAfterTrimming", "${planAfterTrimming}", planAfterTrimming);

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(plan);
    RelNode output = hepPlanner.findBestExp();
    final String finalPlan = NL + RelOptUtil.toString(output);
    getDiffRepos().assertEquals("finalPlan", "${finalPlan}", finalPlan);
  }

  @Test void testProjectReduceExpressionsWithIsNotDistinctFrom() {

    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // Build a rel equivalent to sql:
    // SELECT hiredate from emp where hiredate is not distinct from '2020-12-11'

    final RexBuilder rexB = relBuilder.getRexBuilder();

    final RexNode dateLiteral = dateLiteral(rexB, 2020, 12, 11);

    RexNode dateInput4 = dateInput(rexB, 4, true);

    final RexNode filterCond = rexB.makeCall(IS_NOT_DISTINCT_FROM,
            dateInput4,
            dateLiteral);

    RelNode plan = relBuilder.scan("EMP")
            .filter(filterCond)
            .project(relBuilder.field("HIREDATE"))
            .build();

    final String planBefore = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planBefore",
            "${planBefore}", planBefore);

    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
            .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    final RexExecutorImpl executor =
               new RexExecutorImpl(Schemas.createDataContext(null, null));
    plan.getCluster().getPlanner().setExecutor(executor);
    hepPlanner.setRoot(plan);
    RelNode output = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(output);

    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testJoinReduceExpressionsWithIsNotDistinctFrom() {

    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // Build a rel equivalent to sql:
    // SELECT hiredate from emp e1 join (select hiredate from emp
    // where hiredate is not distinct from '2020-12-11') e2
    // on e1.hiredate = e2.hiredate

    final RexBuilder rexB = relBuilder.getRexBuilder();

    RexNode dateInput4 = dateInput(rexB, 4, true);

    RexNode dateInput8 = dateInput(rexB, 8, true);

    final RexNode dateLiteral = dateLiteral(rexB, 2020, 12, 11);

    final RexNode filterCond = rexB.makeCall(IS_NOT_DISTINCT_FROM,
            dateInput4,
            dateLiteral);

    RelNode right = relBuilder.scan("EMP")
            .filter(filterCond)
            .project(relBuilder.field("HIREDATE"))
            .build();

    RelNode plan = relBuilder.scan("EMP")
            .push(right)
            .join(JoinRelType.INNER,
                    rexB.makeCall(EQUALS, dateInput4, dateInput8))
            .build();

    final String planBefore = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planBefore",
            "${planBefore}", planBefore);

    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
            .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    final RexExecutorImpl executor =
            new RexExecutorImpl(Schemas.createDataContext(null, null));
    plan.getCluster().getPlanner().setExecutor(executor);
    hepPlanner.setRoot(plan);
    RelNode output = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(output);

    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testFilterRemoveIsNotDistinctFromRule() {
    final DiffRepository diffRepos = getDiffRepos();
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode root = builder
        .scan("EMP")
        .filter(
            builder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            builder.field("DEPTNO"), builder.literal(20)))
        .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  private RexNode dateLiteral(RexBuilder rexB, int year, int month, int day) {
    return rexB.makeDateLiteral(new DateString(year, month, day));
  }

  private RexInputRef dateInput(RexBuilder rexB, int input, boolean nullable) {
    return rexB.makeInputRef(rexB.getTypeFactory()
            .createTypeWithNullability(
                    rexB.getTypeFactory().createSqlType(SqlTypeName.DATE), nullable), input);
  }

  @Test void testOversimplifiedCaseStatement() {
    String sql = "select * from emp "
        + "where MGR > 0 and "
        + "case when MGR > 0 then deptno / MGR else null end > 1";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2726">[CALCITE-2726]
  * ReduceExpressionRule may oversimplify filter conditions containing nulls</a>.
  */
  @Test void testNoOversimplificationBelowIsNull() {
    String sql =
        "select * from emp where ( (empno=1 and mgr=1) or (empno=null and mgr=1) ) is null";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testExchangeRemoveConstantKeysRule() {
    final DiffRepository diffRepos = getDiffRepos();
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode root = builder
        .scan("EMP")
        .filter(
        builder.call(SqlStdOperatorTable.EQUALS,
          builder.field("EMPNO"), builder.literal(10)))
        .exchange(RelDistributions.hash(ImmutableList.of(0)))
        .project(builder.field(0), builder.field(1))
        .sortExchange(RelDistributions.hash(ImmutableList.of(0, 1)),
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1)))
        .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE)
        .addRuleInstance(ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testReduceAverageWithNoReduceSum() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, max(name), avg(deptno), min(name)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testNoReduceAverage() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.noneOf(SqlKind.class);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    String sql = "select name, max(name), avg(deptno), min(name)"
        + " from sales.dept group by name";
    sql(sql).withRule(rule).checkUnchanged();
  }

  @Test void testNoReduceSum() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.noneOf(SqlKind.class);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    String sql = "select name, sum(deptno)"
            + " from sales.dept group by name";
    sql(sql).withRule(rule).checkUnchanged();
  }

  @Test void testReduceAverageAndVarWithNoReduceStddev() {
    // configure rule to reduce AVG and VAR_POP functions
    // other functions like SUM, STDDEV won't be reduced
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.VAR_POP);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testReduceAverageAndSumWithNoReduceStddevAndVar() {
    // configure rule to reduce AVG and SUM functions
    // other functions like VAR_POP, STDDEV_POP won't be reduced
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.SUM);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testReduceAllAggregateFunctions() {
    // configure rule to reduce all used functions
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.SUM,
        SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP, SqlKind.VAR_POP, SqlKind.VAR_SAMP);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " stddev_samp(deptno), var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2803">[CALCITE-2803]
  * Identify expanded IS NOT DISTINCT FROM expression when pushing project past join</a>.
  */
  @Test void testPushProjectWithIsNotDistinctFromPastJoin() {
    final String sql = "select e.sal + b.comm from emp e inner join bonus b\n"
        + "on (e.ename || e.job) IS NOT DISTINCT FROM (b.ename || b.job) and e.deptno = 10";
    sql(sql)
        .withRule(CoreRules.PROJECT_JOIN_TRANSPOSE)
        .check();
  }
  @Test void testDynamicStarWithUnion() {
    String sql = "(select n_nationkey from SALES.CUSTOMER) union all\n"
            + "(select n_name from CUSTOMER_MODIFIABLEVIEW)";

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    RuleSet ruleSet =
        RuleSets.ofList(
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testFilterAndProjectWithMultiJoin() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
          .addRuleCollection(Arrays.asList(MyFilterRule.INSTANCE, MyProjectRule.INSTANCE))
          .build();

    final FilterMultiJoinMergeRule filterMultiJoinMergeRule =
          FilterMultiJoinMergeRule.Config.DEFAULT
          .withOperandFor(MyFilter.class, MultiJoin.class)
              .toRule();

    final ProjectMultiJoinMergeRule projectMultiJoinMergeRule =
          ProjectMultiJoinMergeRule.Config.DEFAULT
              .withOperandFor(MyProject.class, MultiJoin.class)
              .toRule();

    HepProgram program = new HepProgramBuilder()
          .addRuleCollection(
               Arrays.asList(
                      CoreRules.JOIN_TO_MULTI_JOIN,
                      filterMultiJoinMergeRule,
                      projectMultiJoinMergeRule))
          .build();

    sql("select * from emp e1 left outer join dept d on e1.deptno = d.deptno where d.deptno > 3")
        .withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3151">[CALCITE-3151]
   * RexCall's Monotonicity is not considered in determining a Calc's collation</a>
   */
  @Test void testMonotonicityUDF() throws Exception {
    final SqlFunction monotonicityFun =
        new SqlFunction("MONOFUN", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
            OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION) {
          @Override public boolean isDeterministic() {
            return false;
          }

          @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
            return SqlMonotonicity.INCREASING;
          }
        };

    // Build a tree equivalent to the SQL
    // SELECT sal, MONOFUN() AS n FROM emp
    final RelBuilder builder =
        RelBuilder.create(RelBuilderTest.config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("SAL"),
                builder.alias(builder.call(monotonicityFun), "M"))
            .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final RelCollation collationBefore =
        relBefore.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final RelCollation collationAfter =
        relAfter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    assertEquals(collationBefore, collationAfter);
  }

  @Test void testPushFiltertWithIsNotDistinctFromPastJoin() {
    String query = "SELECT * FROM "
        + "emp t1 INNER JOIN "
        + "emp t2 "
        + "ON t1.deptno = t2.deptno "
        + "WHERE t1.ename is not distinct from t2.ename";
    sql(query).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  /**
   * Custom implementation of {@link Filter} for use
   * in test case to verify that {@link FilterMultiJoinMergeRule}
   * can be created with any {@link Filter} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalFilter}
   */
  private static class MyFilter extends Filter {

    MyFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition) {
      super(cluster, traitSet, child, condition);
    }

    public MyFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new MyFilter(getCluster(), traitSet, input, condition);
    }

  }

  /**
   * Rule to transform {@link LogicalFilter} into
   * custom MyFilter
   */
  private static class MyFilterRule extends RelOptRule {
    static final MyFilterRule INSTANCE =
        new MyFilterRule(LogicalFilter.class, RelFactories.LOGICAL_BUILDER);

    private MyFilterRule(Class<? extends Filter> clazz,
        RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalFilter logicalFilter = call.rel(0);
      final RelNode input = logicalFilter.getInput();
      final MyFilter myFilter = new MyFilter(input.getCluster(), input.getTraitSet(), input,
          logicalFilter.getCondition());
      call.transformTo(myFilter);
    }
  }

  /**
   * Custom implementation of {@link Project} for use
   * in test case to verify that {@link ProjectMultiJoinMergeRule}
   * can be created with any {@link Project} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalProject}
   */
  private static class MyProject extends Project {
    MyProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, input, projects, rowType);
    }

    public MyProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new MyProject(getCluster(), traitSet, input, projects, rowType);
    }
  }

  /**
   * Rule to transform {@link LogicalProject} into custom
   * MyProject
   */
  private static class MyProjectRule extends RelOptRule {
    static final MyProjectRule INSTANCE =
        new MyProjectRule(LogicalProject.class, RelFactories.LOGICAL_BUILDER);

    private MyProjectRule(Class<? extends Project> clazz,
        RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalProject logicalProject = call.rel(0);
      final RelNode input = logicalProject.getInput();
      final MyProject myProject = new MyProject(input.getCluster(), input.getTraitSet(), input,
          logicalProject.getChildExps(), logicalProject.getRowType());
      call.transformTo(myProject);
    }
  }

  @Test void testSortJoinCopyInnerJoinOrderBy() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final String sql = "select * from sales.emp join sales.dept on\n"
        + "sales.emp.deptno = sales.dept.deptno order by sal";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopyInnerJoinOrderByLimit() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final String sql = "select * from sales.emp e join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by sal limit 10";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopyInnerJoinOrderByTwoFields() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e join  sales.dept d on\n"
        + " e.deptno = d.deptno order by e.sal,d.name";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderBy() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderByLimitOffset() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno limit 10 offset 2";
    // Do not copy the limit and offset
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderByOffset() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in"
        + " (select e.deptno from sales.emp e) order by d.deptno offset 2";
    // Do not copy the offset
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3296">[CALCITE-3296]
   * Decorrelator gives empty result after decorrelating sort rel with
   * null offset and fetch</a>.
   */
  @Test void testDecorrelationWithSort() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)\n"
        + "order by e1.empno";

    sql(sql)
        .withRule() // empty program
        .expand(true)
        .withDecorrelation(true)
        .checkUnchanged();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3319">[CALCITE-3319]
   * AssertionError for ReduceDecimalsRule </a>
   */
  @Test void testReduceDecimal() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .addRuleInstance(CoreRules.CALC_REDUCE_DECIMALS)
        .build();
    final String sql = "select ename from emp where sal > cast (100.0 as decimal(4, 1))";
    sql(sql).with(program).check();
  }

  @Test void testEnumerableCalcRule() {
    final String sql = "select FNAME, LNAME from SALES.CUSTOMER where CONTACTNO > 10";
    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    RuleSet ruleSet =
        RuleSets.ofList(
            CoreRules.FILTER_TO_CALC,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_CALC_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3404">[CALCITE-3404]
   * Treat agg expressions that can ignore distinct constraint as distinct
   * in AggregateExpandDistinctAggregatesRule
   * when all the other agg expressions are distinct and have same arguments</a>
   */
  @Test void testMaxReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "max(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testMinReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "min(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testBitAndReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "bit_and(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testBitOrReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "bit_or(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testProjectJoinTransposeItem() {
    ProjectJoinTransposeRule projectJoinTransposeRule =
        CoreRules.PROJECT_JOIN_TRANSPOSE.config
            .withOperandFor(Project.class, Join.class)
            .withPreserveExprCondition(skipItem)
            .toRule();

    final String sql = "select t1.c_nationkey[0], t2.c_nationkey[0]\n"
        + "from sales.customer as t1\n"
        + "left outer join sales.customer as t2\n"
        + "on t1.c_nationkey[0] = t2.c_nationkey[0]";

    sql(sql)
        .withTester(t -> createDynamicTester())
        .withRule(projectJoinTransposeRule)
        .check();
  }

  @Test void testSimplifyItemIsNotNull() {
    String query = "select * from sales.customer as t1 where t1.c_nationkey[0] is not null";

    sql(query)
        .withTester(t -> createDynamicTester())
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  @Test void testSimplifyItemIsNull() {
    String query = "select * from sales.customer as t1 where t1.c_nationkey[0] is null";

    sql(query)
        .withTester(t -> createDynamicTester())
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  // Tests for MultiJoinOptimizeBushyRule
  /** Test case for {@link MultiJoinOptimizeBushyRule} with a simple 3-table join. */
  @Test public void testMultiJoinOptimizeBushySimple() {
    HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
        .build();
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno or d.deptno = e2.deptno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /** Test case for {@link MultiJoinOptimizeBushyRule} with a 4-table join. */
  @Test public void testMultiJoinOptimizeBushy4Tables() {
    HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
        .build();
    final String sql = "select e1.ename from emp e1, dept d, emp e2, emp e3\n"
        + "where e1.deptno = d.deptno and (d.deptno = e2.deptno\n"
        + "or e2.empno = e3.empno)";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test public void testMultiJoinOptimizeBushySimpleFilterOnEmp() {
    HepProgram preProgram = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
        .build();
    final String sql = "select e1.ename from emp e1 inner join dept d\n"
        + "on e1.deptno = e1.empno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test public void testMultiJoinOptimizeBushySimpleFilterOnDept() {
    HepProgram preProgram = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
        .build();
    final String sql = "select e1.ename from emp e1 inner join dept d\n"
        + "on d.deptno = 40";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /**
   * Rule to transform {@link LogicalFilter} into
   * custom MyFilter
   */

  /**
   * Custom implementation of {@link SetOp} for use
   * in test case to verify that {@link FilterSetOpTransposeRule}
   * can be created with any {@link SetOp} and not limited to
   * {@link org.apache.calcite.rel.core.SetOp}
   */
  private static class MySetOp extends SetOp {

    MySetOp(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        SqlKind k,
        boolean all) {
      super(cluster, traitSet, inputs, k, all);
    }

    public MySetOp copy(RelTraitSet traitSet, List<RelNode> inputs,
                        boolean all) {
      return new MySetOp(getCluster(), traitSet, inputs, SqlKind.UNION, all);
    }
  }

  /**
   * Rule to transform {@link SetOp} into custom
   * MySetOp
   */
  private static class MySetOpRule extends RelOptRule {
    static final MySetOpRule INSTANCE =
        new MySetOpRule(SetOp.class, RelFactories.LOGICAL_BUILDER);

    private MySetOpRule(Class<? extends SetOp> clazz,
                          RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final SetOp setOp = call.rel(0);
      final List<RelNode> inputs = setOp.getInputs();
      final MySetOp newMySetOp =
          new MySetOp(inputs.get(0).getCluster(), setOp.getTraitSet(), inputs, SqlKind.UNION, true);
      call.transformTo(newMySetOp);
    }
  }

  /**
   * Test case to push custom Filter past custom union.
   */
  @Ignore
  @Test public void testFilterSetOpTranspose() {

    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleCollection(Arrays.asList(MyFilterRule.INSTANCE, MySetOpRule.INSTANCE))
        .build();

    List<RelOptRule> rules = Arrays.asList(
        CoreRules.FILTER_SET_OP_TRANSPOSE, // default
        FilterSetOpTransposeRule.Config.DEFAULT
                .withOperandSupplier(b0 ->
                        b0.operand(MyFilter.class).oneInput(b1 ->
                                b1.operand(MySetOp.class).anyInputs()))
                .as(FilterSetOpTransposeRule.Config.class)
                .toRule());

    HepProgram program = new HepProgramBuilder()
        .addRuleCollection(rules)
        .build();

    final String sql = "SELECT e3.empno AS \"$C0\",\n"
        + "MIN(e3.sal) AS \"$C1\",\n"
        + "e3.slacker AS \"$C2\"\n"
        + "FROM ("
        + "   SELECT empno,\n"
        + "          sal,\n"
        + "          slacker\n"
        + "     FROM emp as e1\n"
        + "   UNION ALL\n"
        + "     (SELECT empno, deptno, slacker\n"
        + "          FROM emp as e2)) AS e3\n"
        + "WHERE empno > 40\n"
        + "GROUP BY empno, slacker\n";

    sql(sql).withPre(preProgram).with(program).check();
  }
  @Test public void testPartialExpressionReduction() {
    final String sql = "select "
        + "EXTRACT(SECOND FROM CAST(CASE WHEN TRUE THEN {ts '2018-01-01 01:23:45'} ELSE NULL END AS TIMESTAMP)) = "
        + "EXTRACT(SECOND FROM d) "
        + "FROM (VALUES({ts '2018-01-01 01:23:45'})) tbl(d)";

    checkPlanning(CoreRules.PROJECT_REDUCE_EXPRESSIONS, sql);
  }

  @Test public void testRangeSimplification() {
    final String sql = "select *"
        + "FROM (VALUES({ts '2018-01-01 01:23:45'})) tbl(d) "
        + "WHERE d = '2018-01-01 01:23:45' AND d > '2018-01-01 01:23:45'";


    checkPlanning(CoreRules.FILTER_REDUCE_EXPRESSIONS, sql);
  }

  @Test public void testPushProjectWithIsNotDistinctFromPastJoin() {
    checkPlanning(CoreRules.PROJECT_JOIN_TRANSPOSE,
        "select e.sal + b.comm from emp e inner join bonus b "
            + "on e.ename IS NOT DISTINCT FROM b.ename and e.deptno = 10");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3296">[CALCITE-3296]
   * Decorrelator gives empty result
   * after decorrelating sort rel with null offset and fetch </a>
   */
  @Test public void testDecorrelationWithSort() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)\n"
        + "order by e1.empno";

    sql(sql).with(HepProgram.builder().build()).withDecorrelation(true).checkUnchanged();
  }

  @Test public void testDecorrelateUnion0() {
    final String sql = ""
        + "SELECT deptno FROM dept where exists\n"
        + "(SELECT 1 FROM emp where sal < 100 and emp.deptno=dept.deptno\n"
        + "UNION ALL\n"
        + "SELECT 1 FROM bonus where bonus.comm=dept.deptno)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Input frames with the same correlated variables and old-new output mappings,
   * but correlated variables referenced by different columns.
   */
  @Test public void testDecorrelateUnion1() {
    final String sql = "SELECT * FROM emp where exists \n"
        + "(SELECT 1 FROM dept where deptno = emp.deptno\n"
        + "UNION ALL\n"
        + "SELECT 1 FROM dept where deptno + 1 = emp.deptno)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Input frames with different correlated variables
   * and referenced by different columns;
   */
  @Test public void testDecorrelateUnion2() {
    final String sql = "SELECT * FROM emp where exists \n"
        + "(SELECT 1 FROM dept where deptno = emp.deptno\n"
        + "UNION ALL\n"
        + "SELECT 1 FROM dept where name = emp.ename)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Input frames with the same correlated variables,
   * but different old-new output mappings.
   */
  @Test public void testDecorrelateUnion3() {
    final String sql = "SELECT * FROM emp A where (deptno, sal) in \n"
        + "(SELECT deptno, max(sal) FROM emp B where A.mgr = B.empno group by deptno\n"
        + "UNION ALL\n"
        + "SELECT deptno, sal FROM emp B where A.mgr = B.empno)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4317">[CALCITE-4317]
   * RelFieldTrimmer after trimming all the fields in an aggregate
   * should not return a zero field Aggregate</a>.
   */
  @Test public void testProjectJoinTransposeRuleOnAggWithNoFieldsWithTrimmer() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // Build a rel equivalent to sql:
    // SELECT name FROM (SELECT count(*) cnt_star, count(empno) cnt_en FROM sales.emp)
    // cross join sales.dept
    // limit 10

    RelNode left = relBuilder.scan("DEPT").build();
    RelNode right = relBuilder.scan("EMP")
        .project(
          ImmutableList.of(relBuilder.getRexBuilder().makeExactLiteral(BigDecimal.ZERO)),
          ImmutableList.of("DUMMY"))
        .aggregate(
          relBuilder.groupKey(),
          relBuilder.count(false, "DUMMY_COUNT", relBuilder.field(0)))
        .build();

    RelNode plan = relBuilder.push(left)
        .push(right)
        .join(JoinRelType.INNER,
          relBuilder.getRexBuilder().makeLiteral(true))
        .project(relBuilder.field("DEPTNO"))
        .build();

    final String planBeforeTrimming = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planBeforeTrimming",
        "${planBeforeTrimming}", planBeforeTrimming);

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    Tester tester = new TesterImpl(getDiffRepos(), false, true, false,
            false, false, null, null)
            .withClusterFactory(
              relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    plan = tester.trimRelNode(plan);

    final String planAfterTrimming = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planAfterTrimming", "${planAfterTrimming}", planAfterTrimming);

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(plan);
    RelNode output = hepPlanner.findBestExp();
    final String finalPlan = NL + RelOptUtil.toString(output);
    getDiffRepos().assertEquals("finalPlan", "${finalPlan}", finalPlan);
  }

  @Test public void testProjectReduceExpressionsWithIsNotDistinctFrom() {

    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // Build a rel equivalent to sql:
    // SELECT hiredate from emp where hiredate is not distinct from '2020-12-11'

    final RexBuilder rexB = relBuilder.getRexBuilder();

    final RexNode dateLiteral = dateLiteral(rexB, 2020, 12, 11);

    RexNode dateInput4 = dateInput(rexB, 4, true);

    final RexNode filterCond = rexB.makeCall(IS_NOT_DISTINCT_FROM,
            dateInput4,
            dateLiteral);

    RelNode plan = relBuilder.scan("EMP")
            .filter(filterCond)
            .project(relBuilder.field("HIREDATE"))
            .build();

    final String planBefore = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planBefore",
            "${planBefore}", planBefore);

    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
            .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    final RexExecutorImpl executor =
               new RexExecutorImpl(Schemas.createDataContext(null, null));
    plan.getCluster().getPlanner().setExecutor(executor);
    hepPlanner.setRoot(plan);
    RelNode output = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(output);

    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testJoinReduceExpressionsWithIsNotDistinctFrom() {

    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // Build a rel equivalent to sql:
    // SELECT hiredate from emp e1 join (select hiredate from emp
    // where hiredate is not distinct from '2020-12-11') e2
    // on e1.hiredate = e2.hiredate

    final RexBuilder rexB = relBuilder.getRexBuilder();

    RexNode dateInput4 = dateInput(rexB, 4, true);

    RexNode dateInput8 = dateInput(rexB, 8, true);

    final RexNode dateLiteral = dateLiteral(rexB, 2020, 12, 11);

    final RexNode filterCond = rexB.makeCall(IS_NOT_DISTINCT_FROM,
            dateInput4,
            dateLiteral);

    RelNode right = relBuilder.scan("EMP")
            .filter(filterCond)
            .project(relBuilder.field("HIREDATE"))
            .build();

    RelNode plan = relBuilder.scan("EMP")
            .push(right)
            .join(JoinRelType.INNER,
                    rexB.makeCall(EQUALS, dateInput4, dateInput8))
            .build();

    final String planBefore = NL + RelOptUtil.toString(plan);
    getDiffRepos().assertEquals("planBefore",
            "${planBefore}", planBefore);

    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS)
            .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    final RexExecutorImpl executor =
            new RexExecutorImpl(Schemas.createDataContext(null, null));
    plan.getCluster().getPlanner().setExecutor(executor);
    hepPlanner.setRoot(plan);
    RelNode output = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(output);

    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testFilterRemoveIsNotDistinctFromRule() {
    final DiffRepository diffRepos = getDiffRepos();
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode root = builder
        .scan("EMP")
        .filter(
            builder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            builder.field("DEPTNO"), builder.literal(20)))
        .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  private RexNode dateLiteral(RexBuilder rexB, int year, int month, int day) {
    return rexB.makeDateLiteral(new DateString(year, month, day));
  }

  private RexInputRef dateInput(RexBuilder rexB, int input, boolean nullable) {
    return rexB.makeInputRef(rexB.getTypeFactory()
            .createTypeWithNullability(
                    rexB.getTypeFactory().createSqlType(SqlTypeName.DATE), nullable), input);
  }

  @Test public void testOversimplifiedCaseStatement() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    String sql = "select * from emp "
        + "where MGR > 0 and "
        + "case when MGR > 0 then deptno / MGR else null end > 1";
    checkPlanning(program, sql);
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2726">[CALCITE-2726]
  * ReduceExpressionRule may oversimplify filter conditions containing nulls</a>.
  */
  @Test public void testNoOversimplificationBelowIsNull() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    String sql =
        "select * from emp where ( (empno=1 and mgr=1) or (empno=null and mgr=1) ) is null";
    checkPlanning(program, sql);
  }

  @Test public void testExchangeRemoveConstantKeysRule() {
    final DiffRepository diffRepos = getDiffRepos();
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode root = builder
        .scan("EMP")
        .filter(
        builder.call(SqlStdOperatorTable.EQUALS,
          builder.field("EMPNO"), builder.literal(10)))
        .exchange(RelDistributions.hash(ImmutableList.of(0)))
        .project(builder.field(0), builder.field(1))
        .sortExchange(RelDistributions.hash(ImmutableList.of(0, 1)),
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1)))
        .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE)
        .addRuleInstance(ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testReduceAverageWithNoReduceSum() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG);
    checkPlanning(
        new AggregateReduceFunctionsRule(
          LogicalAggregate.class, RelFactories.LOGICAL_BUILDER,
            functionsToReduce),
                  "select name, max(name), avg(deptno), min(name)"
                          + " from sales.dept group by name");
  }

  @Test public void testNoReduceAverage() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.noneOf(SqlKind.class);
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(
          new AggregateReduceFunctionsRule(LogicalAggregate.class,
            RelFactories.LOGICAL_BUILDER, functionsToReduce))
        .build();
    String sql = "select name, max(name), avg(deptno), min(name)"
        + " from sales.dept group by name";
    sql(sql).with(program).checkUnchanged();
  }

  @Test public void testNoReduceSum() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.noneOf(SqlKind.class);
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(
              new AggregateReduceFunctionsRule(LogicalAggregate.class,
                RelFactories.LOGICAL_BUILDER, functionsToReduce))
            .build();
    String sql = "select name, sum(deptno)"
            + " from sales.dept group by name";
    sql(sql).with(program).checkUnchanged();
  }

  @Test public void testReduceAverageAndVarWithNoReduceStddev() {
    // configure rule to reduce AVG and VAR_POP functions
    // other functions like SUM, STDDEV won't be reduced
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.VAR_POP);
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(
         new AggregateReduceFunctionsRule(LogicalAggregate.class,
           RelFactories.LOGICAL_BUILDER, functionsToReduce))
        .build();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).with(program).check();
  }

  @Test public void testReduceAverageAndSumWithNoReduceStddevAndVar() {
    // configure rule to reduce AVG and SUM functions
    // other functions like VAR_POP, STDDEV_POP won't be reduced
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.SUM);
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(
          new AggregateReduceFunctionsRule(LogicalAggregate.class,
            RelFactories.LOGICAL_BUILDER, functionsToReduce))
        .build();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).with(program).check();
  }

  @Test public void testReduceAllAggregateFunctions() {
    // configure rule to reduce all used functions
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.SUM,
        SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP, SqlKind.VAR_POP, SqlKind.VAR_SAMP);
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(
          new AggregateReduceFunctionsRule(LogicalAggregate.class,
            RelFactories.LOGICAL_BUILDER, functionsToReduce))
        .build();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " stddev_samp(deptno), var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).with(program).check();
  }

  @Test public void testDynamicStarWithUnion() {
    String sql = "(select n_nationkey from SALES.CUSTOMER) union all\n"
            + "(select n_name from CUSTOMER_MODIFIABLEVIEW)";

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    RuleSet ruleSet =
        RuleSets.ofList(
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testFilterAndProjectWithMultiJoin() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
          .addRuleCollection(Arrays.asList(MyFilterRule.INSTANCE, MyProjectRule.INSTANCE))
          .build();

    final FilterMultiJoinMergeRule filterMultiJoinMergeRule =
          FilterMultiJoinMergeRule.Config.DEFAULT
          .withOperandFor(MyFilter.class, MultiJoin.class)
              .toRule();

    final ProjectMultiJoinMergeRule projectMultiJoinMergeRule =
          ProjectMultiJoinMergeRule.Config.DEFAULT
              .withOperandFor(MyProject.class, MultiJoin.class)
              .toRule();

    HepProgram program = new HepProgramBuilder()
          .addRuleCollection(
               Arrays.asList(
                      CoreRules.JOIN_TO_MULTI_JOIN,
                      filterMultiJoinMergeRule,
                      projectMultiJoinMergeRule))
          .build();

    sql("select * from emp e1 left outer join dept d on e1.deptno = d.deptno where d.deptno > 3")
        .withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3151">[CALCITE-3151]
   * RexCall's Monotonicity is not considered in determining a Calc's collation</a>
   */
  @Test public void testMonotonicityUDF() throws Exception {
    final SqlFunction monotonicityFun =
        new SqlFunction("MONOFUN", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
            OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION) {
          @Override public boolean isDeterministic() {
            return false;
          }

          @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
            return SqlMonotonicity.INCREASING;
          }
        };

    // Build a tree equivalent to the SQL
    // SELECT sal, MONOFUN() AS n FROM emp
    final RelBuilder builder =
        RelBuilder.create(RelBuilderTest.config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("SAL"),
                builder.alias(builder.call(monotonicityFun), "M"))
            .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final RelCollation collationBefore =
        relBefore.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final RelCollation collationAfter =
        relAfter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    assertEquals(collationBefore, collationAfter);
  }

  @Test public void testPushFiltertWithIsNotDistinctFromPastJoin() {
    String query = "SELECT * FROM "
        + "emp t1 INNER JOIN "
        + "emp t2 "
        + "ON t1.deptno = t2.deptno "
        + "WHERE t1.ename is not distinct from t2.ename";
    checkPlanning(CoreRules.FILTER_INTO_JOIN, query);
  }

  /**
   * Custom implementation of {@link Filter} for use
   * in test case to verify that {@link FilterMultiJoinMergeRule}
   * can be created with any {@link Filter} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalFilter}
   */
  private static class MyFilter extends Filter {

    MyFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition) {
      super(cluster, traitSet, child, condition);
    }

    public MyFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new MyFilter(getCluster(), traitSet, input, condition);
    }

  }

  /**
   * Rule to transform {@link LogicalFilter} into
   * custom MyFilter
   */
  private static class MyFilterRule extends RelOptRule {
    static final MyFilterRule INSTANCE =
        new MyFilterRule(LogicalFilter.class, RelFactories.LOGICAL_BUILDER);

    private MyFilterRule(Class<? extends Filter> clazz,
        RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalFilter logicalFilter = call.rel(0);
      final RelNode input = logicalFilter.getInput();
      final MyFilter myFilter = new MyFilter(input.getCluster(), input.getTraitSet(), input,
          logicalFilter.getCondition());
      call.transformTo(myFilter);
    }
  }

  /**
   * Custom implementation of {@link Project} for use
   * in test case to verify that {@link ProjectMultiJoinMergeRule}
   * can be created with any {@link Project} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalProject}
   */
  private static class MyProject extends Project {
    MyProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, input, projects, rowType);
    }

    public MyProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new MyProject(getCluster(), traitSet, input, projects, rowType);
    }
  }

  /**
   * Rule to transform {@link LogicalProject} into custom
   * MyProject
   */
  private static class MyProjectRule extends RelOptRule {
    static final MyProjectRule INSTANCE =
        new MyProjectRule(LogicalProject.class, RelFactories.LOGICAL_BUILDER);

    private MyProjectRule(Class<? extends Project> clazz,
        RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalProject logicalProject = call.rel(0);
      final RelNode input = logicalProject.getInput();
      final MyProject myProject = new MyProject(input.getCluster(), input.getTraitSet(), input,
          logicalProject.getChildExps(), logicalProject.getRowType());
      call.transformTo(myProject);
    }
  }

  @Test public void testSortJoinCopyInnerJoinOrderBy() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp join sales.dept on\n"
        + "sales.emp.deptno = sales.dept.deptno order by sal";
    final HepPlanner hepPlanner = new HepPlanner(program);
    checkPlanning(tester, preProgram, hepPlanner, sql);
  }

  @Test public void testSortJoinCopyInnerJoinOrderByLimit() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by sal limit 10";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinCopyInnerJoinOrderByTwoFields() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.SORT_PROJECT_TRANSPOSE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e join  sales.dept d on\n"
        + " e.deptno = d.deptno order by e.sal,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinCopySemiJoinOrderBy() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinCopySemiJoinOrderByLimitOffset() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno limit 10 offset 2";
    // Do not copy the limit and offset
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinCopySemiJoinOrderByOffset() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in"
        + " (select e.deptno from sales.emp e) order by d.deptno offset 2";
    // Do not copy the offset
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }
}
<<<<<<< HEAD
=======

// End RelOptRulesTest.java
