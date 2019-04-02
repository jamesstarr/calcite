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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.JdbcTest.Department;
import org.apache.calcite.test.JdbcTest.Dependent;
import org.apache.calcite.test.JdbcTest.Employee;
import org.apache.calcite.test.JdbcTest.Event;
import org.apache.calcite.test.JdbcTest.Location;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the materialized view rewrite mechanism. Each test has a
 * query and one or more materializations (what Oracle calls materialized views)
 * and checks that the materialization is used.
 */
@Category(SlowTests.class)
public class MaterializationTest {
  private static final Consumer<ResultSet> CONTAINS_M0 =
      CalciteAssert.checkResultContains(
          "EnumerableTableScan(table=[[hr, m0]])");

  @Test public void testJoinAggregateMaterializationAggregateFuncs14() {
    checkMaterialize(
        "select \"empid\", \"emps\".\"name\", \"emps\".\"deptno\", \"depts\".\"name\", "
            + "count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b')\n"
            + "group by \"empid\", \"emps\".\"name\", \"depts\".\"name\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"name\" is not null and \"emps\".\"name\" = 'a'\n"
            + "group by \"depts\".\"deptno\"",
        HR_FKUK_MODEL,
        CONTAINS_M0);
  }

  @Test public void testJoinMaterialization4() {
    checkMaterialize(
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], expr#1=[1], expr#2=[CAST($t0):INTEGER NOT NULL], "
                + "expr#3=[=($t1, $t2)], deptno=[$t0], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterialization5() {
    sql("select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" > 1")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
                + "expr#2=[1], expr#3=[<($t2, $t1)], EXPR$0=[$t1], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterialization6() {
    sql("select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
                + "expr#2=[1], expr#3=[CAST($t1):INTEGER NOT NULL], expr#4=[=($t2, $t3)], "
                + "EXPR$0=[$t1], $condition=[$t4])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterialization7() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")",
        HR_FKUK_MODEL,
        CalciteAssert.checkResultContains(
            "EnumerableCalc(expr#0..2=[{inputs}], empid=[$t1])\n"
                + "  EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
                + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], name=[$t1])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])\n"
                + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t1):VARCHAR], empid=[$t0], name0=[$t2])\n"
                + "      EnumerableTableScan(table=[[hr, dependents]])"));
  }

  @Test void testJoinMaterialization8() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")")
        .withResultContains(
            "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t2])\n"
                + "  EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
                + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], proj#0..1=[{exprs}])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])\n"
                + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t1):VARCHAR], proj#0..2=[{exprs}])\n"
                + "      EnumerableTableScan(table=[[hr, dependents]])"));
  }

  @Test void testJoinMaterialization9() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterialization10() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 30",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10")
        .withResultContains(
            "EnumerableUnion(all=[true])",
            "EnumerableTableScan(table=[[hr, m0]])",
            "expr#5=[10], expr#6=[>($t0, $t5)], expr#7=[30], expr#8=[>=($t7, $t0)]")
        .ok();
  }

  @Test public void testJoinMaterialization12() {
    checkMaterialize(
        "select \"empid\", \"emps\".\"name\", \"emps\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'c')",
        "select \"depts\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b')",
        HR_FKUK_MODEL,
        CONTAINS_M0);
  }

  @Test public void testJoinMaterializationUKFK1() {
    checkMaterialize(
        "select \"a\".\"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .withResultContains(
            "PLAN=EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK2() {
    sql("select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK3() {
    sql("select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"name\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .noMat();
  }

  @Test void testJoinMaterializationUKFK4() {
    sql("select \"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1)\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\" where \"empid\" = 1\n")
        .withResultContains(
            "PLAN=EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterializationUKFK5() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterializationUKFK6() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"deptno\"=\"b\".\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK7() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"name\"=\"a\".\"name\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"name\"=\"b\".\"name\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .noMat();
  }

  @Test void testJoinMaterializationUKFK8() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"name\"=\"b\".\"name\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .noMat();
  }

  @Tag("slow")
  @Test void testJoinMaterializationUKFK9() {
    sql("select * from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"emps\".\"empid\", \"dependents\".\"empid\", \"emps\".\"deptno\"\n"
            + "from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "where \"emps\".\"name\" = 'Bill'")
        .withResultContains(
            "EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateOnJoinKeys() {
    sql("select \"deptno\", \"empid\", \"salary\" "
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"empid\", \"depts\".\"deptno\" "
            + "from \"emps\"\n"
            + "join \"depts\" on \"depts\".\"deptno\" = \"empid\" group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], empid=[$t0], empid0=[$t0])\n"
                + "  EnumerableAggregate(group=[{1}])\n"
                + "    EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateOnJoinKeys2() {
    sql("select \"deptno\", \"empid\", \"salary\", sum(1) "
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select sum(1) "
            + "from \"emps\"\n"
            + "join \"depts\" on \"depts\".\"deptno\" = \"empid\" group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n"
                + "  EnumerableAggregate(group=[{1}], EXPR$0=[$SUM0($3)])\n"
                + "    EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testViewMaterialization() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"depts\".\"name\"\n"
            + "from \"depts\"\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "matview",
        true,
        HR_FKUK_MODEL,
        CalciteAssert.checkResultContains(
            "EnumerableValues(tuples=[[{ 'noname' }]])"),
        RuleSets.ofList(ImmutableList.of()))
        .returnsValue("noname");
  }

  @Test void testSubQuery() {
    String q = "select \"empid\", \"deptno\", \"salary\" from \"emps\" e1\n"
        + "where \"empid\" = (\n"
        + "  select max(\"empid\") from \"emps\"\n"
        + "  where \"deptno\" = e1.\"deptno\")";
    final String m = "select \"empid\", \"deptno\" from \"emps\"\n";
    sql(m, q).withChecker(
        CalciteAssert.checkResultContains(
            "EnumerableTableScan(table=[[hr, m0]])", 1))
        .ok();
  }

  @Test void testTableModify() {
    final String m = "select \"deptno\", \"empid\", \"name\""
        + "from \"emps\" where \"deptno\" = 10";
    final String q = "upsert into \"dependents\""
        + "select \"empid\" + 1 as x, \"name\""
        + "from \"emps\" where \"deptno\" = 10";

    final List<List<List<String>>> substitutedNames = new ArrayList<>();
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", m)
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .explainContains("hr, m0");
    } catch (Exception e) {
      // Table "dependents" not modifiable.
    }
    assertThat(substitutedNames, is(list3(new String[][][]{{{"hr", "m0"}}})));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-761">[CALCITE-761]
   * Pre-populated materializations</a>. */
  @Test public void testPrePopulated() {
    String q = "select \"deptno\" from \"emps\"";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(
              HR_FKUK_MODEL, builder -> {
                final Map<String, Object> map = builder.map();
                map.put("table", "locations");
                String sql = "select `deptno` as `empid`, '' as `name`\n"
                    + "from `emps`";
                final String sql2 = sql.replaceAll("`", "\"");
                map.put("sql", sql2);
                return ImmutableList.of(map);
              })
          .query(q)
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test public void testViewSchemaPath() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final String m = "select empno, deptno from emp";
      final String q = "select deptno from scott.emp";
      final List<String> path = ImmutableList.of("SCOTT");
      final JsonBuilder builder = new JsonBuilder();
      final String model = "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'hr',\n"
          + "  schemas: [\n"
          + JdbcTest.SCOTT_SCHEMA
          + "  ,\n"
          + "    {\n"
          + "      materializations: [\n"
          + "        {\n"
          + "          table: 'm0',\n"
          + "          view: 'm0v',\n"
          + "          sql: " + builder.toJsonString(m) + ",\n"
          + "          viewSchemaPath: " + builder.toJsonString(path)
          + "        }\n"
          + "      ],\n"
          + "      type: 'custom',\n"
          + "      name: 'hr',\n"
          + "      factory: 'org.apache.calcite.adapter.java.ReflectiveSchema$Factory',\n"
          + "      operand: {\n"
          + "        class: 'org.apache.calcite.test.JdbcTest$HrSchema'\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";
      CalciteAssert.that()
          .withModel(model)
          .query(q)
          .enableMaterializations(true)
          .explainMatches("", CONTAINS_M0)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testSingleMaterializationMultiUsage() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";
    String m = "select * from \"emps\" where \"empid\" < 500";
    sql(m, q).withChecker(
        CalciteAssert.checkResultContains(
            "EnumerableTableScan(table=[[hr, m0]])", 2))
        .ok();
  }

  @Test void testMultiMaterializationMultiUsage() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select \"deptno\", count(*) as c from \"emps\" group by \"deptno\") using (\"deptno\")";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select \"deptno\", count(*) as c, sum(\"empid\") as s from \"emps\" group by \"deptno\"",
              "m1", "select * from \"emps\" where \"empid\" < 500")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .explainContains("EnumerableTableScan(table=[[hr, m1]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testMaterializationOnJoinQuery() {
    final String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" < 300 ";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 500")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Disabled("Creating mv for depts considering all its column throws exception")
  @Test void testMultiMaterializationOnJoinQuery() {
    final String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" < 300 "
        + "and \"depts\".\"deptno\" > 200";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 500",
              "m1", "select * from \"depts\" where \"deptno\" > 100")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .explainContains("EnumerableTableScan(table=[[hr, m1]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery1() {
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    sql("select \"deptno\", \"empid\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"deptno\", count(distinct \"empid\") as c from (\n"
            + "select \"deptno\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], C=[COUNT($1)])\n"
                + "  EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery2() {
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    sql("select \"deptno\", \"salary\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\", \"empid\"",
        "select \"deptno\", count(distinct \"empid\") as c from (\n"
            + "select \"deptno\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], C=[COUNT($2)])\n"
                + "  EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery3() {
    // The column salary is not unique, thus we end up with
    // a different rewriting
    sql("select \"deptno\", \"empid\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"deptno\", count(distinct \"salary\") from (\n"
            + "select \"deptno\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
                + "  EnumerableAggregate(group=[{0, 2}])\n"
                + "    EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery4() {
    // Although there is no DISTINCT in the COUNT, this is
    // equivalent to previous query
    sql("select \"deptno\", \"salary\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\", \"empid\"",
        "select \"deptno\", count(\"salary\") from (\n"
            + "select \"deptno\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])\n"
                + "  EnumerableAggregate(group=[{0, 1}])\n"
                + "    EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testMaterializationSubstitution() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";

    final String[][][] expectedNames = {
        {{"hr", "emps"}, {"hr", "m0"}},
        {{"hr", "emps"}, {"hr", "m1"}},
        {{"hr", "m0"}, {"hr", "emps"}},
        {{"hr", "m0"}, {"hr", "m0"}},
        {{"hr", "m0"}, {"hr", "m1"}},
        {{"hr", "m1"}, {"hr", "emps"}},
        {{"hr", "m1"}, {"hr", "m0"}},
        {{"hr", "m1"}, {"hr", "m1"}}};

    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final List<List<List<String>>> substitutedNames = new ArrayList<>();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 300",
              "m1", "select * from \"emps\" where \"empid\" < 600")
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
      substitutedNames.sort(CASE_INSENSITIVE_LIST_LIST_COMPARATOR);
      assertThat(substitutedNames, is(list3(expectedNames)));
    }
  }

  @Test void testMaterializationSubstitution2() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";

    final String[][][] expectedNames = {
        {{"hr", "emps"}, {"hr", "m0"}},
        {{"hr", "emps"}, {"hr", "m1"}},
        {{"hr", "emps"}, {"hr", "m2"}},
        {{"hr", "m0"}, {"hr", "emps"}},
        {{"hr", "m0"}, {"hr", "m0"}},
        {{"hr", "m0"}, {"hr", "m1"}},
        {{"hr", "m0"}, {"hr", "m2"}},
        {{"hr", "m1"}, {"hr", "emps"}},
        {{"hr", "m1"}, {"hr", "m0"}},
        {{"hr", "m1"}, {"hr", "m1"}},
        {{"hr", "m1"}, {"hr", "m2"}},
        {{"hr", "m2"}, {"hr", "emps"}},
        {{"hr", "m2"}, {"hr", "m0"}},
        {{"hr", "m2"}, {"hr", "m1"}},
        {{"hr", "m2"}, {"hr", "m2"}}};

    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final List<List<List<String>>> substitutedNames = new ArrayList<>();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 300",
              "m1", "select * from \"emps\" where \"empid\" < 600",
              "m2", "select * from \"m1\"")
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
      substitutedNames.sort(CASE_INSENSITIVE_LIST_LIST_COMPARATOR);
      assertThat(substitutedNames, is(list3(expectedNames)));
    }
  }

  @Test void testMaterializationAfterTrimingOfUnusedFields() {
    String sql =
        "select \"y\".\"deptno\", \"y\".\"name\", \"x\".\"sum_salary\"\n"
            + "from\n"
            + "  (select \"deptno\", sum(\"salary\") \"sum_salary\"\n"
            + "  from \"emps\"\n"
            + "  group by \"deptno\") \"x\"\n"
            + "  join\n"
            + "  \"depts\" \"y\"\n"
            + "  on \"x\".\"deptno\"=\"y\".\"deptno\"\n";
    sql(sql, sql).ok();
  }

  @Test void testUnionAllToUnionAll() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    sql(sql0 + " union all " + sql1, sql1 + " union all " + sql0).ok();
  }

  @Test void testUnionDistinctToUnionDistinct() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    sql(sql0 + " union " + sql1, sql1 + " union " + sql0).ok();
  }

  @Test void testUnionDistinctToUnionAll() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    sql(sql0 + " union " + sql1, sql0 + " union all " + sql1).noMat();
  }

  @Test void testUnionOnCalcsToUnion() {
    final String mv = ""
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300\n"
        + "union all\n"
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100";
    final String query = ""
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300 and \"salary\" > 100\n"
        + "union all\n"
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100 and \"salary\" > 100";
    sql(mv, query).ok();
  }

  @Test void testIntersectOnCalcsToIntersect() {
    final String mv = ""
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300\n"
        + "intersect all\n"
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100";
    final String query = ""
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300 and \"salary\" > 100\n"
        + "intersect all\n"
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100 and \"salary\" > 100";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testIntersectToIntersect0() {
    final String mv = ""
        + "select \"deptno\" from \"emps\"\n"
        + "intersect\n"
        + "select \"deptno\" from \"depts\"";
    final String query = ""
        + "select \"deptno\" from \"depts\"\n"
        + "intersect\n"
        + "select \"deptno\" from \"emps\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testIntersectToIntersect1() {
    final String mv = ""
        + "select \"deptno\" from \"emps\"\n"
        + "intersect all\n"
        + "select \"deptno\" from \"depts\"";
    final String query = ""
        + "select \"deptno\" from \"depts\"\n"
        + "intersect all\n"
        + "select \"deptno\" from \"emps\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testIntersectToCalcOnIntersect() {
    final String intersect = ""
        + "select \"deptno\",\"name\" from \"emps\"\n"
        + "intersect all\n"
        + "select \"deptno\",\"name\" from \"depts\"";
    final String mv = "select \"name\", \"deptno\" from (" + intersect + ")";

    final String query = ""
        + "select \"name\",\"deptno\" from \"depts\"\n"
        + "intersect all\n"
        + "select \"name\",\"deptno\" from \"emps\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  private static <E> List<List<List<E>>> list3(E[][][] as) {
    final ImmutableList.Builder<List<List<E>>> builder =
        ImmutableList.builder();
    for (E[][] a : as) {
      builder.add(list2(a));
    }
    return builder.build();
  }

  private static <E> List<List<E>> list2(E[][] as) {
    final ImmutableList.Builder<List<E>> builder = ImmutableList.builder();
    for (E[] a : as) {
      builder.add(ImmutableList.copyOf(a));
    }
    return builder.build();
  }

  /**
   * Implementation of RelVisitor to extract substituted table names.
   */
  private static class TableNameVisitor extends RelVisitor {
    private List<List<String>> names = new ArrayList<>();

    List<List<String>> run(RelNode input) {
      go(input);
      return names;
    }

    @Override public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof TableScan) {
        RelOptTable table = node.getTable();
        List<String> qName = table.getQualifiedName();
        names.add(qName);
      }
      super.visit(node, ordinal, parent);
    }
  }

  /**
   * Hr schema with FK-UK relationship.
   */
  public static class HrFKUKSchema {
    @Override public String toString() {
      return "HrFKUKSchema";
    }

    public final Employee[] emps = {
        new Employee(100, 10, "Bill", 10000, 1000),
        new Employee(200, 20, "Eric", 8000, 500),
        new Employee(150, 10, "Sebastian", 7000, null),
        new Employee(110, 10, "Theodore", 10000, 250),
    };
    public final Department[] depts = {
        new Department(10, "Sales", Arrays.asList(emps[0], emps[2], emps[3]),
            new Location(-122, 38)),
        new Department(30, "Marketing", ImmutableList.of(),
            new Location(0, 52)),
        new Department(20, "HR", Collections.singletonList(emps[1]), null),
    };
    public final Dependent[] dependents = {
        new Dependent(10, "Michael"),
        new Dependent(10, "Jane"),
    };
    public final Dependent[] locations = {
        new Dependent(10, "San Francisco"),
        new Dependent(20, "San Diego"),
    };
    public final Event[] events = {
        new Event(100, new Timestamp(0)),
        new Event(200, new Timestamp(0)),
        new Event(150, new Timestamp(0)),
        new Event(110, null),
    };

    public final RelReferentialConstraint rcs0 =
        RelReferentialConstraintImpl.of(
            ImmutableList.of("hr", "emps"), ImmutableList.of("hr", "depts"),
            ImmutableList.of(IntPair.of(1, 0)));

    public QueryableTable foo(int count) {
      return Smalls.generateStrings(count);
    }

    public TranslatableTable view(String s) {
      return Smalls.view(s);
    }

    public TranslatableTable matview() {
      return Smalls.strView("noname");
    }
  }

  /** Fluent class that contains information necessary to run a test. */
  private interface Sql {
    default void ok() {
      that().sameResultWithMaterializationsDisabled();
    }

    default CalciteAssert.AssertQuery that() {
      return checkThatMaterialize_(getMaterialize(), getQuery(), getViewName(),
          isExisting(), getModel(), getChecker(), getRuleSet(),
          isOnlyBySubstitution());
    }

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isExisting();
    Sql withExisting(boolean existing);

    default void noMat() {
      checkNoMaterialize_(getMaterialize(), getQuery(),
          getModel(), isOnlyBySubstitution());
    }

    default Sql withResultContains(String... expected) {
      return withChecker(CalciteAssert.checkResultContains(expected));
    }

    @ImmutableBeans.Property
    String getMaterialize();
    Sql withMaterialize(String materialize);

    @ImmutableBeans.Property
    String getQuery();
    Sql withQuery(String query);

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isOnlyBySubstitution();
    Sql withOnlyBySubstitution(boolean onlyBySubstitution);

    @ImmutableBeans.Property
    String getModel();
    Sql withModel(String model);

    @ImmutableBeans.Property
    Consumer<ResultSet> getChecker();
    Sql withChecker(Consumer<ResultSet> explainChecker);

    @ImmutableBeans.Property
    RuleSet getRuleSet();
    Sql withRuleSet(RuleSet ruleSet);

    @ImmutableBeans.Property
    @ImmutableBeans.StringDefault("m0")
    String getViewName();
    Sql withViewName(String viewName);
  }
}
