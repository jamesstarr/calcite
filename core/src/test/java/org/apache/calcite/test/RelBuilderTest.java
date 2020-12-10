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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
>>>>>>> cd779c1b5 (DX-7801 [CALCITE-4433] Add UNPIVOT operator to SQL)
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Unit test for {@link RelBuilder}.
 *
 * <p>Tasks:</p>
 * <ol>
 *   <li>Add RelBuilder.scan(List&lt;String&gt;)</li>
 *   <li>Add RelBuilder.scan(Table)</li>
 *   <li>Test that {@link RelBuilder#filter} does not create a filter if the
 *   predicates optimize to true</li>
 *   <li>Test that {@link RelBuilder#filter} DOES create a filter if the
 *   predicates optimize to false. (Creating an empty Values seems too
 *   devious.)</li>
 *   <li>Test that {@link RelBuilder#scan} throws good error if table not
 *   found</li>
 *   <li>Test that {@link RelBuilder#scan} obeys case-sensitivity</li>
 *   <li>Test that {@link RelBuilder#join(JoinRelType, String...)} obeys
 *   case-sensitivity</li>
 *   <li>Test RelBuilder with alternative factories</li>
 *   <li>Test that {@link RelBuilder#field(String)} obeys case-sensitivity</li>
 *   <li>Test case-insensitive unique field names</li>
 *   <li>Test that an alias created using
 *      {@link RelBuilder#alias(RexNode, String)} is removed if not a top-level
 *      project</li>
 *   <li>{@link RelBuilder#aggregate} with grouping sets</li>
  @Test public void testPivot() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT mgr, deptno, job, sal FROM emp)
    //   PIVOT (SUM(sal) AS ss, COUNT(*) AS c
    //       FOR (job, deptno)
    //       IN (('CLERK', 10) AS c10, ('MANAGER', 20) AS m20))
    //
    // translates to
    //   SELECT mgr,
    //     SUM(sal) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_ss,
    //     COUNT(*) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_c,
    //     SUM(sal) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_ss,
    //     COUNT(*) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_c
    //   FROM emp
    //   GROUP BY mgr
    //
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .pivot(b.groupKey("MGR"),
                Arrays.asList(
                    b.sum(b.field("SAL")).as("SS"),
                    b.count().as("C")),
                b.fields(Arrays.asList("JOB", "DEPTNO")),
                ImmutableMap.<String, List<RexNode>>builder()
                    .put("C10",
                        Arrays.asList(b.literal("CLERK"), b.literal(10)))
                    .put("M20",
                        Arrays.asList(b.literal("MANAGER"), b.literal(20)))
                    .build()
                    .entrySet())
            .build();
    final String expected = ""
        + "LogicalAggregate("
            + "group=[{3}], "
            + "C10_SS=[SUM($5) FILTER $8], "
            + "C10_C=[COUNT() FILTER $8], "
            + "M20_SS=[SUM($5) FILTER $9], "
            + "M20_C=[COUNT() FILTER $9])\n"
            + "  LogicalProject("
            +     "EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
            +     "SAL=[$5], COMM=[$6], DEPTNO=[$7], "
            +     "$f8=[IS TRUE(AND(=($2, 'CLERK'), =($7, 10)))], "
            +     "$f9=[IS TRUE(AND(=($2, 'MANAGER'), =($7, 20)))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test public void testUnpivot() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT deptno, job, sal, comm FROM emp)
    //   UNPIVOT INCLUDE NULLS (remuneration
    //     FOR remuneration_type IN (comm AS 'commission',
    //                               sal AS 'salary'))
    //
    // translates to
    //   SELECT e.deptno, e.job,
    //     CASE t.remuneration_type
    //     WHEN 'commission' THEN comm
    //     ELSE sal
    //     END AS remuneration
    //   FROM emp
    //   CROSS JOIN VALUES ('commission', 'salary') AS t (remuneration_type)
    //
    final BiFunction<RelBuilder, Boolean, RelNode> f = (b, includeNulls) ->
        b.scan("EMP")
            .unpivot(includeNulls,
                ImmutableList.of("REMUNERATION"),
                ImmutableList.of("REMUNERATION_TYPE"),
                Pair.zip(
                    Arrays.asList(ImmutableList.of(b.literal("commission")),
                        ImmutableList.of(b.literal("salary"))),
                    Arrays.asList(ImmutableList.of(b.field("COMM")),
                        ImmutableList.of(b.field("SAL")))))
            .build();
    final String expectedIncludeNulls = ""
        + "LogicalProject("
            + "EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
            + "DEPTNO=[$7], REMUNERATION_TYPE=[$8], REMUNERATION=[CASE(=($8, 'commission'), "
            + "$6, =($8, 'salary'), $5, null)])\n"
            + "  LogicalJoin(condition=[true], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalValues(tuples=[[{ 'commission' }, { 'salary' }]])\n";
    final String expectedExcludeNulls = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], DEPTNO=[$5], REMUNERATION_TYPE=[$6], "
        + "REMUNERATION=[CAST($7):DECIMAL(7, 2) NOT NULL])\n"
        + "  LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "    " + expectedIncludeNulls.replace("\n  ", "\n      ");
    assertThat(f.apply(createBuilder(), true), hasTree(expectedIncludeNulls));
    assertThat(f.apply(createBuilder(), false), hasTree(expectedExcludeNulls));
  }

  @Test public void testMatchRecognize() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   MATCH_RECOGNIZE (
    //     PARTITION BY deptno
    //     ORDER BY empno asc
    //     MEASURES
    //       STRT.mgr as start_nw,
    //       LAST(DOWN.mgr) as bottom_nw,
    //     PATTERN (STRT DOWN+ UP+) WITHIN INTERVAL '5' SECOND
    //     DEFINE
    //       DOWN as DOWN.mgr < PREV(DOWN.mgr),
    //       UP as UP.mgr > PREV(UP.mgr)
    //   )
    final RelBuilder builder = RelBuilder.create(config().build()).scan("EMP");
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    RexNode pattern = builder.patternConcat(
        builder.literal("STRT"),
        builder.patternQuantify(builder.literal("DOWN"), builder.literal(1),
            builder.literal(-1), builder.literal(false)),
        builder.patternQuantify(builder.literal("UP"), builder.literal(1),
            builder.literal(-1), builder.literal(false)));

    ImmutableMap.Builder<String, RexNode> pdBuilder = new ImmutableMap.Builder<>();
    RexNode downDefinition = builder.call(SqlStdOperatorTable.LESS_THAN,
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("DOWN", intType, 3),
            builder.literal(0)),
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("DOWN", intType, 3),
            builder.literal(1)));
    pdBuilder.put("DOWN", downDefinition);
    RexNode upDefinition = builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("UP", intType, 3),
            builder.literal(0)),
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("UP", intType, 3),
            builder.literal(1)));
    pdBuilder.put("UP", upDefinition);

    ImmutableList.Builder<RexNode> measuresBuilder = new ImmutableList.Builder<>();
    measuresBuilder.add(
        builder.alias(builder.patternField("STRT", intType, 3),
            "start_nw"));
    measuresBuilder.add(
        builder.alias(
            builder.call(SqlStdOperatorTable.LAST,
                builder.patternField("DOWN", intType, 3),
                builder.literal(0)),
            "bottom_nw"));

    RexNode after = builder.getRexBuilder().makeFlag(
        SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW);

    ImmutableList.Builder<RexNode> partitionKeysBuilder = new ImmutableList.Builder<>();
    partitionKeysBuilder.add(builder.field("DEPTNO"));

    ImmutableList.Builder<RexNode> orderKeysBuilder = new ImmutableList.Builder<>();
    orderKeysBuilder.add(builder.field("EMPNO"));

    RexNode interval = builder.literal("INTERVAL '5' SECOND");

    final ImmutableMap<String, TreeSet<String>> subsets = ImmutableMap.of();
    final RelNode root = builder
        .match(pattern, false, false, pdBuilder.build(),
            measuresBuilder.build(), after, subsets, false,
            partitionKeysBuilder.build(), orderKeysBuilder.build(), interval)
        .build();
    final String expected = "LogicalMatch(partition=[[7]], order=[[0]], "
        + "outputFields=[[$7, 'start_nw', 'bottom_nw']], allRows=[false], "
        + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[(('STRT', "
        + "PATTERN_QUANTIFIER('DOWN', 1, -1, false)), "
        + "PATTERN_QUANTIFIER('UP', 1, -1, false))], "
        + "isStrictStarts=[false], isStrictEnds=[false], "
        + "interval=['INTERVAL ''5'' SECOND'], subsets=[[]], "
        + "patternDefinitions=[[<(PREV(DOWN.$3, 0), PREV(DOWN.$3, 1)), "
        + ">(PREV(UP.$3, 0), PREV(UP.$3, 1))]], "
        + "inputFields=[[EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testFilterCastAny() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelDataType anyType =
        builder.getTypeFactory().createSqlType(SqlTypeName.ANY);
    final RelNode root =
        builder.scan("EMP")
            .filter(
                builder.cast(
                    builder.getRexBuilder().makeInputRef(anyType, 0),
                    SqlTypeName.BOOLEAN))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[CAST($0):BOOLEAN NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }
<<<<<<< HEAD
=======

  /** Tests filter builder with correlation variables */
  @Test public void testFilterWithCorrelationVariables() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(Collections.singletonList(v.get().id),
            Collections.singletonList(
                builder.call(SqlStdOperatorTable.OR,
                builder.call(SqlStdOperatorTable.AND,
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field(v.get(), "DEPTNO"),
                        builder.literal(30)),
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field(v.get(), "DEPTNO"),
                        builder.literal(20))),
                builder.isNull(builder.field(2)))))
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "SAL"),
                builder.literal(1000)),
            ImmutableSet.of(v.get().id))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], "
        + "requiredColumns=[{5, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    LogicalFilter(condition=[OR(AND(<($cor0.DEPTNO, 30), >($cor0.DEPTNO, 20)), "
        + "IS NULL($2))], variablesSet=[[$cor0]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";

    assertThat(root, hasTree(expected));
  }

  @Test public void testCorrelate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testSimpleLeftCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.LEFT);
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testSimpleInnerCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.INNER);
    final String expected = ""
        + "LogicalFilter(condition=[=($7, $8)])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testSimpleRightCorrelateViaJoinThrowsException() {
    try {
      buildSimpleCorrelateWithJoin(JoinRelType.RIGHT);
    } catch (IllegalArgumentException ex) {
      return;
    }
  }

  @Test public void testSimpleFullCorrelateViaJoinThrowsException() {
    try {
      buildSimpleCorrelateWithJoin(JoinRelType.FULL);
    } catch (IllegalArgumentException ex) {
      return;
    }
  }

  private static RelNode buildSimpleCorrelateWithJoin(JoinRelType type) {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    return builder
        .scan("EMP")
        .variable(v)
        .scan("DEPT")
        .join(type,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")), ImmutableSet.of(v.get().id))
        .build();
  }

  @Test public void testCorrelateWithComplexFields() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.LEFT, v.get().id,
            builder.field(2, 0, "DEPTNO"),
            builder.getRexBuilder().makeCall(SqlStdOperatorTable.AS,
                builder.field(2, 0, "EMPNO"),
                builder.literal("RENAMED_EMPNO")))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 7}])\n"
        + "  LogicalProject(RENAMED_EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }
}

  /** Tests filter builder with correlation variables */
  @Test void testFilterWithCorrelationVariables() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(Collections.singletonList(v.get().id),
            Collections.singletonList(
                builder.call(SqlStdOperatorTable.OR,
                builder.call(SqlStdOperatorTable.AND,
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field(v.get(), "DEPTNO"),
                        builder.literal(30)),
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field(v.get(), "DEPTNO"),
                        builder.literal(20))),
                builder.isNull(builder.field(2)))))
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "SAL"),
                builder.literal(1000)),
            ImmutableSet.of(v.get().id))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], "
        + "requiredColumns=[{5, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    LogicalFilter(condition=[OR(AND(<($cor0.DEPTNO, 30), >($cor0.DEPTNO, 20)), "
        + "IS NULL($2))], variablesSet=[[$cor0]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";

    assertThat(root, hasTree(expected));
  }

  @Test void testCorrelate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSimpleLeftCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.LEFT);
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSimpleInnerCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.INNER);
    final String expected = ""
        + "LogicalFilter(condition=[=($7, $8)])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSimpleRightCorrelateViaJoinThrowsException() {
    try {
      buildSimpleCorrelateWithJoin(JoinRelType.RIGHT);
    } catch (IllegalArgumentException ex) {
      return;
    }
  }

  @Test void testSimpleFullCorrelateViaJoinThrowsException() {
    try {
      buildSimpleCorrelateWithJoin(JoinRelType.FULL);
    } catch (IllegalArgumentException ex) {
      return;
    }
  }

  private static RelNode buildSimpleCorrelateWithJoin(JoinRelType type) {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    return builder
        .scan("EMP")
        .variable(v)
        .scan("DEPT")
        .join(type,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")), ImmutableSet.of(v.get().id))
        .build();
  }

  @Test void testCorrelateWithComplexFields() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.LEFT, v.get().id,
            builder.field(2, 0, "DEPTNO"),
            builder.getRexBuilder().makeCall(SqlStdOperatorTable.AS,
                builder.field(2, 0, "EMPNO"),
                builder.literal("RENAMED_EMPNO")))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 7}])\n"
        + "  LogicalProject(RENAMED_EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testFilterCastNull() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelNode root =
        builder.scan("EMP")
            .filter(
                builder.getRexBuilder().makeCast(
                    typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.BOOLEAN), true),
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(10))))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[=($7, 10)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }


  @Test void testRelBuilderToString() {
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP");

    // One entry on the stack, a single-node tree
    final String expected1 = "LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(Util.toLinux(builder.toString()), is(expected1));

    // One entry on the stack, a two-node tree
    builder.filter(builder.equals(builder.field(2), builder.literal(3)));
    final String expected2 = "LogicalFilter(condition=[=($2, 3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(Util.toLinux(builder.toString()), is(expected2));

    // Two entries on the stack
    builder.scan("DEPT");
    final String expected3 = "LogicalTableScan(table=[[scott, DEPT]])\n"
        + "LogicalFilter(condition=[=($2, 3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(Util.toLinux(builder.toString()), is(expected3));
  }

  /**
   * Ensures that relational algebra ({@link RelBuilder}) works with SQL views.
   *
   * <p>This test currently fails (thus ignored).
   */
  @Test void testExpandViewInRelBuilder() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      final Frameworks.ConfigBuilder configBuilder =
          expandingConfig(connection);
      final RelOptTable.ViewExpander viewExpander =
          (RelOptTable.ViewExpander) Frameworks.getPlanner(configBuilder.build());
      configBuilder.context(Contexts.of(viewExpander));
      final RelBuilder builder = RelBuilder.create(configBuilder.build());
      RelNode node = builder.scan("MYVIEW").build();

      int count = 0;
      try (PreparedStatement statement =
               connection.unwrap(RelRunner.class).prepare(node);
           ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          count++;
        }
      }

      assertTrue(count > 1);
    }
  }

  @Test void testExpandViewShouldKeepAlias() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      final Frameworks.ConfigBuilder configBuilder =
          expandingConfig(connection);
      final RelOptTable.ViewExpander viewExpander =
          (RelOptTable.ViewExpander) Frameworks.getPlanner(configBuilder.build());
      configBuilder.context(Contexts.of(viewExpander));
      final RelBuilder builder = RelBuilder.create(configBuilder.build());
      RelNode node =
          builder.scan("MYVIEW")
              .project(
                  builder.field(1, "MYVIEW", "EMPNO"),
                  builder.field(1, "MYVIEW", "ENAME"))
              .build();
      String expected =
          "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
              + "  LogicalFilter(condition=[=(1, 1)])\n"
                  + "    LogicalTableScan(table=[[scott, EMP]])\n";
      assertThat(node, hasTree(expected));
    }
  }

  @Test void testExpandTable() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      // RelBuilder expands as default. Plan contains JdbcTableScan,
      // because RelBuilder.scan has called RelOptTable.toRel.
      final Frameworks.ConfigBuilder configBuilder =
          expandingConfig(connection);
      final RelBuilder builder = RelBuilder.create(configBuilder.build());
      final String expected = "LogicalFilter(condition=[>($2, 10)])\n"
          + "  JdbcTableScan(table=[[JDBC_SCOTT, EMP]])\n";
      checkExpandTable(builder, hasTree(expected));
    }
  }

  private void checkExpandTable(RelBuilder builder, Matcher<RelNode> matcher) {
    final RelNode root =
        builder.scan("JDBC_SCOTT", "EMP")
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field(2),
                    builder.literal(10)))
            .build();
    assertThat(root, matcher);
  }

  @Test void testExchange() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root = builder.scan("EMP")
        .exchange(RelDistributions.hash(Lists.newArrayList(0)))
        .build();
    final String expected =
        "LogicalExchange(distribution=[hash[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSortExchange() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortExchange(RelDistributions.hash(Lists.newArrayList(0)),
                RelCollations.of(0))
            .build();
    final String expected =
        "LogicalSortExchange(distribution=[hash[0]], collation=[[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testHints() {
    final RelHint indexHint = RelHint.builder("INDEX")
        .hintOption("_idx1")
        .hintOption("_idx2")
        .build();
    final RelHint propsHint = RelHint.builder("PROPERTIES")
        .inheritPath(0)
        .hintOption("parallelism", "3")
        .hintOption("mem", "20Mb")
        .build();
    final RelHint noHashJoinHint = RelHint.builder("NO_HASH_JOIN")
        .inheritPath(0)
        .build();
    final RelBuilder builder = RelBuilder.create(config().build());
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp /*+ INDEX(_idx1, _idx2) */
    final RelNode root = builder
            .scan("EMP")
            .hints(indexHint)
            .build();
    assertThat(root,
        hasHints("[[INDEX inheritPath:[] options:[_idx1, _idx2]]]"));
    // Equivalent SQL:
    //   SELECT /*+  PROPERTIES(parallelism='3', mem='20Mb') */
    //   *
    //   FROM emp /*+ INDEX(_idx1, _idx2) */
    final RelNode root1 = builder
            .scan("EMP")
            .hints(indexHint, propsHint)
            .build();
    assertThat(root1,
        hasHints("[[INDEX inheritPath:[] options:[_idx1, _idx2]], "
            + "[PROPERTIES inheritPath:[0] options:{parallelism=3, mem=20Mb}]]"));
    // Equivalent SQL:
    //   SELECT /*+ NO_HASH_JOIN */
    //   *
    //   FROM emp
    //     join dept
    //     on emp.deptno = dept.deptno
    final RelNode root2 = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .hints(noHashJoinHint)
        .build();
    assertThat(root2, hasHints("[[NO_HASH_JOIN inheritPath:[0]]]"));
  }

  @Test void testHintsOnEmptyStack() {
    final RelHint indexHint = RelHint.builder("INDEX")
        .hintOption("_idx1")
        .hintOption("_idx2")
        .build();
    // Attach hints on empty stack.
    final AssertionError error = assertThrows(
        AssertionError.class,
        () -> RelBuilder.create(config().build()).hints(indexHint),
        "hints() should fail on empty stack");
    assertThat(error.getMessage(),
        containsString("There is no relational expression to attach the hints"));
  }

  @Test void testHintsOnNonHintable() {
    final RelHint indexHint = RelHint.builder("INDEX")
        .hintOption("_idx1")
        .hintOption("_idx2")
        .build();
    // Attach hints on non hintable.
    final AssertionError error1 = assertThrows(
        AssertionError.class,
        () -> {
          final RelBuilder builder = RelBuilder.create(config().build());
          // Equivalent SQL:
          //   SELECT *
          //   FROM emp
          //   WHERE EMPNO = 124
          builder
              .scan("EMP")
              .filter(
                  builder.equals(
                      builder.field("EMPNO"),
                      builder.literal(124)))
              .hints(indexHint);
        },
        "hints() should fail on non Hintable relational expression");
    assertThat(error1.getMessage(),
        containsString("The top relational expression is not a Hintable"));
  }

  /** Creates a matcher that matches if a RelNode has the expected hints. */
  private static Matcher<RelNode> hasHints(String expected) {
    return new org.hamcrest.CustomTypeSafeMatcher<RelNode>("has hints " + expected) {
      @Override protected boolean matchesSafely(RelNode relNode) {
        if (!(relNode instanceof org.apache.calcite.rel.hint.Hintable)) {
          return false;
        }
        org.apache.calcite.rel.hint.Hintable hintable =
            (org.apache.calcite.rel.hint.Hintable) relNode;
        String actual = hintable.getHints().toString();
        return actual.equals(expected);
      }
    };
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3747">[CALCITE-3747]
   * Constructing BETWEEN with RelBuilder throws class cast exception</a>. */
  @Test void testCallBetweenOperator() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RexNode call = builder.scan("EMP")
        .call(
            SqlStdOperatorTable.BETWEEN,
            builder.field("EMPNO"),
            builder.literal(1),
            builder.literal(5));
    assertThat(call.toString(), is("BETWEEN ASYMMETRIC($0, 1, 5)"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3926">[CALCITE-3926]
   * CannotPlanException when an empty LogicalValues requires a certain collation</a>. */
  @Test void testEmptyValuesWithCollation() throws Exception {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder
            .scan("DEPT").empty()
            .sort(
                builder.field("DNAME"),
                builder.field("DEPTNO"))
            .build();
    try (PreparedStatement preparedStatement = RelRunners.run(root)) {
      final String result = CalciteAssert.toString(preparedStatement.executeQuery());
      final String expectedResult = "";
      assertThat(result, is(expectedResult));
    }
  }
}
