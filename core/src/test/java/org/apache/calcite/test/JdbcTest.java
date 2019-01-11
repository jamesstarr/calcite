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

import org.apache.calcite.adapter.clone.CloneSchema;
import org.apache.calcite.adapter.generate.RangeTable;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.HandlerImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
>>>>>>> 0f9894602 (Use real properties, not their names, in tests)
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
            + "EnumerableCalc(expr#0..7=[{inputs}], expr#8=[0:BIGINT], expr#9=[>($t4, $t8)], expr#10=[CAST($t5):JavaType(class java.lang.Float)], expr#11=[null:JavaType(class java.lang.Float)], expr#12=[CASE($t9, $t10, $t11)], expr#13=[5], deptno=[$t1], empid=[$t0], S=[$t12], FIVE=[$t13], M=[$t6], C=[$t7])\n"
            + "  EnumerableWindow(window#0=[window(partition {1} order by [0] rows between $4 PRECEDING and CURRENT ROW aggs [COUNT($3), $SUM0($3), MIN($2), COUNT()])])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[+($t3, $t0)], proj#0..1=[{exprs}], salary=[$t3], $3=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, emps]])\n")
        .returnsUnordered(
            "deptno=10; empid=100; S=10100.0; FIVE=5; M=10000.0; C=1",
            "deptno=10; empid=110; S=21710.0; FIVE=5; M=10000.0; C=2",
            "deptno=10; empid=150; S=18760.0; FIVE=5; M=7000.0; C=2",
            "deptno=20; empid=200; S=8200.0; FIVE=5; M=8000.0; C=1")
        .planContains(CalciteSystemProperty.DEBUG.value()
            ? "_list.add(new Object[] {\n"
            + "        row[0],\n" // box-unbox is optimized
            + "        row[1],\n"
            + "        row[2],\n"
            + "        row[3],\n"
            + "        COUNTa0w0,\n"
            + "        $SUM0a1w0,\n"
            + "        MINa2w0,\n"
            + "        COUNTa3w0});"
            : "_list.add(new Object[] {\n"
            + "        row[0],\n" // box-unbox is optimized
            + "        row[1],\n"
            + "        row[2],\n"
            + "        row[3],\n"
            + "        a0w0,\n"
            + "        a1w0,\n"
            + "        a2w0,\n"
            + "        a3w0});")
        .planContains("return new Object[] {\n"
            + "                  current[1],\n"
            + "                  current[0],\n"
            // Float.valueOf(SqlFunctions.toFloat(current[5])) comes from SUM0
            + "                  org.apache.calcite.runtime.SqlFunctions.toLong(current[4]) > 0L ? Float.valueOf(org.apache.calcite.runtime.SqlFunctions.toFloat(current[5])) : (Float) null,\n"
            + "                  5,\n"
            + "                  current[6],\n"
            + "                  current[7]};\n");
  }

  /** Tests windowed aggregation with multiple windows.
   * One window straddles the current row.
   * Some windows have no PARTITION BY clause. */
  @Test void testWinAgg2() {
    CalciteAssert.hr()
        .query("select"
            + " \"deptno\",\n"
            + " \"empid\",\n"
            + "sum(\"salary\" + \"empid\") over w as s,\n"
            + " 5 as five,\n"
            + " min(\"salary\") over w as m,\n"
            + " count(*) over w as c,\n"
            + " count(*) over w2 as c2,\n"
            + " count(*) over w11 as c11,\n"
            + " count(*) over w11dept as c11dept\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"empid\" rows 1 preceding),\n"
            + " w2 as (order by \"empid\" rows 2 preceding),\n"
            + " w11 as (order by \"empid\" rows between 1 preceding and 1 following),\n"
            + " w11dept as (partition by \"deptno\" order by \"empid\" rows between 1 preceding and 1 following)")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, S REAL, FIVE INTEGER NOT NULL, M REAL, C BIGINT NOT NULL, C2 BIGINT NOT NULL, C11 BIGINT NOT NULL, C11DEPT BIGINT NOT NULL]")
        // Check that optimizes for window whose PARTITION KEY is empty
        .planContains("tempList.size()")
        .returnsUnordered(
            "deptno=20; empid=200; S=15350.0; FIVE=5; M=7000.0; C=2; C2=3; C11=2; C11DEPT=1",
            "deptno=10; empid=100; S=10100.0; FIVE=5; M=10000.0; C=1; C2=1; C11=2; C11DEPT=2",
            "deptno=10; empid=110; S=21710.0; FIVE=5; M=10000.0; C=2; C2=2; C11=3; C11DEPT=3",
            "deptno=10; empid=150; S=18760.0; FIVE=5; M=7000.0; C=2; C2=3; C11=3; C11DEPT=2");
  }

  /**
   * Tests that window aggregates work when computed over non-nullable
   * {@link org.apache.calcite.adapter.enumerable.JavaRowFormat#SCALAR} inputs.
   * Window aggregates use temporary buffers, thus need to check if
   * primitives are properly boxed and un-boxed.
   */
  @Test void testWinAggScalarNonNullPhysType() {
    String planLine =
        "a0s0w0 = org.apache.calcite.runtime.SqlFunctions.lesser(a0s0w0, org.apache.calcite.runtime.SqlFunctions.toFloat(_rows[j]));";
    if (CalciteSystemProperty.DEBUG.value()) {
      planLine = planLine.replace("a0s0w0", "MINa0s0w0");
    }
    CalciteAssert.hr()
        .query("select min(\"salary\"+1) over w as m\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"salary\"+1 rows 1 preceding)\n")
        .typeIs(
            "[M REAL]")
        .planContains(planLine)
        .returnsUnordered(
            "M=7001.0",
            "M=7001.0",
            "M=8001.0",
            "M=10001.0");
  }

  /**
   * Tests that {@link org.apache.calcite.rel.logical.LogicalCalc} is
   * implemented properly when input is
   * {@link org.apache.calcite.rel.logical.LogicalWindow} and literal.
   */
  @Test void testWinAggScalarNonNullPhysTypePlusOne() {
    String planLine =
        "a0s0w0 = org.apache.calcite.runtime.SqlFunctions.lesser(a0s0w0, org.apache.calcite.runtime.SqlFunctions.toFloat(_rows[j]));";
    if (CalciteSystemProperty.DEBUG.value()) {
      planLine = planLine.replace("a0s0w0", "MINa0s0w0");
    }
    CalciteAssert.hr()
        .query("select 1+min(\"salary\"+1) over w as m\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"salary\"+1 rows 1 preceding)\n")
        .typeIs(
            "[M REAL]")
        .planContains(planLine)
        .returnsUnordered(
            "M=7002.0",
            "M=7002.0",
            "M=8002.0",
            "M=10002.0");
  }

  /** Tests for RANK and ORDER BY ... DESCENDING, NULLS FIRST, NULLS LAST. */
  @Test void testWinAggRank() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " rank() over (partition by \"deptno\" order by \"commission\" desc nulls first) as rcnf,\n"
            + " rank() over (partition by \"deptno\" order by \"commission\" desc nulls last) as rcnl,\n"
            + " rank() over (partition by \"deptno\" order by \"empid\") as r,\n"
            + " rank() over (partition by \"deptno\" order by \"empid\" desc) as rd\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, RCNF BIGINT NOT NULL, RCNL BIGINT NOT NULL, R BIGINT NOT NULL, RD BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; RCNF=2; RCNL=1; R=1; RD=3",
            "deptno=10; empid=110; commission=250; RCNF=3; RCNL=2; R=2; RD=2",
            "deptno=10; empid=150; commission=null; RCNF=1; RCNL=3; R=3; RD=1",
            "deptno=20; empid=200; commission=500; RCNF=1; RCNL=1; R=1; RD=1");
  }

  /** Tests for RANK with same values */
  @Test void testWinAggRankValues() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " rank() over (order by \"deptno\") as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=20; R=4"); // 4 for rank and 2 for dense_rank
  }

  /** Tests for RANK with same values */
  @Test void testWinAggRankValuesDesc() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " rank() over (order by \"deptno\" desc) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=20; R=1");
  }

  /** Tests for DENSE_RANK with same values */
  @Test void testWinAggDenseRankValues() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " dense_rank() over (order by \"deptno\") as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=20; R=2");
  }

  /** Tests for DENSE_RANK with same values */
  @Test void testWinAggDenseRankValuesDesc() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " dense_rank() over (order by \"deptno\" desc) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=20; R=1");
  }

  /** Tests for DATE +- INTERVAL window frame */
  @Test void testWinIntervalFrame() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"hire_date\",\n"
            + " count(*) over (partition by \"deptno\" order by \"hire_date\""
            + " range between interval '1' year preceding and interval '1' year following) as r\n"
            + "from (select \"empid\", \"deptno\",\n"
            + "  DATE '2014-06-12' + \"empid\"*interval '0' day \"hire_date\"\n"
            + "  from \"hr\".\"emps\")")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, hire_date DATE NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered("deptno=10; empid=100; hire_date=2014-06-12; R=3",
            "deptno=10; empid=110; hire_date=2014-06-12; R=3",
            "deptno=10; empid=150; hire_date=2014-06-12; R=3",
            "deptno=20; empid=200; hire_date=2014-06-12; R=1");
  }

  @Test void testNestedWin() {
    CalciteAssert.hr()
        .query("select\n"
            + " lag(a2, 1, 0) over (partition by \"deptno\" order by a1) as lagx\n"
            + "from\n"
            + " (\n"
            + "  select\n"
            + "   \"deptno\",\n"
            + "   \"salary\" / \"commission\" as a1,\n"
            + "   sum(\"commission\") over ( partition by \"deptno\" order by \"salary\" / "
            + "\"commission\") / sum(\"commission\") over (partition by \"deptno\") as a2\n"
            + "  from\n"
            + "   \"hr\".\"emps\"\n"
            + " )\n")
        .typeIs(
            "[LAGX INTEGER NOT NULL]")
        .returnsUnordered(
            "LAGX=0",
            "LAGX=0",
            "LAGX=0",
            "LAGX=1");
  }

  private void startOfGroupStep1(String startOfGroup) {
    CalciteAssert.that()
        .query("select t.*\n"
            + "  from (\n"
            + "       select  t.*,\n"
            + "               case when " + startOfGroup
            + " then 0 else 1 end start_of_group\n"
            + "         from "
            + START_OF_GROUP_DATA
            + ") t\n")
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, START_OF_GROUP INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; START_OF_GROUP=1",
            "RN=2; VAL=0; EXPECTED=1; START_OF_GROUP=0",
            "RN=3; VAL=1; EXPECTED=2; START_OF_GROUP=1",
            "RN=4; VAL=0; EXPECTED=3; START_OF_GROUP=1",
            "RN=5; VAL=0; EXPECTED=3; START_OF_GROUP=0",
            "RN=6; VAL=0; EXPECTED=3; START_OF_GROUP=0",
            "RN=7; VAL=1; EXPECTED=4; START_OF_GROUP=1",
            "RN=8; VAL=1; EXPECTED=4; START_OF_GROUP=0");
  }

  private void startOfGroupStep2(String startOfGroup) {
    CalciteAssert.that()
        .query("select t.*\n"
            // current row is assumed, group_id should be NOT NULL
            + "       ,sum(start_of_group) over (order by rn rows unbounded preceding) group_id\n"
            + "  from (\n"
            + "       select  t.*,\n"
            + "               case when " + startOfGroup
            + " then 0 else 1 end start_of_group\n"
            + "         from "
            + START_OF_GROUP_DATA
            + ") t\n")
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, START_OF_GROUP INTEGER NOT NULL, GROUP_ID INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; START_OF_GROUP=1; GROUP_ID=1",
            "RN=2; VAL=0; EXPECTED=1; START_OF_GROUP=0; GROUP_ID=1",
            "RN=3; VAL=1; EXPECTED=2; START_OF_GROUP=1; GROUP_ID=2",
            "RN=4; VAL=0; EXPECTED=3; START_OF_GROUP=1; GROUP_ID=3",
            "RN=5; VAL=0; EXPECTED=3; START_OF_GROUP=0; GROUP_ID=3",
            "RN=6; VAL=0; EXPECTED=3; START_OF_GROUP=0; GROUP_ID=3",
            "RN=7; VAL=1; EXPECTED=4; START_OF_GROUP=1; GROUP_ID=4",
            "RN=8; VAL=1; EXPECTED=4; START_OF_GROUP=0; GROUP_ID=4");
  }

  private void startOfGroupStep3(String startOfGroup) {
    CalciteAssert.that()
        .query("select group_id, min(rn) min_rn, max(rn) max_rn,\n"
            + "  count(rn) cnt_rn, avg(val) avg_val"
            + " from (\n"
            + "select t.*\n"
            // current row is assumed, group_id should be NOT NULL
            + "       ,sum(start_of_group) over (order by rn rows unbounded preceding) group_id\n"
            + "  from (\n"
            + "       select  t.*,\n"
            + "               case when " + startOfGroup
            + " then 0 else 1 end start_of_group\n"
            + "         from "
            + START_OF_GROUP_DATA
            + ") t\n"
            + ") group by group_id\n")
        .typeIs(
            "[GROUP_ID INTEGER NOT NULL, MIN_RN INTEGER NOT NULL, MAX_RN INTEGER NOT NULL, CNT_RN BIGINT NOT NULL, AVG_VAL INTEGER NOT NULL]")
        .returnsUnordered(
            "GROUP_ID=1; MIN_RN=1; MAX_RN=2; CNT_RN=2; AVG_VAL=0",
            "GROUP_ID=2; MIN_RN=3; MAX_RN=3; CNT_RN=1; AVG_VAL=1",
            "GROUP_ID=3; MIN_RN=4; MAX_RN=6; CNT_RN=3; AVG_VAL=0",
            "GROUP_ID=4; MIN_RN=7; MAX_RN=8; CNT_RN=2; AVG_VAL=1");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step1, implemented as last_value.
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLastValueStep1() {
    startOfGroupStep1(
        "val = last_value(val) over (order by rn rows between 1 preceding and 1 preceding)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step2, that gets the final group numbers
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLastValueStep2() {
    startOfGroupStep2(
        "val = last_value(val) over (order by rn rows between 1 preceding and 1 preceding)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step3, that aggregates the computed groups
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLastValueStep3() {
    startOfGroupStep3(
        "val = last_value(val) over (order by rn rows between 1 preceding and 1 preceding)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step1, implemented as last_value.
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLagStep1() {
    startOfGroupStep1("val = lag(val) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step2, that gets the final group numbers
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLagValueStep2() {
    startOfGroupStep2("val = lag(val) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step3, that aggregates the computed groups
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLagStep3() {
    startOfGroupStep3("val = lag(val) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step1, implemented as last_value.
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLeadStep1() {
    startOfGroupStep1("val = lead(val, -1) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step2, that gets the final group numbers
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLeadValueStep2() {
    startOfGroupStep2("val = lead(val, -1) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step3, that aggregates the computed groups
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test void testStartOfGroupLeadStep3() {
    startOfGroupStep3("val = lead(val, -1) over (order by rn)");
  }

  /**
   * Tests default value of LAG function.
   */
  @Test void testLagDefaultValue() {
    CalciteAssert.that()
        .query("select t.*, lag(rn+expected,1,42) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; L=42",
            "RN=2; VAL=0; EXPECTED=1; L=2",
            "RN=3; VAL=1; EXPECTED=2; L=3",
            "RN=4; VAL=0; EXPECTED=3; L=5",
            "RN=5; VAL=0; EXPECTED=3; L=7",
            "RN=6; VAL=0; EXPECTED=3; L=8",
            "RN=7; VAL=1; EXPECTED=4; L=9",
            "RN=8; VAL=1; EXPECTED=4; L=11");
  }

  /**
   * Tests default value of LEAD function.
   */
  @Test void testLeadDefaultValue() {
    CalciteAssert.that()
        .query("select t.*, lead(rn+expected,1,42) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; L=3",
            "RN=2; VAL=0; EXPECTED=1; L=5",
            "RN=3; VAL=1; EXPECTED=2; L=7",
            "RN=4; VAL=0; EXPECTED=3; L=8",
            "RN=5; VAL=0; EXPECTED=3; L=9",
            "RN=6; VAL=0; EXPECTED=3; L=11",
            "RN=7; VAL=1; EXPECTED=4; L=12",
            "RN=8; VAL=1; EXPECTED=4; L=42");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Test void testLagExpressionOffset() {
    CalciteAssert.that()
        .query("select t.*, lag(rn, expected, 42) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; L=42",
            "RN=2; VAL=0; EXPECTED=1; L=1",
            "RN=3; VAL=1; EXPECTED=2; L=1",
            "RN=4; VAL=0; EXPECTED=3; L=1",
            "RN=5; VAL=0; EXPECTED=3; L=2",
            "RN=6; VAL=0; EXPECTED=3; L=3",
            "RN=7; VAL=1; EXPECTED=4; L=3",
            "RN=8; VAL=1; EXPECTED=4; L=4");
  }

  /**
   * Tests DATE as offset argument of LAG function.
   */
  @Test void testLagInvalidOffsetArgument() {
    CalciteAssert.that()
        .query("select t.*,\n"
            + "  lag(rn, DATE '2014-06-20', 42) over (order by rn) l\n"
            + "from " + START_OF_GROUP_DATA)
        .throws_(
            "Cannot apply 'LAG' to arguments of type 'LAG(<INTEGER>, <DATE>, <INTEGER>)'");
  }

  /**
   * Tests LAG function with IGNORE NULLS.
   */
  @Test void testLagIgnoreNulls() {
    final String sql = "select\n"
        + "  lag(rn, expected, 42) ignore nulls over (w) l,\n"
        + "  lead(rn, expected) over (w),\n"
        + "  lead(rn, expected) over (order by expected)\n"
        + "from (values"
        + "  (1,0,1),\n"
        + "  (2,0,1),\n"
        + "  (2,0,1),\n"
        + "  (3,1,2),\n"
        + "  (4,0,3),\n"
        + "  (cast(null as int),0,3),\n"
        + "  (5,0,3),\n"
        + "  (6,0,3),\n"
        + "  (7,1,4),\n"
        + "  (8,1,4)) as t(rn,val,expected)\n"
        + "window w as (order by rn)";
    CalciteAssert.that().query(sql)
        .throws_("IGNORE NULLS not supported");
  }

  /**
   * Tests NTILE(2).
   */
  @Test void testNtile1() {
    CalciteAssert.that()
        .query("select rn, ntile(1) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, L BIGINT NOT NULL]")
        .returnsUnordered(
            "RN=1; L=1",
            "RN=2; L=1",
            "RN=3; L=1",
            "RN=4; L=1",
            "RN=5; L=1",
            "RN=6; L=1",
            "RN=7; L=1",
            "RN=8; L=1");
  }

  /**
   * Tests NTILE(2).
   */
  @Test void testNtile2() {
    CalciteAssert.that()
        .query("select rn, ntile(2) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, L BIGINT NOT NULL]")
        .returnsUnordered(
            "RN=1; L=1",
            "RN=2; L=1",
            "RN=3; L=1",
            "RN=4; L=1",
            "RN=5; L=2",
            "RN=6; L=2",
            "RN=7; L=2",
            "RN=8; L=2");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Disabled("Have no idea how to validate that expression is constant")
  @Test void testNtileConstantArgs() {
    CalciteAssert.that()
        .query("select rn, ntile(1+1) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; L=1",
            "RN=2; L=1",
            "RN=3; L=1",
            "RN=4; L=1",
            "RN=5; L=2",
            "RN=6; L=2",
            "RN=7; L=2",
            "RN=8; L=2");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Test void testNtileNegativeArg() {
    CalciteAssert.that()
        .query("select rn, ntile(-1) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .throws_(
            "Argument to function 'NTILE' must be a positive integer literal");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Test void testNtileDecimalArg() {
    CalciteAssert.that()
        .query("select rn, ntile(3.141592653) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .throws_(
            "Cannot apply 'NTILE' to arguments of type 'NTILE(<DECIMAL(10, 9)>)'");
  }

  /** Tests for FIRST_VALUE */
  @Test void testWinAggFirstValue() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " first_value(\"commission\") over (partition by \"deptno\" order by \"empid\") as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R INTEGER]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=1000",
            "deptno=10; empid=110; commission=250; R=1000",
            "deptno=10; empid=150; commission=null; R=1000",
            "deptno=20; empid=200; commission=500; R=500");
  }

  /** Tests for FIRST_VALUE desc */
  @Test void testWinAggFirstValueDesc() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " first_value(\"commission\") over (partition by \"deptno\" order by \"empid\" desc) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R INTEGER]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=null",
            "deptno=10; empid=110; commission=250; R=null",
            "deptno=10; empid=150; commission=null; R=null",
            "deptno=20; empid=200; commission=500; R=500");
  }

  /** Tests for FIRST_VALUE empty window */
  @Test void testWinAggFirstValueEmptyWindow() {
    CalciteAssert.hr()
        .query("select \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " first_value(\"commission\") over (partition by \"deptno\" order by \"empid\" desc range between 1000 preceding and 999 preceding) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R INTEGER]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=null",
            "deptno=10; empid=110; commission=250; R=null",
            "deptno=10; empid=150; commission=null; R=null",
            "deptno=20; empid=200; commission=500; R=null");
  }

  /** Tests for ROW_NUMBER */
  @Test void testWinRowNumber() {
    CalciteAssert.hr()
        .query("select \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " row_number() over (partition by \"deptno\") as r,\n"
            + " row_number() over (partition by \"deptno\" order by \"commission\" desc nulls first) as rcnf,\n"
            + " row_number() over (partition by \"deptno\" order by \"commission\" desc nulls last) as rcnl,\n"
            + " row_number() over (partition by \"deptno\" order by \"empid\") as r,\n"
            + " row_number() over (partition by \"deptno\" order by \"empid\" desc) as rd\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R BIGINT NOT NULL, RCNF BIGINT NOT NULL, RCNL BIGINT NOT NULL, R BIGINT NOT NULL, RD BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=1; RCNF=2; RCNL=1; R=1; RD=3",
            "deptno=10; empid=110; commission=250; R=3; RCNF=3; RCNL=2; R=2; RD=2",
            "deptno=10; empid=150; commission=null; R=2; RCNF=1; RCNL=3; R=3; RD=1",
            "deptno=20; empid=200; commission=500; R=1; RCNF=1; RCNL=1; R=1; RD=1");
  }

  /** Tests UNBOUNDED PRECEDING clause. */
  @Test void testOverUnboundedPreceding() {
    CalciteAssert.hr()
        .query("select \"empid\",\n"
            + "  \"commission\",\n"
            + "  count(\"empid\") over (partition by 42\n"
            + "    order by \"commission\" nulls first\n"
            + "    rows between UNBOUNDED PRECEDING and current row) as m\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[empid INTEGER NOT NULL, commission INTEGER, M BIGINT NOT NULL]")
        .returnsUnordered(
            "empid=100; commission=1000; M=4",
            "empid=200; commission=500; M=3",
            "empid=150; commission=null; M=1",
            "empid=110; commission=250; M=2");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3563">[CALCITE-3563]
   * When resolving method call in calcite runtime, add type check and match
   * mechanism for input arguments</a>. */
  @Test void testMethodParameterTypeMatch() {
    CalciteAssert.that()
        .query("SELECT mod(12.5, cast(3 as bigint))")
        .planContains("final java.math.BigDecimal v = "
            + "$L4J$C$new_java_math_BigDecimal_12_5_")
        .planContains("org.apache.calcite.runtime.SqlFunctions.mod(v, "
            + "$L4J$C$new_java_math_BigDecimal_3L_)")
        .returns("EXPR$0=0.5\n");
  }

  /** Tests UNBOUNDED PRECEDING clause. */
  @Test void testSumOverUnboundedPreceding() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"empid\",\n"
            + "  \"commission\",\n"
            + "  sum(\"empid\") over (partition by 42\n"
            + "    order by \"commission\" nulls first\n"
            + "    rows between UNBOUNDED PRECEDING and current row) as m\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[empid INTEGER NOT NULL, commission INTEGER, M INTEGER NOT NULL]")
        .returnsUnordered(
            "empid=100; commission=1000; M=560",
            "empid=110; commission=250; M=260",
            "empid=150; commission=null; M=150",
            "empid=200; commission=500; M=460");
  }

  /** Tests that sum over possibly empty window is nullable. */
  @Test void testSumOverPossiblyEmptyWindow() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"empid\",\n"
            + "  \"commission\",\n"
            + "  sum(\"empid\") over (partition by 42\n"
            + "    order by \"commission\" nulls first\n"
            + "    rows between UNBOUNDED PRECEDING and 1 preceding) as m\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[empid INTEGER NOT NULL, commission INTEGER, M INTEGER]")
        .returnsUnordered(
            "empid=100; commission=1000; M=460",
            "empid=110; commission=250; M=150",
            "empid=150; commission=null; M=null",
            "empid=200; commission=500; M=260");
  }

  /** Tests windowed aggregation with no ORDER BY clause.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-285">[CALCITE-285]
   * Window functions throw exception without ORDER BY</a>.
   *
   * <p>Note:</p>
   *
   * <ul>
   * <li>With no ORDER BY, the window is over all rows in the partition.
   * <li>With an ORDER BY, the implicit frame is 'RANGE BETWEEN
   *     UNBOUNDED PRECEDING AND CURRENT ROW'.
   * <li>With no ORDER BY or PARTITION BY, the window contains all rows in the
   *     table.
   * </ul>
   */
  @Test void testOverNoOrder() {
    // If no range is specified, default is "RANGE BETWEEN UNBOUNDED PRECEDING
    // AND CURRENT ROW".
    // The aggregate function is within the current partition;
    // if there is no partition, that means the whole table.
    // Rows are deemed "equal to" the current row per the ORDER BY clause.
    // If there is no ORDER BY clause, CURRENT ROW has the same effect as
    // UNBOUNDED FOLLOWING; that is, no filtering effect at all.
    final String sql = "select *,\n"
        + " count(*) over (partition by deptno) as m1,\n"
        + " count(*) over (partition by deptno order by ename) as m2,\n"
        + " count(*) over () as m3\n"
        + "from emp";
    withEmpDept(sql).returnsUnordered(
        "ENAME=Adam ; DEPTNO=50; GENDER=M; M1=2; M2=1; M3=9",
        "ENAME=Alice; DEPTNO=30; GENDER=F; M1=2; M2=1; M3=9",
        "ENAME=Bob  ; DEPTNO=10; GENDER=M; M1=2; M2=1; M3=9",
        "ENAME=Eric ; DEPTNO=20; GENDER=M; M1=1; M2=1; M3=9",
        "ENAME=Eve  ; DEPTNO=50; GENDER=F; M1=2; M2=2; M3=9",
        "ENAME=Grace; DEPTNO=60; GENDER=F; M1=1; M2=1; M3=9",
        "ENAME=Jane ; DEPTNO=10; GENDER=F; M1=2; M2=2; M3=9",
        "ENAME=Susan; DEPTNO=30; GENDER=F; M1=2; M2=2; M3=9",
        "ENAME=Wilma; DEPTNO=null; GENDER=F; M1=1; M2=1; M3=9");
  }

  /** Tests that field-trimming creates a project near the table scan. */
  @Test void testTrimFields() throws Exception {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      CalciteAssert.hr()
          .query("select \"name\", count(\"commission\") + 1\n"
              + "from \"hr\".\"emps\"\n"
              + "group by \"deptno\", \"name\"")
          .convertContains("LogicalProject(name=[$1], EXPR$1=[+($2, 1)])\n"
              + "  LogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)])\n"
              + "    LogicalProject(deptno=[$1], name=[$2], commission=[$4])\n"
              + "      LogicalTableScan(table=[[hr, emps]])\n");
    }
  }

  /** Tests that field-trimming creates a project near the table scan, in a
   * query with windowed-aggregation. */
  @Test void testTrimFieldsOver() throws Exception {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      // The correct plan has a project on a filter on a project on a scan.
      CalciteAssert.hr()
          .query("select \"name\",\n"
              + "  count(\"commission\") over (partition by \"deptno\") + 1\n"
              + "from \"hr\".\"emps\"\n"
              + "where \"empid\" > 10")
          .convertContains(""
              + "LogicalProject(name=[$2], EXPR$1=[+(COUNT($3) OVER (PARTITION BY $1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 1)])\n"
              + "  LogicalFilter(condition=[>($0, 10)])\n"
              + "    LogicalProject(empid=[$0], deptno=[$1], name=[$2], commission=[$4])\n"
              + "      LogicalTableScan(table=[[hr, emps]])\n");
    }
  }

  /** Tests window aggregate whose argument is a constant. */
  @Test void testWinAggConstant() {
    CalciteAssert.hr()
        .query("select max(1) over (partition by \"deptno\"\n"
            + "  order by \"empid\") as m\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "M=1",
            "M=1",
            "M=1",
            "M=1");
  }

  /** Tests multiple window aggregates over constants.
   * This tests that EnumerableWindowRel is able to reference the right slot
   * when accessing constant for aggregation argument. */
  @Test void testWinAggConstantMultipleConstants() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"deptno\", sum(1) over (partition by \"deptno\"\n"
            + "  order by \"empid\" rows between unbounded preceding and current row) as a,\n"
            + " sum(-1) over (partition by \"deptno\"\n"
            + "  order by \"empid\" rows between unbounded preceding and current row) as b\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "deptno=10; A=1; B=-1",
            "deptno=10; A=2; B=-2",
            "deptno=10; A=3; B=-3",
            "deptno=20; A=1; B=-1");
  }

  /** Tests window aggregate PARTITION BY constant. */
  @Test void testWinAggPartitionByConstant() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(""
            // *0 is used to make results predictable.
            // If using just max(empid) calcite cannot compute the result
            // properly since it does not support range windows yet :(
            + "select max(\"empid\"*0) over (partition by 42\n"
            + "  order by \"empid\") as m\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "M=0",
            "M=0",
            "M=0",
            "M=0");
  }

  /** Tests window aggregate ORDER BY constant. Unlike in SELECT ... ORDER BY,
   * the constant does not mean a column. It means a constant, therefore the
   * order of the rows is not changed. */
  @Test void testWinAggOrderByConstant() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(""
            // *0 is used to make results predictable.
            // If using just max(empid) calcite cannot compute the result
            // properly since it does not support range windows yet :(
            + "select max(\"empid\"*0) over (partition by \"deptno\"\n"
            + "  order by 42) as m\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "M=0",
            "M=0",
            "M=0",
            "M=0");
  }

  /** Tests WHERE comparing a nullable integer with an integer literal. */
  @Test void testWhereNullable() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select * from \"hr\".\"emps\"\n"
            + "where \"commission\" > 800")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Test case for rewriting queries that contain {@code GROUP_ID()} function.
   * For instance, the query
   * {@code
   *    select deptno, group_id() as gid
   *    from scott.emp
   *    group by grouping sets(deptno, deptno, deptno, (), ())
   * }
   * will be converted into:
   * {@code
   *    select deptno, 0 as gid
   *    from scott.emp group by grouping sets(deptno, ())
   *    union all
   *    select deptno, 1 as gid
   *    from scott.emp group by grouping sets(deptno, ())
   *    union all
   *    select deptno, 2 as gid
   *    from scott.emp group by grouping sets(deptno)
   * }
   */
  @Test void testGroupId() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.SCOTT)
        .query("select deptno, group_id() + 1 as g, count(*) as c\n"
            + "from \"scott\".emp\n"
            + "group by grouping sets (deptno, deptno, deptno, (), ())\n"
            + "having group_id() > 0")
        .explainContains("EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[+($t1, $t3)], "
            + "expr#5=[0], expr#6=[>($t1, $t5)], DEPTNO=[$t0], G=[$t4], C=[$t2], $condition=[$t6])\n"
            + "  EnumerableUnion(all=[true])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[0:BIGINT], DEPTNO=[$t0], $f1=[$t2], C=[$t1])\n"
            + "      EnumerableAggregate(group=[{7}], groups=[[{7}, {}]], C=[COUNT()])\n"
            + "        EnumerableTableScan(table=[[scott, EMP]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1:BIGINT], DEPTNO=[$t0], $f1=[$t2], C=[$t1])\n"
            + "      EnumerableAggregate(group=[{7}], groups=[[{7}, {}]], C=[COUNT()])\n"
            + "        EnumerableTableScan(table=[[scott, EMP]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[2:BIGINT], DEPTNO=[$t0], $f1=[$t2], C=[$t1])\n"
            + "      EnumerableAggregate(group=[{7}], C=[COUNT()])\n"
            + "        EnumerableTableScan(table=[[scott, EMP]])")
        .returnsUnordered("DEPTNO=10; G=2; C=3",
            "DEPTNO=10; G=3; C=3",
            "DEPTNO=20; G=2; C=5",
            "DEPTNO=20; G=3; C=5",
            "DEPTNO=30; G=2; C=6",
            "DEPTNO=30; G=3; C=6",
            "DEPTNO=null; G=2; C=14");
  }

  /** Tests CALCITE-980: Not (C='a' or C='b') causes NPE */
  @Test void testWhereOrAndNullable() {
    /* Generates the following code:
       public boolean moveNext() {
         while (inputEnumerator.moveNext()) {
           final Object[] current = (Object[]) inputEnumerator.current();
           final String inp0_ = current[0] == null ? (String) null : current[0].toString();
           final String inp1_ = current[1] == null ? (String) null : current[1].toString();
           if (inp0_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp0_, "a")
               && (inp1_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp1_, "b"))
               || inp0_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp0_, "b")
               && (inp1_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp1_, "c"))) {
             return true;
           }
         }
         return false;
       }
     */
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("with tst(c) as (values('a'),('b'),('c'),(cast(null as varchar)))"
            + " select u.c u, v.c v from tst u, tst v where ((u.c = 'a' and v.c = 'b') or (u.c = 'b' and v.c = 'c'))")
        .returnsUnordered(
            "U=a; V=b",
            "U=b; V=c");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-980">[CALCITE-980]
   * different flavors of boolean logic</a>.
   *
   * @see QuidemTest sql/conditions.iq */
  @Disabled("Fails with org.codehaus.commons.compiler.CompileException: Line 16, Column 112:"
      + " Cannot compare types \"int\" and \"java.lang.String\"\n")
  @Test void testComparingIntAndString() throws Exception {
    // if (((...test.ReflectiveSchemaTest.IntAndString) inputEnumerator.current()).id == "T")

    CalciteAssert.that()
        .withSchema("s",
            new ReflectiveSchema(
                new ReflectiveSchemaTest.CatchallSchema()))
        .query("select a.\"value\", b.\"value\"\n"
            + "  from \"bools\" a\n"
            + "     , \"bools\" b\n"
            + " where b.\"value\" = 'T'\n"
            + " order by 1, 2")
        .returnsUnordered(
            "should fail with 'not a number' sql error while converting text to number");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1015">[CALCITE-1015]
   * OFFSET 0 causes AssertionError</a>. */
  @Test void testTrivialSort() {
    final String sql = "select a.\"value\", b.\"value\"\n"
        + "  from \"bools\" a\n"
        + "     , \"bools\" b\n"
        + " offset 0";
    CalciteAssert.that()
        .withSchema("s",
            new ReflectiveSchema(
                new ReflectiveSchemaTest.CatchallSchema()))
        .query(sql)
        .returnsUnordered("value=T; value=T",
            "value=T; value=F",
            "value=T; value=null",
            "value=F; value=T",
            "value=F; value=F",
            "value=F; value=null",
            "value=null; value=T",
            "value=null; value=F",
            "value=null; value=null");
  }

  /** Tests the LIKE operator. */
  @Test void testLike() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select * from \"hr\".\"emps\"\n"
            + "where \"name\" like '%i__'")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n");
  }

  /** Tests array index. */
  @Test void testArrayIndexing() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(
            "select \"deptno\", \"employees\"[1] as e from \"hr\".\"depts\"\n").returnsUnordered(
        "deptno=10; E={100, 10, Bill, 10000.0, 1000}",
        "deptno=30; E=null",
        "deptno=40; E={200, 20, Eric, 8000.0, 500}");
  }

  @Test void testVarcharEquals() {
    CalciteAssert.model(FOODMART_MODEL)
        .query("select \"lname\" from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=Nowmer\n");

    // lname is declared as VARCHAR(30), comparing it with a string longer
    // than 30 characters would introduce a cast to the least restrictive
    // type, thus lname would be cast to a varchar(40) in this case.
    // These sorts of casts are removed though when constructing the jdbc
    // sql, since e.g. HSQLDB does not support them.
    CalciteAssert.model(FOODMART_MODEL)
        .query("select count(*) as c from \"customer\" "
            + "where \"lname\" = 'this string is longer than 30 characters'")
        .returns("C=0\n");

    CalciteAssert.model(FOODMART_MODEL)
        .query("select count(*) as c from \"customer\" "
            + "where cast(\"customer_id\" as char(20)) = 'this string is longer than 30 characters'")
        .returns("C=0\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1153">[CALCITE-1153]
   * Invalid CAST when push JOIN down to Oracle</a>. */
  @Test void testJoinMismatchedVarchar() {
    final String sql = "select count(*) as c\n"
        + "from \"customer\" as c\n"
        + "join \"product\" as p on c.\"lname\" = p.\"brand_name\"";
    CalciteAssert.model(FOODMART_MODEL)
        .query(sql)
        .returns("C=607\n");
  }

  @Test void testIntersectMismatchedVarchar() {
    final String sql = "select count(*) as c from (\n"
        + "  select \"lname\" from \"customer\" as c\n"
        + "  intersect\n"
        + "  select \"brand_name\" from \"product\" as p)";
    CalciteAssert.model(FOODMART_MODEL)
        .query(sql)
        .returns("C=12\n");
  }

  /** Tests the NOT IN operator. Problems arose in code-generation because
   * the column allows nulls. */
  @Test void testNotIn() {
    // Expand needs to be true since there is a
    // NOT IN(a, b, NULL) and the correlated variable isn't executable on JDBC
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      predicate("\"name\" not in ('a', 'b') or \"name\" is null")
              .returns(""
                      + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
                      + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
                      + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
                      + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");

      // And some similar combinations...
      predicate("\"name\" in ('a', 'b') or \"name\" is null");
      predicate("\"name\" in ('a', 'b', null) or \"name\" is null");
      predicate("\"name\" in ('a', 'b') or \"name\" is not null");
      predicate("\"name\" in ('a', 'b', null) or \"name\" is not null");
      predicate("\"name\" not in ('a', 'b', null) or \"name\" is not null");
      predicate("\"name\" not in ('a', 'b', null) and \"name\" is not null");
    }
  }

  @Test void testNotInEmptyQuery() {
    // RHS is empty, therefore returns all rows from emp, including the one
    // with deptno = NULL.
    final String sql = "select deptno from emp where deptno not in (\n"
        + "select deptno from dept where deptno = -1)";
    withEmpDept(sql)
//        .explainContains("EnumerableCalc(expr#0..2=[{inputs}], "
//            + "expr#3=[IS NOT NULL($t2)], expr#4=[true], "
//            + "expr#5=[IS NULL($t0)], expr#6=[null], expr#7=[false], "
//            + "expr#8=[CASE($t3, $t4, $t5, $t6, $t7)], expr#9=[NOT($t8)], "
//            + "EXPR$1=[$t0], $condition=[$t9])")
        .returnsUnordered("DEPTNO=null",
            "DEPTNO=10",
            "DEPTNO=10",
            "DEPTNO=20",
            "DEPTNO=30",
            "DEPTNO=30",
            "DEPTNO=50",
            "DEPTNO=50",
            "DEPTNO=60");
  }

  @Test void testNotInQuery() {
    // None of the rows from RHS is NULL.
    final String sql = "select deptno from emp where deptno not in (\n"
        + "select deptno from dept)";
    withEmpDept(sql)
        .returnsUnordered("DEPTNO=50",
            "DEPTNO=50",
            "DEPTNO=60");
  }

  @Test void testNotInQueryWithNull() {
    // There is a NULL on the RHS, and '10 not in (20, null)' yields unknown
    // (similarly for every other value of deptno), so no rows are returned.
    final String sql = "select deptno from emp where deptno not in (\n"
        + "select deptno from emp)";
    withEmpDept(sql)
        .returnsCount(0);
  }

  @Test void testTrim() {
    CalciteAssert.model(FOODMART_MODEL)
        .query("select trim(\"lname\") as \"lname\" "
            + "from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=Nowmer\n");

    CalciteAssert.model(FOODMART_MODEL)
        .query("select trim(leading 'N' from \"lname\") as \"lname\" "
            + "from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=owmer\n");
  }

  private CalciteAssert.AssertQuery predicate(String foo) {
    return CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select * from \"hr\".\"emps\"\n"
            + "where " + foo)
        .runs();
  }

  @Test void testExistsCorrelated() {
    final String sql = "select*from \"hr\".\"emps\" where exists (\n"
        + " select 1 from \"hr\".\"depts\"\n"
        + " where \"emps\".\"deptno\"=\"depts\".\"deptno\")";
    final String plan = ""
        + "LogicalProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  LogicalFilter(condition=[EXISTS($cor0, {\n"
        + "LogicalFilter(condition=[=($cor0.deptno, $0)])\n"
        + "  LogicalTableScan(table=[[hr, depts]])\n"
        + "})], variablesSet=[[$cor0]])\n"
        + "    LogicalTableScan(table=[[hr, emps]])\n";
    CalciteAssert.hr().query(sql).convertContains(plan)
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250");
  }

  @Test void testNotExistsCorrelated() {
    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..5=[{inputs}], expr#6=[IS NULL($t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
        + "  EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n"
        + "    EnumerableAggregate(group=[{0}])\n"
        + "      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[true], expr#5=[$cor0], expr#6=[$t5.deptno], expr#7=[=($t6, $t0)], i=[$t4], $condition=[$t7])\n"
        + "        EnumerableTableScan(table=[[hr, depts]])\n";
    final String sql = "select * from \"hr\".\"emps\" where not exists (\n"
        + " select 1 from \"hr\".\"depts\"\n"
        + " where \"emps\".\"deptno\"=\"depts\".\"deptno\")";
    CalciteAssert.hr()
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .query(sql)
        .explainContains(plan)
        .returnsUnordered(
            "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500");
  }

  /** Manual expansion of EXISTS in {@link #testNotExistsCorrelated()}. */
  @Test void testNotExistsCorrelated2() {
    final String sql = "select * from \"hr\".\"emps\" as e left join lateral (\n"
        + " select distinct true as i\n"
        + " from \"hr\".\"depts\"\n"
        + " where e.\"deptno\"=\"depts\".\"deptno\") on true";
    final String explain = ""
        + "EnumerableCalc(expr#0..6=[{inputs}], proj#0..4=[{exprs}], I=[$t6])\n"
        + "  EnumerableHashJoin(condition=[=($1, $5)], joinType=[left])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n"
        + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[true], proj#0..1=[{exprs}])\n"
        + "      EnumerableAggregate(group=[{0}])\n"
        + "        EnumerableTableScan(table=[[hr, depts]])";
    CalciteAssert.hr()
        .query(sql)
        .explainContains(explain)
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; I=true",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; I=true",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null; I=true",
            "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500; I=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-313">[CALCITE-313]
   * Query decorrelation fails</a>. */
  @Test void testJoinInCorrelatedSubQuery() {
    CalciteAssert.hr()
        .query("select *\n"
            + "from \"hr\".\"depts\" as d\n"
            + "where \"deptno\" in (\n"
            + "  select d2.\"deptno\"\n"
            + "  from \"hr\".\"depts\" as d2\n"
            + "  join \"hr\".\"emps\" as e2 using (\"deptno\")\n"
            + "where d.\"deptno\" = d2.\"deptno\")")
        .convertMatches(relNode -> {

          String s = RelOptUtil.toString(relNode);
          assertThat(s, not(containsString("Correlate")));
          return null;

        });
  }

  /** Tests a correlated scalar sub-query in the SELECT clause.
   *
   * <p>Note that there should be an extra row "empid=200; deptno=20;
   * DNAME=null" but left join doesn't work.</p> */
  @Test void testScalarSubQuery() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      CalciteAssert.hr()
          .query("select \"empid\", \"deptno\",\n"
              + " (select \"name\" from \"hr\".\"depts\"\n"
              + "  where \"deptno\" = e.\"deptno\") as dname\n"
              + "from \"hr\".\"emps\" as e")
          .returnsUnordered("empid=100; deptno=10; DNAME=Sales",
              "empid=110; deptno=10; DNAME=Sales",
              "empid=150; deptno=10; DNAME=Sales",
              "empid=200; deptno=20; DNAME=null");
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-559">[CALCITE-559]
   * Correlated scalar sub-query in WHERE gives error</a>. */
  @Test void testJoinCorrelatedScalarSubQuery() throws SQLException {
    final String sql = "select e.employee_id, d.department_id "
        + " from employee e, department d "
        + " where e.department_id = d.department_id "
        + " and e.salary > (select avg(e2.salary) "
        + "                 from employee e2 "
        + "                 where e2.store_id = e.store_id)";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .with(Lex.JAVA)
        .query(sql)
        .returnsCount(599);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-685">[CALCITE-685]
   * Correlated scalar sub-query in SELECT clause throws</a>. */
  @Disabled("[CALCITE-685]")
  @Test void testCorrelatedScalarSubQuery() throws SQLException {
    final String sql = "select e.department_id, sum(e.employee_id),\n"
        + "       ( select sum(e2.employee_id)\n"
        + "         from  employee e2\n"
        + "         where e.department_id = e2.department_id\n"
        + "       )\n"
        + "from employee e\n"
        + "group by e.department_id\n";
    final String explain = "EnumerableHashJoin(condition=[true], joinType=[left])\n"
        + "  EnumerableAggregate(group=[{7}], EXPR$1=[$SUM0($0)])\n"
        + "    EnumerableTableScan(table=[[foodmart2, employee]])\n"
        + "  EnumerableAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "    EnumerableCalc(expr#0..16=[{inputs}], expr#17=[$cor0], expr#18=[$t17.department_id], expr#19=[=($t18, $t7)], employee_id=[$t0], department_id=[$t7], $condition=[$t19])\n"
        + "      EnumerableTableScan(table=[[foodmart2, employee]])\n";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .with(Lex.JAVA)
        .query(sql)
        .explainContains(explain)
        .returnsCount(0);
  }

  @Test void testLeftJoin() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  left join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=20; deptno=null");
  }

  @Test void testFullJoin() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  full join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=20; deptno=null",
            "deptno=null; deptno=30",
            "deptno=null; deptno=40");
  }

  @Test void testRightJoin() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  right join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=null; deptno=30",
            "deptno=null; deptno=40");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2464">[CALCITE-2464]
   * Allow to set nullability for columns of structured types</a>. */
  @Test public void testLeftJoinWhereStructIsNotNull() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  left join \"hr\".\"depts\" as d using (\"deptno\")"
            + "where d.\"location\" is not null")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10");
  }

  /** Various queries against EMP and DEPT, in particular involving composite
   * join conditions in various flavors of outer join. Results are verified
   * against MySQL (except full join, which MySQL does not support). */
  @Test void testVariousOuter() {
    final String sql =
        "select * from emp join dept on emp.deptno = dept.deptno";
    withEmpDept(sql).returnsUnordered(
        "ENAME=Alice; DEPTNO=30; GENDER=F; DEPTNO0=30; DNAME=Engineering",
        "ENAME=Bob  ; DEPTNO=10; GENDER=M; DEPTNO0=10; DNAME=Sales      ",
        "ENAME=Eric ; DEPTNO=20; GENDER=M; DEPTNO0=20; DNAME=Marketing  ",
        "ENAME=Jane ; DEPTNO=10; GENDER=F; DEPTNO0=10; DNAME=Sales      ",
        "ENAME=Susan; DEPTNO=30; GENDER=F; DEPTNO0=30; DNAME=Engineering");
  }

  private CalciteAssert.AssertQuery withEmpDept(String sql) {
    // Append a 'WITH' clause that supplies EMP and DEPT tables like this:
    //
    // drop table emp;
    // drop table dept;
    // create table emp(ename varchar(10), deptno int, gender varchar(1));
    // insert into emp values ('Jane', 10, 'F');
    // insert into emp values ('Bob', 10, 'M');
    // insert into emp values ('Eric', 20, 'M');
    // insert into emp values ('Susan', 30, 'F');
    // insert into emp values ('Alice', 30, 'F');
    // insert into emp values ('Adam', 50, 'M');
    // insert into emp values ('Eve', 50, 'F');
    // insert into emp values ('Grace', 60, 'F');
    // insert into emp values ('Wilma', null, 'F');
    // create table dept (deptno int, dname varchar(12));
    // insert into dept values (10, 'Sales');
    // insert into dept values (20, 'Marketing');
    // insert into dept values (30, 'Engineering');
    // insert into dept values (40, 'Empty');
    return CalciteAssert.that()
        .query("with\n"
            + "  emp(ename, deptno, gender) as (values\n"
            + "    ('Jane', 10, 'F'),\n"
            + "    ('Bob', 10, 'M'),\n"
            + "    ('Eric', 20, 'M'),\n"
            + "    ('Susan', 30, 'F'),\n"
            + "    ('Alice', 30, 'F'),\n"
            + "    ('Adam', 50, 'M'),\n"
            + "    ('Eve', 50, 'F'),\n"
            + "    ('Grace', 60, 'F'),\n"
            + "    ('Wilma', cast(null as integer), 'F')),\n"
            + "  dept(deptno, dname) as (values\n"
            + "    (10, 'Sales'),\n"
            + "    (20, 'Marketing'),\n"
            + "    (30, 'Engineering'),\n"
            + "    (40, 'Empty'))\n"
            + sql);
  }

  @Test void testScalarSubQueryUncorrelated() {
    CalciteAssert.hr()
        .query("select \"empid\", \"deptno\",\n"
            + " (select \"name\" from \"hr\".\"depts\"\n"
            + "  where \"deptno\" = 30) as dname\n"
            + "from \"hr\".\"emps\" as e")
        .returnsUnordered("empid=100; deptno=10; DNAME=Marketing",
            "empid=110; deptno=10; DNAME=Marketing",
            "empid=150; deptno=10; DNAME=Marketing",
            "empid=200; deptno=20; DNAME=Marketing");
  }

  @Test void testScalarSubQueryInCase() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      CalciteAssert.hr()
          .query("select e.\"name\",\n"
              + " (CASE e.\"deptno\"\n"
              + "  WHEN (Select \"deptno\" from \"hr\".\"depts\" d\n"
              + "        where d.\"deptno\" = e.\"deptno\")\n"
              + "  THEN (Select d.\"name\" from \"hr\".\"depts\" d\n"
              + "        where d.\"deptno\" = e.\"deptno\")\n"
              + "  ELSE 'DepartmentNotFound'  END) AS DEPTNAME\n"
              + "from \"hr\".\"emps\" e")
          .returnsUnordered("name=Bill; DEPTNAME=Sales",
              "name=Eric; DEPTNAME=DepartmentNotFound",
              "name=Sebastian; DEPTNAME=Sales",
              "name=Theodore; DEPTNAME=Sales");
    }
  }

  @Test void testScalarSubQueryInCase2() {
    CalciteAssert.hr()
        .query("select e.\"name\",\n"
            + " (CASE WHEN e.\"deptno\" = (\n"
            + "    Select \"deptno\" from \"hr\".\"depts\" d\n"
            + "    where d.\"name\" = 'Sales')\n"
            + "  THEN 'Sales'\n"
            + "  ELSE 'Not Matched'  END) AS DEPTNAME\n"
            + "from \"hr\".\"emps\" e")
        .returnsUnordered("name=Bill; DEPTNAME=Sales      ",
            "name=Eric; DEPTNAME=Not Matched",
            "name=Sebastian; DEPTNAME=Sales      ",
            "name=Theodore; DEPTNAME=Sales      ");
  }

  /** Tests the TABLES table in the information schema. */
  @Test void testMetaTables() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR_PLUS_METADATA)
        .query("select * from \"metadata\".TABLES")
        .returns(
            CalciteAssert.checkResultContains(
                "tableSchem=metadata; tableName=COLUMNS; tableType=SYSTEM TABLE; "));

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR_PLUS_METADATA)
        .query("select count(distinct \"tableSchem\") as c\n"
            + "from \"metadata\".TABLES")
        .returns("C=3\n");
  }

  /** Tests that {@link java.sql.Statement#setMaxRows(int)} is honored. */
  @Test void testSetMaxRows() throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {

          try {
            final Statement statement = connection.createStatement();
            try {
              statement.setMaxRows(-1);
              fail("expected error");
            } catch (SQLException e) {
              assertEquals(e.getMessage(), "illegal maxRows value: -1");
            }
            statement.setMaxRows(2);
            assertEquals(2, statement.getMaxRows());
            final ResultSet resultSet = statement.executeQuery(
                "select * from \"hr\".\"emps\"");
            assertTrue(resultSet.next());
            assertTrue(resultSet.next());
            assertFalse(resultSet.next());
            resultSet.close();
            statement.close();

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests a {@link PreparedStatement} with parameters. */
  @Test void testPreparedStatement() throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {

          try {
            final PreparedStatement preparedStatement =
                connection.prepareStatement("select \"deptno\", \"name\" "
                    + "from \"hr\".\"emps\"\n"
                    + "where \"deptno\" < ? and \"name\" like ?");

            // execute with vars unbound - gives error
            ResultSet resultSet;
            try {
              resultSet = preparedStatement.executeQuery();
              fail("expected error, got " + resultSet);
            } catch (SQLException e) {
              assertThat(e.getMessage(),
                  containsString(
                      "exception while executing query: unbound parameter"));
            }

            // execute with both vars null - no results
            preparedStatement.setNull(1, Types.INTEGER);
            preparedStatement.setNull(2, Types.VARCHAR);
            resultSet = preparedStatement.executeQuery();
            assertFalse(resultSet.next());

            // execute with ?0=15, ?1='%' - 3 rows
            preparedStatement.setInt(1, 15);
            preparedStatement.setString(2, "%");
            resultSet = preparedStatement.executeQuery();
            assertEquals("deptno=10; name=Bill\n"
                + "deptno=10; name=Sebastian\n"
                + "deptno=10; name=Theodore\n",
                CalciteAssert.toString(resultSet));

            // execute with ?0=15 (from last bind), ?1='%r%' - 1 row
            preparedStatement.setString(2, "%r%");
            resultSet = preparedStatement.executeQuery();
            assertEquals(
                "deptno=10; name=Theodore\n",
                CalciteAssert.toString(resultSet));

            // Now BETWEEN, with 3 arguments, 2 of which are parameters
            final String sql2 = "select \"deptno\", \"name\" "
                + "from \"hr\".\"emps\"\n"
                + "where \"deptno\" between symmetric ? and ?\n"
                + "order by 2";
            final PreparedStatement preparedStatement2 =
                connection.prepareStatement(sql2);
            preparedStatement2.setInt(1, 15);
            preparedStatement2.setInt(2, 5);
            resultSet = preparedStatement2.executeQuery();
            assertThat(CalciteAssert.toString(resultSet),
                is("deptno=10; name=Bill\n"
                    + "deptno=10; name=Sebastian\n"
                    + "deptno=10; name=Theodore\n"));

            resultSet.close();
            preparedStatement2.close();
            preparedStatement.close();

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2061">[CALCITE-2061]
   * Dynamic parameters in offset/fetch</a>. */
  @Test void testPreparedOffsetFetch() throws Exception {
    checkPreparedOffsetFetch(0, 0, Matchers.returnsUnordered());
    checkPreparedOffsetFetch(100, 4, Matchers.returnsUnordered());
    checkPreparedOffsetFetch(3, 4,
        Matchers.returnsUnordered("name=Eric"));
  }

  private void checkPreparedOffsetFetch(final int offset, final int fetch,
      final Matcher<? super ResultSet> matcher) throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {

          final String sql = "select \"name\"\n"
              + "from \"hr\".\"emps\"\n"
              + "order by \"empid\" offset ? fetch next ? rows only";
          try (PreparedStatement p =
                   connection.prepareStatement(sql)) {
            final ParameterMetaData pmd = p.getParameterMetaData();
            assertThat(pmd.getParameterCount(), is(2));
            assertThat(pmd.getParameterType(1), is(Types.INTEGER));
            assertThat(pmd.getParameterType(2), is(Types.INTEGER));
            p.setInt(1, offset);
            p.setInt(2, fetch);
            try (ResultSet r = p.executeQuery()) {
              assertThat(r, matcher);

            }
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests a JDBC connection that provides a model (a single schema based on
   * a JDBC database). */
  @Test void testModel() {
    CalciteAssert.model(FOODMART_MODEL)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Tests a JSON model with a comment. Not standard JSON, but harmless to
   * allow Jackson's comments extension.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-160">[CALCITE-160]
   * Allow comments in schema definitions</a>. */
  @Test void testModelWithComment() {
    final String model =
        FOODMART_MODEL.replace("schemas:", "/* comment */ schemas:");
    assertThat(model, not(equalTo(FOODMART_MODEL)));
    CalciteAssert.model(model)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Defines a materialized view and tests that the query is rewritten to use
   * it, and that the query produces the same result with and without it. There
   * are more comprehensive tests in {@link MaterializationTest}. */
  @Disabled("until JdbcSchema can define materialized views")
  @Test void testModelWithMaterializedView() {
    CalciteAssert.model(FOODMART_MODEL)
        .enable(false)
        .query(
            "select count(*) as c from \"foodmart\".\"sales_fact_1997\" join \"foodmart\".\"time_by_day\" using (\"time_id\")")
        .returns("C=86837\n");
    CalciteAssert.that().withMaterializations(
        FOODMART_MODEL,
        "agg_c_10_sales_fact_1997",
            "select t.`month_of_year`, t.`quarter`, t.`the_year`, sum(s.`store_sales`) as `store_sales`, sum(s.`store_cost`), sum(s.`unit_sales`), count(distinct s.`customer_id`), count(*) as `fact_count` from `time_by_day` as t join `sales_fact_1997` as s using (`time_id`) group by t.`month_of_year`, t.`quarter`, t.`the_year`")
        .query(
            "select t.\"month_of_year\", t.\"quarter\", t.\"the_year\", sum(s.\"store_sales\") as \"store_sales\", sum(s.\"store_cost\"), sum(s.\"unit_sales\"), count(distinct s.\"customer_id\"), count(*) as \"fact_count\" from \"time_by_day\" as t join \"sales_fact_1997\" as s using (\"time_id\") group by t.\"month_of_year\", t.\"quarter\", t.\"the_year\"")
        .explainContains(
            "JdbcTableScan(table=[[foodmart, agg_c_10_sales_fact_1997]])")
        .enableMaterializations(false)
        .explainContains("JdbcTableScan(table=[[foodmart, sales_fact_1997]])")
        .sameResultWithMaterializationsDisabled();
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test void testModelCustomTable() {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + EmpDeptTableFactory.class.getName() + "',\n"
        + "           operand: {'foo': 1, 'bar': [345, 357] }\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .query("select * from \"adhoc\".EMPLOYEES where \"deptno\" = 10")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test void testModelCustomTable2() {
    testRangeTable("object");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test void testModelCustomTableArrayRowSingleColumn() {
    testRangeTable("array");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test void testModelCustomTableIntegerRowSingleColumn() {
    testRangeTable("integer");
  }

  private void testRangeTable(String elementType) {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'MATH',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'INTEGERS',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + RangeTable.Factory.class.getName() + "',\n"
        + "           operand: {'column': 'N', 'start': 3, 'end': 7, "
        + " 'elementType': '" + elementType + "'}\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .query("select * from math.integers")
        .returns("N=3\n"
            + "N=4\n"
            + "N=5\n"
            + "N=6\n");
  }

  /** Tests a JDBC connection that provides a model that contains a custom
   * schema. */
  @Test void testModelCustomSchema() throws Exception {
    final CalciteAssert.AssertThat that =
        CalciteAssert.model("{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'adhoc',\n"
            + "  schemas: [\n"
            + "    {\n"
            + "      name: 'empty'\n"
            + "    },\n"
            + "    {\n"
            + "      name: 'adhoc',\n"
            + "      type: 'custom',\n"
            + "      factory: '"
            + MySchemaFactory.class.getName()
            + "',\n"
            + "      operand: {'tableName': 'ELVIS'}\n"
            + "    }\n"
            + "  ]\n"
            + "}");
    // check that the specified 'defaultSchema' was used
    that.doWithConnection(connection -> {

      try {
        assertEquals("adhoc", connection.getSchema());

      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }

    });
    that.query("select * from \"adhoc\".ELVIS where \"deptno\" = 10")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
    that.query("select * from \"adhoc\".EMPLOYEES")
        .throws_("Object 'EMPLOYEES' not found within 'adhoc'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1360">[CALCITE-1360]
   * Custom schema in file in current directory</a>. */
  @Test void testCustomSchemaInFileInPwd() throws SQLException {
    checkCustomSchemaInFileInPwd("custom-schema-model.json");
    switch (File.pathSeparatorChar) {
    case '/':
      // Skip this test on Windows; the mapping from file names to URLs is too
      // weird.
      checkCustomSchemaInFileInPwd("." + File.pathSeparatorChar
          + "custom-schema-model2.json");
    }
  }

  private void checkCustomSchemaInFileInPwd(String fileName)
      throws SQLException {
    final File file = new File(fileName);
    try (PrintWriter pw = Util.printWriter(file)) {
      file.deleteOnExit();
      pw.println("{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'adhoc',\n"
          + "  schemas: [\n"
          + "    {\n"
          + "      name: 'empty'\n"
          + "    },\n"
          + "    {\n"
          + "      name: 'adhoc',\n"
          + "      type: 'custom',\n"
          + "      factory: '"
          + MySchemaFactory.class.getName()
          + "',\n"
          + "      operand: {'tableName': 'ELVIS'}\n"
          + "    }\n"
          + "  ]\n"
          + "}");
      pw.flush();
      final String url = "jdbc:calcite:model=" + file;
      try (Connection c = DriverManager.getConnection(url);
           Statement s = c.createStatement();
           ResultSet r = s.executeQuery("values 1")) {
        assertThat(r.next(), is(true));
      }
      //noinspection ResultOfMethodCallIgnored
      file.delete();
    } catch (IOException e) {
      // current directory is not writable; an environment issue, not
      // necessarily a bug
    }
  }

  /** Connects to a custom schema without writing a model.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1259">[CALCITE-1259]
   * Allow connecting to a single schema without writing a model</a>. */
  @Test void testCustomSchemaDirectConnection() throws Exception {
    final String url = "jdbc:calcite:"
        + "schemaFactory=" + MySchemaFactory.class.getName()
        + "; schema.tableName=ELVIS";
    checkCustomSchema(url, "adhoc"); // implicit schema is called 'adhoc'
    checkCustomSchema(url + "; schema=xyz", "xyz"); // explicit schema
  }

  private void checkCustomSchema(String url, String schemaName) throws SQLException {
    try (Connection connection = DriverManager.getConnection(url)) {
      assertThat(connection.getSchema(), is(schemaName));
      final String sql = "select * from \"" + schemaName + "\".ELVIS where \"deptno\" = 10";
      final String sql2 = "select * from ELVIS where \"deptno\" = 10";
      String expected = ""
          + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
          + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
          + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n";
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          assertThat(CalciteAssert.toString(resultSet), is(expected));
        }
        try (ResultSet resultSet = statement.executeQuery(sql2)) {
          assertThat(CalciteAssert.toString(resultSet), is(expected));
        }
      }
    }
  }

  /** Connects to a JDBC schema without writing a model. */
  @Test void testJdbcSchemaDirectConnection() throws Exception {
    checkJdbcSchemaDirectConnection(
        "schemaFactory=org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
    checkJdbcSchemaDirectConnection("schemaType=JDBC");
  }

  private void checkJdbcSchemaDirectConnection(String s) throws SQLException {
    final StringBuilder b = new StringBuilder("jdbc:calcite:");
    b.append(s);
    pv(b, "schema.jdbcUser", SCOTT.username);
    pv(b, "schema.jdbcPassword", SCOTT.password);
    pv(b, "schema.jdbcUrl", SCOTT.url);
    pv(b, "schema.jdbcCatalog", SCOTT.catalog);
    pv(b, "schema.jdbcDriver", SCOTT.driver);
    pv(b, "schema.jdbcSchema", SCOTT.schema);
    final String url =  b.toString();
    Connection connection = DriverManager.getConnection(url);
    assertThat(connection.getSchema(), is("adhoc"));
    String expected = "C=14\n";
    final String sql = "select count(*) as c from emp";
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(CalciteAssert.toString(resultSet), is(expected));
    }
  }

  private void pv(StringBuilder b, String p, String v) {
    if (v != null) {
      b.append("; ").append(p).append("=").append(v);
    }
  }

  /** Connects to a map schema without writing a model. */
  @Test void testMapSchemaDirectConnection() throws Exception {
    checkMapSchemaDirectConnection("schemaType=MAP");
    checkMapSchemaDirectConnection(
        "schemaFactory=org.apache.calcite.schema.impl.AbstractSchema$Factory");
  }

  private void checkMapSchemaDirectConnection(String s) throws SQLException {
    final String url = "jdbc:calcite:" + s;
    Connection connection = DriverManager.getConnection(url);
    assertThat(connection.getSchema(), is("adhoc"));
    String expected = "EXPR$0=1\n";
    final String sql = "values 1";
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(CalciteAssert.toString(resultSet), is(expected));
    }
  }

  /** Tests that an immutable schema in a model cannot contain a view. */
  @Test void testModelImmutableSchemaCannotContainView()
      throws Exception {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'empty'\n"
        + "    },\n"
        + "    {\n"
        + "      name: 'adhoc',\n"
        + "      type: 'custom',\n"
        + "      tables: [\n"
        + "        {\n"
        + "          name: 'v',\n"
        + "          type: 'view',\n"
        + "          sql: 'values (1)'\n"
        + "        }\n"
        + "      ],\n"
        + "      factory: '"
        + MySchemaFactory.class.getName()
        + "',\n"
        + "      operand: {\n"
        + "           'tableName': 'ELVIS',\n"
        + "           'mutable': false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}")
        .connectThrows(
            "Cannot define view; parent schema 'adhoc' is not mutable");
  }

  private CalciteAssert.AssertThat modelWithView(String view,
      Boolean modifiable) {
    final Class<EmpDeptTableFactory> clazz = EmpDeptTableFactory.class;
    return CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + clazz.getName() + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MUTABLE_EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + clazz.getName() + "',\n"
        + "           operand: {'foo': false}\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'V',\n"
        + "           type: 'view',\n"
        + (modifiable == null ? "" : " modifiable: " + modifiable + ",\n")
        + "           sql: " + new JsonBuilder().toJsonString(view) + "\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}");
  }

  /** Tests a JDBC connection that provides a model that contains a view. */
  @Test void testModelView() throws Exception {
    final CalciteAssert.AssertThat with =
        modelWithView("select * from \"EMPLOYEES\" where \"deptno\" = 10",
            null);

    with.query("select * from \"adhoc\".V order by \"name\" desc")
        .returns(""
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");

    // Make sure that views appear in metadata.
    with.doWithConnection(connection -> {

      try {
        final DatabaseMetaData metaData = connection.getMetaData();

        // all table types
        try (ResultSet r =
                 metaData.getTables(null, "adhoc", null, null)) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=MUTABLE_EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
              CalciteAssert.toString(r));
        }

        // including system tables; note that table type is "SYSTEM TABLE"
        // not "SYSTEM_TABLE"
        try (ResultSet r = metaData.getTables(null, null, null, null)) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=MUTABLE_EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=metadata; TABLE_NAME=COLUMNS; TABLE_TYPE=SYSTEM TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=metadata; TABLE_NAME=TABLES; TABLE_TYPE=SYSTEM TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
              CalciteAssert.toString(r));
        }

        // views only
        try (ResultSet r = metaData.getTables(null, "adhoc", null,
            new String[]{Schema.TableType.VIEW.jdbcName})) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
              CalciteAssert.toString(r));
        }

        // columns
        try (ResultSet r =
                 metaData.getColumns(null, "adhoc", "V", null)) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=empid; DATA_TYPE=4; TYPE_NAME=JavaType(int) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=1; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=deptno; DATA_TYPE=4; TYPE_NAME=JavaType(int) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=2; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=name; DATA_TYPE=12; TYPE_NAME=JavaType(class java.lang.String); COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=1; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=3; IS_NULLABLE=YES; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=salary; DATA_TYPE=7; TYPE_NAME=JavaType(float) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=4; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=commission; DATA_TYPE=4; TYPE_NAME=JavaType(class java.lang.Integer); COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=1; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=5; IS_NULLABLE=YES; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n",
              CalciteAssert.toString(r));
        }

        // catalog
        try (ResultSet r = metaData.getCatalogs()) {
          assertEquals(
              "TABLE_CAT=null\n",
              CalciteAssert.toString(r));
        }

        // schemas
        try (ResultSet r = metaData.getSchemas()) {
          assertEquals(
              "TABLE_SCHEM=adhoc; TABLE_CATALOG=null\n"
                  + "TABLE_SCHEM=metadata; TABLE_CATALOG=null\n",
              CalciteAssert.toString(r));
        }

        // schemas (qualified)
        try (ResultSet r = metaData.getSchemas(null, "adhoc")) {
          assertEquals(
              "TABLE_SCHEM=adhoc; TABLE_CATALOG=null\n",
              CalciteAssert.toString(r));
        }

        // table types
        try (ResultSet r = metaData.getTableTypes()) {
          assertEquals("TABLE_TYPE=TABLE\n"
                  + "TABLE_TYPE=VIEW\n",
              CalciteAssert.toString(r));
        }

      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }

    });
  }

  /** Tests a view with ORDER BY and LIMIT clauses. */
  @Test void testOrderByView() throws Exception {
    final CalciteAssert.AssertThat with =
        modelWithView("select * from \"EMPLOYEES\" where \"deptno\" = 10 "
            + "order by \"empid\" limit 2", null);
    with
        .query("select \"name\" from \"adhoc\".V order by \"name\"")
        .returns("name=Bill\n"
            + "name=Theodore\n");

    // Now a sub-query with ORDER BY and LIMIT clauses. (Same net effect, but
    // ORDER BY and LIMIT in sub-query were not standard SQL until SQL:2008.)
    with
        .query("select \"name\" from (\n"
            + "select * from \"adhoc\".\"EMPLOYEES\" where \"deptno\" = 10\n"
            + "order by \"empid\" limit 2)\n"
            + "order by \"name\"")
        .returns("name=Bill\n"
            + "name=Theodore\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1900">[CALCITE-1900]
   * Improve error message for cyclic views</a>.
   * Previously got a {@link StackOverflowError}. */
  @Test void testSelfReferentialView() throws Exception {
    final CalciteAssert.AssertThat with =
        modelWithView("select * from \"V\"", null);
    with.query("select \"name\" from \"adhoc\".V")
        .throws_("Cannot resolve 'adhoc.V'; it references view 'adhoc.V', "
            + "whose definition is cyclic");
  }

  @Test void testSelfReferentialView2() throws Exception {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "    name: 'adhoc',\n"
        + "    tables: [ {\n"
        + "      name: 'A',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString("select * from B") + "\n"
        + "    }, {\n"
        + "      name: 'B',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString("select * from C") + "\n"
        + "    }, {\n"
        + "      name: 'C',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString("select * from D, B") + "\n"
        + "    }, {\n"
        + "      name: 'D',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString(
            "select * from (values (1, 'a')) as t(x, y)") + "\n"
        + "    } ]\n"
        + "  } ]\n"
        + "}";
    final CalciteAssert.AssertThat with =
        CalciteAssert.model(model);
    //
    //       +-----+
    //       V     |
    // A --> B --> C --> D
    //
    // A is not in a cycle, but depends on cyclic views
    // B is cyclic
    // C is cyclic
    // D is not cyclic
    with.query("select x from \"adhoc\".a")
        .throws_("Cannot resolve 'adhoc.A'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
    with.query("select x from \"adhoc\".b")
        .throws_("Cannot resolve 'adhoc.B'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
    // as previous, but implicit schema
    with.query("select x from b")
        .throws_("Cannot resolve 'B'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
    with.query("select x from \"adhoc\".c")
        .throws_("Cannot resolve 'adhoc.C'; it references view 'adhoc.C', "
            + "whose definition is cyclic");
    with.query("select x from \"adhoc\".d")
        .returns("X=1\n");
    with.query("select x from \"adhoc\".d except select x from \"adhoc\".a")
        .throws_("Cannot resolve 'adhoc.A'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
  }

  /** Tests saving query results into temporary tables, per
   * {@link org.apache.calcite.avatica.Handler.ResultSink}. */
  @Test void testAutomaticTemporaryTable() throws Exception {
    final List<Object> objects = new ArrayList<>();
    CalciteAssert.that()
        .with(
            new CalciteAssert.ConnectionFactory() {
              public CalciteConnection createConnection() throws SQLException {
                CalciteConnection connection = (CalciteConnection)
                    new AutoTempDriver(objects)
                        .connect("jdbc:calcite:", new Properties());
                final SchemaPlus rootSchema = connection.getRootSchema();
                rootSchema.add("hr",
                    new ReflectiveSchema(new HrSchema()));
                connection.setSchema("hr");
                return connection;
              }
            })
        .doWithConnection(connection -> {

          try {
            final String sql = "select * from \"hr\".\"emps\" "
                + "where \"deptno\" = 10";
            connection.createStatement()
                .executeQuery(sql);
            assertThat(objects.size(), is(1));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  @Test void testExplain() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with(CalciteAssert.Config.FOODMART_CLONE);
    with.query("explain plan for values (1, 'ab')")
        .returns("PLAN=EnumerableValues(tuples=[[{ 1, 'ab' }]])\n\n");
    final String expectedXml = "PLAN=<RelNode type=\"EnumerableValues\">\n"
        + "\t<Property name=\"tuples\">\n"
        + "\t\t[{ 1, &#39;ab&#39; }]\n"
        + "\t</Property>\n"
        + "\t<Inputs/>\n"
        + "</RelNode>\n"
        + "\n";
    with.query("explain plan as xml for values (1, 'ab')")
        .returns(expectedXml);
    final String expectedJson = "PLAN={\n"
        + "  \"rels\": [\n"
        + "    {\n"
        + "      \"id\": \"0\",\n"
        + "      \"relOp\": \"org.apache.calcite.adapter.enumerable.EnumerableValues\",\n"
        + "      \"type\": [\n"
        + "        {\n"
        + "          \"type\": \"INTEGER\",\n"
        + "          \"nullable\": false,\n"
        + "          \"name\": \"EXPR$0\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"type\": \"CHAR\",\n"
        + "          \"nullable\": false,\n"
        + "          \"precision\": 2,\n"
        + "          \"name\": \"EXPR$1\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"type\": \"TIMESTAMP\",\n"
        + "          \"nullable\": false,\n"
        + "          \"precision\": 0,\n"
        + "          \"name\": \"EXPR$2\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"type\": \"DECIMAL\",\n"
        + "          \"nullable\": false,\n"
        + "          \"precision\": 3,\n"
        + "          \"scale\": 2,\n"
        + "          \"name\": \"EXPR$3\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"tuples\": [\n"
        + "        [\n"
        + "          {\n"
        + "            \"literal\": 1,\n"
        + "            \"type\": {\n"
        + "              \"type\": \"INTEGER\",\n"
        + "              \"nullable\": false\n"
        + "            }\n"
        + "          },\n"
        + "          {\n"
        + "            \"literal\": \"ab\",\n"
        + "            \"type\": {\n"
        + "              \"type\": \"CHAR\",\n"
        + "              \"nullable\": false,\n"
        + "              \"precision\": 2\n"
        + "            }\n"
        + "          },\n"
        + "          {\n"
        + "            \"literal\": 1364860800000,\n"
        + "            \"type\": {\n"
        + "              \"type\": \"TIMESTAMP\",\n"
        + "              \"nullable\": false,\n"
        + "              \"precision\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          {\n"
        + "            \"literal\": 0.01,\n"
        + "            \"type\": {\n"
        + "              \"type\": \"DECIMAL\",\n"
        + "              \"nullable\": false,\n"
        + "              \"precision\": 3,\n"
        + "              \"scale\": 2\n"
        + "            }\n"
        + "          }\n"
        + "        ]\n"
        + "      ],\n"
        + "      \"inputs\": []\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
    with.query("explain plan as json for values (1, 'ab', TIMESTAMP '2013-04-02 00:00:00', 0.01)")
        .returns(expectedJson);
    with.query("explain plan with implementation for values (1, 'ab')")
        .returns("PLAN=EnumerableValues(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan without implementation for values (1, 'ab')")
        .returns("PLAN=LogicalValues(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan with type for values (1, 'ab')")
        .returns("PLAN=EXPR$0 INTEGER NOT NULL,\n"
            + "EXPR$1 CHAR(2) NOT NULL\n");
  }

  /** Test case for bug where if two tables have different element classes
   * but those classes have identical fields, Calcite would generate code to use
   * the wrong element class; a {@link ClassCastException} would ensue. */
  @Test void testDifferentTypesSameFields() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("TEST", new ReflectiveSchema(new MySchema()));
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT \"myvalue\" from TEST.\"mytable2\"");
    assertEquals("myvalue=2\n", CalciteAssert.toString(resultSet));
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Tests that CURRENT_TIMESTAMP gives different values each time a statement
   * is executed. */
  @Test void testCurrentTimestamp() throws Exception {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.TIME_ZONE, "GMT+1:00")
        .doWithConnection(
            new Function<CalciteConnection, Void>() {
              public Void apply(CalciteConnection connection) {
                try {
                  final PreparedStatement statement =
                      connection.prepareStatement("VALUES CURRENT_TIMESTAMP");
                  ResultSet resultSet;

          try {
            final PreparedStatement statement =
                connection.prepareStatement("VALUES CURRENT_TIMESTAMP");
            ResultSet resultSet;

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            String s0 = resultSet.getString(1);
            assertFalse(resultSet.next());

            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              throw TestUtil.rethrow(e);
            }

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            String s1 = resultSet.getString(1);
            assertFalse(resultSet.next());

            assertThat(s0, ComparatorMatcherBuilder.<String>usingNaturalOrdering().lessThan(s1));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Test for timestamps and time zones, based on pgsql TimezoneTest. */
  @Test void testGetTimestamp() throws Exception {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.TIME_ZONE, "GMT+1:00")
        .doWithConnection(
            new Function<CalciteConnection, Void>() {
              public Void apply(CalciteConnection connection) {
                try {
                  checkGetTimestamp(connection);
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  private void checkGetTimestamp(Connection con) throws SQLException {
    Statement statement = con.createStatement();

    // Not supported yet. We set timezone using connect-string parameters.
    //statement.executeUpdate("alter session set timezone = 'gmt-3'");

    ResultSet rs = statement.executeQuery("SELECT * FROM (VALUES(\n"
        + " TIMESTAMP '1970-01-01 00:00:00',\n"
        + " /* TIMESTAMP '2005-01-01 15:00:00 +0300', */\n"
        + " TIMESTAMP '2005-01-01 15:00:00',\n"
        + " TIME '15:00:00',\n"
        + " /* TIME '15:00:00 +0300', */\n"
        + " DATE '2005-01-01'\n"
        + ")) AS t(ts0, /* tstz, */ ts, t, /* tz, */ d)");
    assertTrue(rs.next());

    TimeZone tzUtc   = TimeZone.getTimeZone("UTC");    // +0000 always
    TimeZone tzGmt03 = TimeZone.getTimeZone("GMT+03"); // +0300 always
    TimeZone tzGmt05 = TimeZone.getTimeZone("GMT-05"); // -0500 always
    TimeZone tzGmt13 = TimeZone.getTimeZone("GMT+13"); // +1000 always

    Calendar cUtc   = Calendar.getInstance(tzUtc, Locale.ROOT);
    Calendar cGmt03 = Calendar.getInstance(tzGmt03, Locale.ROOT);
    Calendar cGmt05 = Calendar.getInstance(tzGmt05, Locale.ROOT);
    Calendar cGmt13 = Calendar.getInstance(tzGmt13, Locale.ROOT);

    Timestamp ts;
    String s;
    int c = 1;

    // timestamp: 1970-01-01 00:00:00
    ts = rs.getTimestamp(c);                     // Convert timestamp to +0100
    assertEquals(-3600000L,      ts.getTime());  // 1970-01-01 00:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);               // Convert timestamp to UTC
    assertEquals(0L,             ts.getTime());  // 1970-01-01 00:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);             // Convert timestamp to +0300
    assertEquals(-10800000L,     ts.getTime());  // 1970-01-01 00:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);             // Convert timestamp to -0500
    assertEquals(18000000L,      ts.getTime());  // 1970-01-01 00:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);             // Convert timestamp to +1300
    assertEquals(-46800000,      ts.getTime());  // 1970-01-01 00:00:00 +1300
    s = rs.getString(c);
    assertEquals("1970-01-01 00:00:00", s);
    ++c;

    if (false) {
      // timestamptz: 2005-01-01 15:00:00+03
      ts = rs.getTimestamp(c);                      // Represents an instant in
                                                    // time, TZ is irrelevant.
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cUtc);                // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt03);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt05);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt13);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ++c;
    }

    // timestamp: 2005-01-01 15:00:00
    ts = rs.getTimestamp(c);                     // Convert timestamp to +0100
    assertEquals(1104588000000L, ts.getTime());  // 2005-01-01 15:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);               // Convert timestamp to UTC
    assertEquals(1104591600000L, ts.getTime());  // 2005-01-01 15:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);             // Convert timestamp to +0300
    assertEquals(1104580800000L, ts.getTime());  // 2005-01-01 15:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);             // Convert timestamp to -0500
    assertEquals(1104609600000L, ts.getTime());  // 2005-01-01 15:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);             // Convert timestamp to +1300
    assertEquals(1104544800000L, ts.getTime());  // 2005-01-01 15:00:00 +1300
    s = rs.getString(c);
    assertEquals("2005-01-01 15:00:00", s);
    ++c;

    // time: 15:00:00
    ts = rs.getTimestamp(c);
    assertEquals(50400000L, ts.getTime());        // 1970-01-01 15:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);
    assertEquals(54000000L, ts.getTime());        // 1970-01-01 15:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);
    assertEquals(43200000L, ts.getTime());        // 1970-01-01 15:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);
    assertEquals(72000000L, ts.getTime());        // 1970-01-01 15:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);
    assertEquals(7200000L, ts.getTime());         // 1970-01-01 15:00:00 +1300
    s = rs.getString(c);
    assertEquals("15:00:00", s);
    ++c;

    if (false) {
      // timetz: 15:00:00+03
      ts = rs.getTimestamp(c);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
      // 1970-01-01 13:00:00 +0100
      ts = rs.getTimestamp(c, cUtc);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
      // 1970-01-01 12:00:00 +0000
      ts = rs.getTimestamp(c, cGmt03);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
      // 1970-01-01 15:00:00 +0300
      ts = rs.getTimestamp(c, cGmt05);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
      // 1970-01-01 07:00:00 -0500
      ts = rs.getTimestamp(c, cGmt13);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
      // 1970-01-02 01:00:00 +1300
      ++c;
    }

    // date: 2005-01-01
    ts = rs.getTimestamp(c);
    assertEquals(1104534000000L, ts.getTime()); // 2005-01-01 00:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);
    assertEquals(1104537600000L, ts.getTime()); // 2005-01-01 00:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);
    assertEquals(1104526800000L, ts.getTime()); // 2005-01-01 00:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);
    assertEquals(1104555600000L, ts.getTime()); // 2005-01-01 00:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);
    assertEquals(1104490800000L, ts.getTime()); // 2005-01-01 00:00:00 +1300
    s = rs.getString(c);
    assertEquals("2005-01-01", s);              // 2005-01-01 00:00:00 +0100
    ++c;

    assertTrue(!rs.next());
  }

  /** Test for MONTHNAME, DAYNAME and DAYOFWEEK functions in two locales. */
  @Test void testMonthName() {
    final String sql = "SELECT * FROM (VALUES(\n"
        + " monthname(TIMESTAMP '1969-01-01 00:00:00'),\n"
        + " monthname(DATE '1969-01-01'),\n"
        + " monthname(DATE '2019-02-10'),\n"
        + " monthname(TIMESTAMP '2019-02-10 02:10:12'),\n"
        + " dayname(TIMESTAMP '1969-01-01 00:00:00'),\n"
        + " dayname(DATE '1969-01-01'),\n"
        + " dayname(DATE '2019-02-10'),\n"
        + " dayname(TIMESTAMP '2019-02-10 02:10:12'),\n"
        + " dayofweek(DATE '2019-02-09'),\n" // sat=7
        + " dayofweek(DATE '2019-02-10'),\n" // sun=1
        + " extract(DOW FROM DATE '2019-02-09'),\n" // sat=7
        + " extract(DOW FROM DATE '2019-02-10'),\n" // sun=1
        + " extract(ISODOW FROM DATE '2019-02-09'),\n" // sat=6
        + " extract(ISODOW FROM DATE '2019-02-10')\n" // sun=7
        + ")) AS t(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13)";
    Stream.of(TestLocale.values()).forEach(t -> {
      try {
        CalciteAssert.that()
            .with(CalciteConnectionProperty.LOCALE, t.localeName)
            .with(CalciteConnectionProperty.FUN, "mysql")
            .doWithConnection(connection -> {
              try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery(sql)) {
                  assertThat(rs.next(), is(true));
                  assertThat(rs.getString(1), is(t.january));
                  assertThat(rs.getString(2), is(t.january));
                  assertThat(rs.getString(3), is(t.february));
                  assertThat(rs.getString(4), is(t.february));
                  assertThat(rs.getString(5), is(t.wednesday));
                  assertThat(rs.getString(6), is(t.wednesday));
                  assertThat(rs.getString(7), is(t.sunday));
                  assertThat(rs.getString(8), is(t.sunday));
                  assertThat(rs.getInt(9), is(7));
                  assertThat(rs.getInt(10), is(1));
                  assertThat(rs.getInt(11), is(7));
                  assertThat(rs.getInt(12), is(1));
                  assertThat(rs.getInt(13), is(6));
                  assertThat(rs.getInt(14), is(7));
                  assertThat(rs.next(), is(false));
                }
              } catch (SQLException e) {
                throw TestUtil.rethrow(e);
              }
            });
      } catch (Exception e) {
        System.out.println(t.localeName + ":" + Locale.getDefault().toString());
        throw TestUtil.rethrow(e);
      }
    });
  }

  /** Tests accessing a column in a JDBC source whose type is DATE. */
  @Test void testGetDate() throws Exception {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {

          try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select min(\"date\") mindate from \"foodmart\".\"currency\"");
            assertTrue(rs.next());
            assertEquals(
                Date.valueOf("1997-01-01"),
                rs.getDate(1));
            assertFalse(rs.next());

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests accessing a date as a string in a JDBC source whose type is DATE. */
  @Test void testGetDateAsString() throws Exception {
    CalciteAssert.that()
      .with(CalciteAssert.Config.JDBC_FOODMART)
      .query("select min(\"date\") mindate from \"foodmart\".\"currency\"")
      .returns2("MINDATE=1997-01-01\n");
  }

  @Test void testGetTimestampObject() throws Exception {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {

          try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select \"hire_date\" from \"foodmart\".\"employee\" where \"employee_id\" = 1");
            assertTrue(rs.next());
            assertEquals(
                Timestamp.valueOf("1994-12-01 00:00:00"),
                rs.getTimestamp(1));
            assertFalse(rs.next());

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  @Test void testRowComparison() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_SCOTT)
        .query("SELECT empno FROM JDBC_SCOTT.emp WHERE (ename, job) < ('Blake', 'Manager')")
        .returnsUnordered("EMPNO=7876", "EMPNO=7499", "EMPNO=7698");
  }

  @Test void testTimestampEqualsComparison() {
    CalciteAssert.that()
        .query("select time0 = time1, time0 <> time1"
            + " from ("
            + "  select timestamp'2000-12-30 21:07:32'as time0,"
            + "         timestamp'2000-12-30 21:07:32'as time1 "
            + "  union all"
            + "  select cast(null as timestamp) as time0,"
            + "         cast(null as timestamp) as time1"
            + ") calcs")
        .planContains("org.apache.calcite.runtime.SqlFunctions.eq(inp0_, inp1_)")
        .planContains("org.apache.calcite.runtime.SqlFunctions.ne(inp0_, inp1_)")
        .returns("EXPR$0=true; EXPR$1=false\n"
            + "EXPR$0=null; EXPR$1=null\n");
  }

  @Test void testUnicode() throws Exception {
    CalciteAssert.AssertThat with =
        CalciteAssert.that().with(CalciteAssert.Config.FOODMART_CLONE);

    // Note that \u82f1 in a Java string is a Java unicode escape;
    // But \\82f1 in a SQL string is a SQL unicode escape.
    // various ways to create a unicode string literal
    with.query("values _UTF16'\u82f1\u56fd'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values U&'\\82F1\\56FD'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values u&'\\82f1\\56fd'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values '\u82f1\u56fd'")
        .throws_(
            "Failed to encode '\u82f1\u56fd' in character set 'ISO-8859-1'");
    with.query("values _UTF8'\u82f1\u56fd'")
        .returns("EXPR$0=英国\n");
    with.query("values _LATIN1'\u82f1\u56fd'")
        .throws_("Failed to encode '英国' in character set 'ISO-8859-1'");

    // comparing a unicode string literal with a regular string literal
    with.query(
        "select * from \"employee\" where \"full_name\" = '\u82f1\u56fd'")
        .throws_("Failed to encode '\u82f1\u56fd' in character set 'ISO-8859-1'");
    with.query(
        "select * from \"employee\" where \"full_name\" = _UTF16'\u82f1\u56fd'")
        .throws_(
            "Cannot apply = to the two different charsets ISO-8859-1 and UTF-16LE");

    // The CONVERT function (what SQL:2011 calls "character transliteration") is
    // not implemented yet. See
    // https://issues.apache.org/jira/browse/CALCITE-111.
    with.query("select * from \"employee\"\n"
        + "where convert(\"full_name\" using UTF16) = _UTF_16'\u82f1\u56fd'")
        .throws_("Column 'UTF16' not found in any table");
  }

  /** Tests metadata for the MySQL lexical scheme. */
  @Test void testLexMySQL() throws Exception {
    CalciteAssert.that()
        .with(Lex.MYSQL)
        .doWithConnection(connection -> {

          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(), equalTo("`"));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests metadata for the MySQL ANSI lexical scheme. */
  @Test void testLexMySQLANSI() throws Exception {
    CalciteAssert.that()
        .with(Lex.MYSQL_ANSI)
        .doWithConnection(connection -> {

          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(), equalTo("\""));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests metadata for different the "SQL_SERVER" lexical scheme. */
  @Test void testLexSqlServer() throws Exception {
    CalciteAssert.that()
        .with(Lex.SQL_SERVER)
        .doWithConnection(connection -> {

          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(), equalTo("["));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests metadata for the ORACLE (and default) lexical scheme. */
  @Test void testLexOracle() throws Exception {
    CalciteAssert.that()
        .with(Lex.ORACLE)
        .doWithConnection(connection -> {

          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(),
                equalTo("\""));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(true));
            // Oracle JDBC 12.1.0.1.0 returns true here, however it is
            // not clear if the bug is in JDBC specification or Oracle
            // driver
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests metadata for the JAVA lexical scheme. */
  @Test void testLexJava() throws Exception {
    CalciteAssert.that()
        .with(Lex.JAVA)
        .doWithConnection(connection -> {

          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(),
                equalTo("`"));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Tests metadata for the ORACLE lexical scheme overridden like JAVA. */
  @Test void testLexOracleAsJava() throws Exception {
    CalciteAssert.that()
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.QUOTING, Quoting.BACK_TICK)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.UNCHANGED)
        .with(CalciteConnectionProperty.QUOTED_CASING, Casing.UNCHANGED)
        .with(CalciteConnectionProperty.CASE_SENSITIVE, true)
        .doWithConnection(
            new Function<CalciteConnection, Void>() {
              public Void apply(CalciteConnection connection) {
                try {
                  DatabaseMetaData metaData = connection.getMetaData();
                  assertThat(metaData.getIdentifierQuoteString(),
                      equalTo("`"));
                  assertThat(metaData.supportsMixedCaseIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesMixedCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                      equalTo(true));
                  assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                      equalTo(false));
                  assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                      equalTo(false));
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Tests case-insensitive resolution of schema and table names. */
  @Test void testLexCaseInsensitive() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with(Lex.MYSQL);
    with.query("select COUNT(*) as c from metaData.tAbles")
        .returns("c=2\n");
    with.query("select COUNT(*) as c from `metaData`.`tAbles`")
        .returns("c=2\n");

    // case-sensitive gives error
    final CalciteAssert.AssertThat with2 =
        CalciteAssert.that().with(Lex.JAVA);
    with2.query("select COUNT(*) as c from `metaData`.`tAbles`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metaData`.`TABLES`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metaData`.`tables`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metaData`.`nonExistent`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metadata`.`tAbles`")
        .throws_("Object 'tAbles' not found within 'metadata'; did you mean 'TABLES'?");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1563">[CALCITE-1563]
   * In case-insensitive connection, non-existent tables use alphabetically
   * preceding table</a>. */
  @Test void testLexCaseInsensitiveFindsNonexistentTable() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with(Lex.MYSQL);
    // With [CALCITE-1563], the following query succeeded; it queried
    // metadata.tables.
    with.query("select COUNT(*) as c from `metaData`.`zoo`")
        .throws_("Object 'zoo' not found within 'metadata'");
    with.query("select COUNT(*) as c from `metaData`.`tAbLes`")
        .returns("c=2\n");
  }

  /** Tests case-insensitive resolution of sub-query columns.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-550">[CALCITE-550]
   * Case-insensitive matching of sub-query columns fails</a>. */
  @Test void testLexCaseInsensitiveSubQueryField() {
    CalciteAssert.that()
        .with(Lex.MYSQL)
        .query("select DID\n"
            + "from (select deptid as did\n"
            + "         FROM\n"
            + "            ( values (1), (2) ) as T1(deptid)\n"
            + "         ) ")
        .returnsUnordered("DID=1", "DID=2");
  }

  @Test void testLexCaseInsensitiveTableAlias() {
    CalciteAssert.that()
        .with(Lex.MYSQL)
        .query("select e.empno\n"
            + "from (values (1, 2)) as E (empno, deptno),\n"
            + "  (values (3, 4)) as d (deptno, name)")
        .returnsUnordered("empno=1");
  }

  @Test void testFunOracle() {
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .with(CalciteConnectionProperty.FUN, "oracle")
        .query("select nvl(\"commission\", -99) as c from \"hr\".\"emps\"")
        .returnsUnordered("C=-99",
            "C=1000",
            "C=250",
            "C=500");

    // NVL is not present in the default operator table
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .query("select nvl(\"commission\", -99) as c from \"hr\".\"emps\"")
        .throws_("No match found for function signature NVL(<NUMERIC>, <NUMERIC>)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2072">[CALCITE-2072]
   * Enable spatial operator table by adding 'fun=spatial'to JDBC URL</a>. */
  @Test void testFunSpatial() {
    final String sql = "select distinct\n"
        + "  ST_PointFromText('POINT(-71.0642.28)') as c\n"
        + "from \"hr\".\"emps\"";
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .with(CalciteConnectionProperty.FUN, "spatial")
        .query(sql)
        .returnsUnordered("C={\"x\":-71.0642,\"y\":0.28}");

    // NVL is present in the Oracle operator table, but not spatial or core
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .query("select nvl(\"commission\", -99) as c from \"hr\".\"emps\"")
        .throws_("No match found for function signature NVL(<NUMERIC>, <NUMERIC>)");
  }

  /** Unit test for LATERAL CROSS JOIN to table function. */
  @Test void testLateralJoin() {
    final String sql = "SELECT *\n"
        + "FROM AUX.SIMPLETABLE ST\n"
        + "CROSS JOIN LATERAL TABLE(AUX.TBLFUN(ST.INTCOL))";
    CalciteAssert.that(CalciteAssert.Config.AUX)
        .query(sql)
        .returnsUnordered(
            "STRCOL=ABC; INTCOL=1; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=0; s=",
            "STRCOL=GHI; INTCOL=3; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=2; s=ab");
  }

  /** Unit test for view expansion with lateral join. */
  @Test void testExpandViewWithLateralJoin() {
    final String sql = "SELECT * FROM AUX.VIEWLATERAL";
    CalciteAssert.that(CalciteAssert.Config.AUX)
        .query(sql)
        .returnsUnordered(
            "STRCOL=ABC; INTCOL=1; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=0; s=",
            "STRCOL=GHI; INTCOL=3; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=2; s=ab");
  }

  /** Tests that {@link Hook#PARSE_TREE} works. */
  @Test void testHook() {
    final int[] callCount = {0};
    try (Hook.Closeable ignored = Hook.PARSE_TREE.<Object[]>addThread(
        args -> {

          assertThat(args.length, equalTo(2));
          assertThat(args[0], instanceOf(String.class));
          assertThat(args[0],
              equalTo("select \"deptno\", \"commission\", sum(\"salary\") s\n"
                  + "from \"hr\".\"emps\"\n"
                  + "group by \"deptno\", \"commission\""));
          assertThat(args[1], instanceOf(SqlSelect.class));
          ++callCount[0];

        })) {
      // Simple query does not run the hook.
      testSimple();
      assertThat(callCount[0], equalTo(0));

      // Non-trivial query runs hook once.
      testGroupByNull();
      assertThat(callCount[0], equalTo(1));
    }
  }

  /** Tests {@link SqlDialect}. */
  @Test void testDialect() {
    final String[] sqls = {null};
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) as c from \"foodmart\".\"employee\" as e1\n"
            + "  where \"first_name\" = 'abcde'\n"
            + "  and \"gender\" = 'F'")
        .withHook(Hook.QUERY_PLAN, (Consumer<String>) sql -> sqls[0] = sql)

        .returns("C=0\n");
    switch (CalciteAssert.DB) {
    case HSQLDB:
      assertThat(sqls[0],
          isLinux("SELECT COUNT(*) AS \"C\"\n"
              + "FROM \"foodmart\".\"employee\"\n"
              + "WHERE \"first_name\" = 'abcde' AND \"gender\" = 'F'"));
      break;
    }
  }

  @Test void testExplicitImplicitSchemaSameName() throws Exception {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<>();
    final SchemaPlus aSchema = rootSchema.add("a",
        new AbstractSchema() {
          @Override protected Map<String, Schema> getSubSchemaMap() {
            return aSubSchemaMap;
          }
        });

    // add explicit schema "/a/b".
    aSchema.add("b", new AbstractSchema());

    // add implicit schema "/a/b"
    aSubSchemaMap.put("b", new AbstractSchema());

    aSchema.setCacheEnabled(true);

    // explicit should win implicit.
    assertThat(aSchema.getSubSchemaNames().size(), is(1));
  }

  @Test void testSimpleCalciteSchema() throws Exception {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<>();
    final SchemaPlus aSchema = rootSchema.add("a",
        new AbstractSchema() {
          @Override protected Map<String, Schema> getSubSchemaMap() {
            return aSubSchemaMap;
          }
        });

    // add explicit schema "/a/b".
    aSchema.add("b", new AbstractSchema());

    // add implicit schema "/a/c"
    aSubSchemaMap.put("c", new AbstractSchema());

    assertThat(aSchema.getSubSchema("c"), notNullValue());
    assertThat(aSchema.getSubSchema("b"), notNullValue());

    // add implicit schema "/a/b"
    aSubSchemaMap.put("b", new AbstractSchema());
    // explicit should win implicit.
    assertThat(aSchema.getSubSchemaNames().size(), is(2));
  }

  @Test void testSimpleCalciteSchemaWithView() throws Exception {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();

    final Multimap<String, org.apache.calcite.schema.Function> functionMap =
        LinkedListMultimap.create();
    // create schema "/a"
    final SchemaPlus aSchema = rootSchema.add("a",
        new AbstractSchema() {
          @Override protected Multimap<String, org.apache.calcite.schema.Function>
          getFunctionMultimap() {
            return functionMap;
          }
        });
    // add view definition
    final String viewName = "V";
    final org.apache.calcite.schema.Function view =
        ViewTable.viewMacro(rootSchema.getSubSchema("a"),
            "values('1', '2')", null, null, false);
    functionMap.put(viewName, view);

    final CalciteSchema calciteSchema = CalciteSchema.from(aSchema);
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction(viewName, true), notNullValue());
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction(viewName, false), notNullValue());
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction("V1", true), nullValue());
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction("V1", false), nullValue());

    assertThat(calciteSchema.getFunctions(viewName, true), hasItem(view));
    assertThat(calciteSchema.getFunctions(viewName, false), hasItem(view));
    assertThat(calciteSchema.getFunctions("V1", true), not(hasItem(view)));
    assertThat(calciteSchema.getFunctions("V1", false), not(hasItem(view)));
  }

  @Test void testSchemaCaching() throws Exception {
    final Connection connection =
        CalciteAssert.that(CalciteAssert.Config.JDBC_FOODMART).connect();
    final CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<>();
    final SchemaPlus aSchema = rootSchema.add("a", new AbstractSchema() {
      @Override protected Map<String, Schema> getSubSchemaMap() {
        return aSubSchemaMap;
      }
    });
    aSchema.setCacheEnabled(true);
    assertThat(aSchema.getSubSchemaNames().size(), is(0));

    // first call, to populate the cache
    assertThat(aSchema.getSubSchemaNames().size(), is(0));

    // create schema "/a/b1". Appears only when we disable caching.
    aSubSchemaMap.put("b1", new AbstractSchema());
    assertThat(aSchema.getSubSchemaNames().size(), is(0));
    assertThat(aSchema.getSubSchema("b1"), nullValue());
    aSchema.setCacheEnabled(false);
    assertThat(aSchema.getSubSchemaNames().size(), is(1));
    assertThat(aSchema.getSubSchema("b1"), notNullValue());

    // create schema "/a/b2". Appears immediately, because caching is disabled.
    aSubSchemaMap.put("b2", new AbstractSchema());
    assertThat(aSchema.getSubSchemaNames().size(), is(2));

    // an explicit sub-schema appears immediately, even if caching is enabled
    aSchema.setCacheEnabled(true);
    assertThat(aSchema.getSubSchemaNames().size(), is(2));
    aSchema.add("b3", new AbstractSchema()); // explicit
    aSubSchemaMap.put("b4", new AbstractSchema()); // implicit
    assertThat(aSchema.getSubSchemaNames().size(), is(3));
    aSchema.setCacheEnabled(false);
    assertThat(aSchema.getSubSchemaNames().size(), is(4));
    for (String name : aSchema.getSubSchemaNames()) {
      assertThat(aSchema.getSubSchema(name), notNullValue());
    }

    // create schema "/a2"
    final Map<String, Schema> a2SubSchemaMap = new HashMap<>();
    final SchemaPlus a2Schema = rootSchema.add("a", new AbstractSchema() {
      @Override protected Map<String, Schema> getSubSchemaMap() {
        return a2SubSchemaMap;
      }
    });
    a2Schema.setCacheEnabled(true);
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));

    // create schema "/a2/b3". Change not visible since caching is enabled.
    a2SubSchemaMap.put("b3", new AbstractSchema());
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));
    Thread.sleep(1);
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));

    // Change visible after we turn off caching.
    a2Schema.setCacheEnabled(false);
    assertThat(a2Schema.getSubSchemaNames().size(), is(1));
    a2SubSchemaMap.put("b4", new AbstractSchema());
    assertThat(a2Schema.getSubSchemaNames().size(), is(2));
    for (String name : aSchema.getSubSchemaNames()) {
      assertThat(aSchema.getSubSchema(name), notNullValue());
    }

    // add tables and retrieve with various case sensitivities
    final TableInRootSchemaTest.SimpleTable table =
        new TableInRootSchemaTest.SimpleTable();
    a2Schema.add("table1", table);
    a2Schema.add("TABLE1", table);
    a2Schema.add("tabLe1", table);
    a2Schema.add("tabLe2", table);
    assertThat(a2Schema.getTableNames().size(), equalTo(4));
    final CalciteSchema a2CalciteSchema = CalciteSchema.from(a2Schema);
    assertThat(a2CalciteSchema.getTable("table1", true), notNullValue());
    assertThat(a2CalciteSchema.getTable("table1", false), notNullValue());
    assertThat(a2CalciteSchema.getTable("taBle1", true), nullValue());
    assertThat(a2CalciteSchema.getTable("taBle1", false), notNullValue());
    final TableMacro function =
        ViewTable.viewMacro(a2Schema, "values 1", null, null, null);
    Util.discard(function);

    connection.close();
  }

  @Test void testCaseSensitiveSubQueryOracle() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(Lex.ORACLE);

    with.query("select DID from (select DEPTID as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select x.DID from (select DEPTID as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X")
        .returnsUnordered("DID=1", "DID=2");
  }

  @Test void testUnquotedCaseSensitiveSubQueryMySql() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(Lex.MYSQL);

    with.query("select DID from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select x.DID from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select X.DID from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select X.DID2 from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");

    with.query("select X.DID2 from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");
  }

  @Test void testQuotedCaseSensitiveSubQueryMySql() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(Lex.MYSQL);

    with.query("select `DID` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select `x`.`DID` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select `X`.`DID` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select `X`.`DID2` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");

    with.query("select `X`.`DID2` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");
  }

  @Test void testUnquotedCaseSensitiveSubQuerySqlServer() {
    CalciteAssert.that()
        .with(Lex.SQL_SERVER)
        .query("select DID from (select deptid as did FROM\n"
            + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");
  }

  @Test void testQuotedCaseSensitiveSubQuerySqlServer() {
    CalciteAssert.that()
        .with(Lex.SQL_SERVER)
        .query("select [DID] from (select deptid as did FROM\n"
            + "     ( values (1), (2) ) as T1([deptid]) ) ")
        .returnsUnordered("DID=1", "DID=2");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-596">[CALCITE-596]
   * JDBC adapter incorrectly reads null values as 0</a>.
   */
  @Test void testPrimitiveColumnsWithNullValues() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE TABLE T1 (\n"
        + "ID INTEGER,\n"
        + "VALS DOUBLE)");
    baseStmt.execute("INSERT INTO T1 VALUES (1, 1.0)");
    baseStmt.execute("INSERT INTO T1 VALUES (2, null)");
    baseStmt.execute("INSERT INTO T1 VALUES (null, 2.0)");

    baseStmt.close();
    baseConnection.commit();

    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'BASEJDBC',\n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'jdbc',\n"
            + "       name: 'BASEJDBC',\n"
            + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
            + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
            + "       jdbcCatalog: null,\n"
            + "       jdbcSchema: null\n"
            + "     }\n"
            + "  ]\n"
            + "}");

    Connection calciteConnection =
        DriverManager.getConnection("jdbc:calcite:", info);

    ResultSet rs = calciteConnection.prepareStatement("select * from t1")
        .executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject("ID"), equalTo(1));
    assertThat((Double) rs.getObject("VALS"), equalTo(1.0));

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject("ID"), equalTo(2));
    assertThat(rs.getObject("VALS"), nullValue());

    assertThat(rs.next(), is(true));
    assertThat(rs.getObject("ID"), nullValue());
    assertThat((Double) rs.getObject("VALS"), equalTo(2.0));

    rs.close();
    calciteConnection.close();

  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2054">[CALCITE-2054]
   * Error while validating UPDATE with dynamic parameter in SET clause</a>.
   */
  @Test void testUpdateBind() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    try (Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
         Statement baseStmt = baseConnection.createStatement()) {
      baseStmt.execute("CREATE TABLE T2 (\n"
          + "ID INTEGER,\n"
          + "VALS DOUBLE)");
      baseStmt.execute("INSERT INTO T2 VALUES (1, 1.0)");
      baseStmt.execute("INSERT INTO T2 VALUES (2, null)");
      baseStmt.execute("INSERT INTO T2 VALUES (null, 2.0)");

      baseStmt.close();
      baseConnection.commit();

      Properties info = new Properties();
      final String model = "inline:"
          + "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'BASEJDBC',\n"
          + "  schemas: [\n"
          + "     {\n"
          + "       type: 'jdbc',\n"
          + "       name: 'BASEJDBC',\n"
          + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
          + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
          + "       jdbcCatalog: null,\n"
          + "       jdbcSchema: null\n"
          + "     }\n"
          + "  ]\n"
          + "}";
      info.put("model", model);

      Connection calciteConnection =
          DriverManager.getConnection("jdbc:calcite:", info);

      ResultSet rs = calciteConnection.prepareStatement("select * from t2")
          .executeQuery();

      assertThat(rs.next(), is(true));
      assertThat((Integer) rs.getObject("ID"), is(1));
      assertThat((Double) rs.getObject("VALS"), is(1.0));

      assertThat(rs.next(), is(true));
      assertThat((Integer) rs.getObject("ID"), is(2));
      assertThat(rs.getObject("VALS"), nullValue());

      assertThat(rs.next(), is(true));
      assertThat(rs.getObject("ID"), nullValue());
      assertThat((Double) rs.getObject("VALS"), equalTo(2.0));

      rs.close();

      final String sql = "update t2 set vals=? where id=?";
      try (PreparedStatement ps =
               calciteConnection.prepareStatement(sql)) {
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertThat(pmd.getParameterCount(), is(2));
        assertThat(pmd.getParameterType(1), is(Types.DOUBLE));
        assertThat(pmd.getParameterType(2), is(Types.INTEGER));
        ps.close();
      }
      calciteConnection.close();
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-730">[CALCITE-730]
   * ClassCastException in table from CloneSchema</a>. */
  @Test void testNullableNumericColumnInCloneSchema() {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'SCOTT_CLONE',\n"
        + "  schemas: [ {\n"
        + "    name: 'SCOTT_CLONE',\n"
        + "    type: 'custom',\n"
        + "    factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',\n"
        + "    operand: {\n"
        + "      jdbcDriver: '" + JdbcTest.SCOTT.driver + "',\n"
        + "      jdbcUser: '" + JdbcTest.SCOTT.username + "',\n"
        + "      jdbcPassword: '" + JdbcTest.SCOTT.password + "',\n"
        + "      jdbcUrl: '" + JdbcTest.SCOTT.url + "',\n"
        + "      jdbcSchema: 'SCOTT'\n"
        + "   } } ]\n"
        + "}")
        .query("select * from emp")
        .returns(input -> {

          final StringBuilder buf = new StringBuilder();
          try {
            final int columnCount = input.getMetaData().getColumnCount();
            while (input.next()) {
              for (int i = 0; i < columnCount; i++) {
                buf.append(input.getObject(i + 1));
              }
            }

          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }

        });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1097">[CALCITE-1097]
   * Exception when executing query with too many aggregation columns</a>. */
  @Test void testAggMultipleMeasures() throws SQLException {
    final Driver driver = new Driver();
    CalciteConnection connection = (CalciteConnection)
        driver.connect("jdbc:calcite:", new Properties());
    SchemaPlus rootSchema = connection.getRootSchema();
    rootSchema.add("sale", new ReflectiveSchema(new Smalls.WideSaleSchema()));
    connection.setSchema("sale");
    final Statement statement = connection.createStatement();

    // 200 columns: sum(sale0) + ... sum(sale199)
    ResultSet resultSet =
        statement.executeQuery("select s.\"prodId\"" + sums(200, true) + "\n"
            + "from \"sale\".\"prod\" as s group by s.\"prodId\"\n");
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(100));
    assertThat(resultSet.getInt(2), is(10));
    assertThat(resultSet.getInt(200), is(10));
    assertThat(resultSet.next(), is(false));

    // 800 columns:
    //   sum(sale0 + 0) + ... + sum(sale0 + 100) + ... sum(sale99 + 799)
    final int n = 800;
    resultSet =
        statement.executeQuery("select s.\"prodId\"" + sums(n, false) + "\n"
            + "from \"sale\".\"prod\" as s group by s.\"prodId\"\n");
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(100));
    assertThat(resultSet.getInt(2), is(10));
    assertThat(resultSet.getInt(n), is(n + 8));
    assertThat(resultSet.next(), is(false));

    connection.close();
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2224">[CALCITE-2224]
   * WITHIN GROUP clause for aggregate functions</a>. */
  @Test public void testWithinGroupClause1() {
    final String sql = "select X,\n"
        + " collect(Y) within group (order by Y desc) as \"SET\"\n"
        + "from (values (1, 'a'), (1, 'b'),\n"
        + "             (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "group by X\n"
        + "limit 10";
    CalciteAssert.that().query(sql)
        .returnsUnordered("X=1; SET=[b, a]",
            "X=3; SET=[d, c]");
  }

  @Test public void testWithinGroupClause2() {
    final String sql = "select X,\n"
        + " collect(Y) within group (order by Y desc) as SET_1,\n"
        + " collect(Y) within group (order by Y asc) as SET_2\n"
        + "from (values (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "group by X\n"
        + "limit 10";
    CalciteAssert
        .that()
        .query(sql)
        .returnsUnordered("X=1; SET_1=[b, a]; SET_2=[a, b]",
            "X=3; SET_1=[d, c]; SET_2=[c, d]");
  }

  @Test public void testWithinGroupClause3() {
    final String sql = "select"
        + " collect(Y) within group (order by Y desc) as SET_1,\n"
        + " collect(Y) within group (order by Y asc) as SET_2\n"
        + "from (values (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "limit 10";
    CalciteAssert.that().query(sql)
        .returns("SET_1=[d, c, b, a]; SET_2=[a, b, c, d]\n");
  }

  @Test public void testWithinGroupClause4() {
    final String sql = "select"
        + " collect(Y) within group (order by Y desc) as SET_1,\n"
        + " collect(Y) within group (order by Y asc) as SET_2\n"
        + "from (values (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "group by X\n"
        + "limit 10";
    CalciteAssert.that().query(sql)
        .returnsUnordered("SET_1=[b, a]; SET_2=[a, b]",
            "SET_1=[d, c]; SET_2=[c, d]");
  }

  @Test public void testWithinGroupClause5() {
    CalciteAssert
        .that()
        .query("select collect(array[X, Y])\n"
            + " within group (order by Y desc) as \"SET\"\n"
            + "from (values ('b', 'a'), ('a', 'b'), ('a', 'c'),\n"
            + "             ('a', 'd')) AS t(X, Y)\n"
            + "limit 10")
        .returns("SET=[[a, d], [a, c], [a, b], [b, a]]\n");
  }

  @Test public void testWithinGroupClause6() {
    final String sql = "select collect(\"commission\")"
        + " within group (order by \"commission\")\n"
        + "from \"hr\".\"emps\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(sql)
        .explainContains("EnumerableAggregate(group=[{}], "
            + "EXPR$0=[COLLECT($4) WITHIN GROUP ([4])])")
        .returns("EXPR$0=[250, 500, 1000]\n");
  }

<<<<<<< HEAD
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3565">[CALCITE-3565]
   * Explicitly cast assignable operand types to decimal for udf</a>. */
  @Test void testAssignableTypeCast() {
    final String sql = "SELECT ST_MakePoint(1, 2.1)";
    CalciteAssert.that()
        .with(CalciteAssert.Config.GEO)
        .query(sql)
        .planContains("static final java.math.BigDecimal $L4J$C$new_java_math_BigDecimal_1_ = "
            + "new java.math.BigDecimal(\n"
            + "              1)")
        .planContains("org.apache.calcite.runtime.GeoFunctions.ST_MakePoint("
            + "$L4J$C$new_java_math_BigDecimal_1_, v)")
        .returns("EXPR$0={\"x\":1,\"y\":2.1}\n");
  }

  @Test void testMatchSimple() {
    final String sql = "select *\n"
        + "from \"hr\".\"emps\" match_recognize (\n"
        + "  order by \"empid\" desc\n"
        + "  measures up.\"commission\" as c,\n"
        + "    up.\"empid\" as empid,\n"
        + "    2 as two\n"
        + "  pattern (up s)\n"
        + "  define up as up.\"empid\" = 100)";
    final String convert = ""
        + "LogicalProject(C=[$0], EMPID=[$1], TWO=[$2])\n"
        + "  LogicalMatch(partition=[[]], order=[[0 DESC]], "
        + "outputFields=[[C, EMPID, TWO]], allRows=[false], "
        + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[('UP', 'S')], "
        + "isStrictStarts=[false], isStrictEnds=[false], subsets=[[]], "
        + "patternDefinitions=[[=(CAST(PREV(UP.$0, 0)):INTEGER NOT NULL, 100)]], "
        + "inputFields=[[empid, deptno, name, salary, commission]])\n"
        + "    LogicalTableScan(table=[[hr, emps]])\n";
    final String plan = "PLAN="
        + "EnumerableMatch(partition=[[]], order=[[0 DESC]], "
        + "outputFields=[[C, EMPID, TWO]], allRows=[false], "
        + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[('UP', 'S')], "
        + "isStrictStarts=[false], isStrictEnds=[false], subsets=[[]], "
        + "patternDefinitions=[[=(CAST(PREV(UP.$0, 0)):INTEGER NOT NULL, 100)]], "
        + "inputFields=[[empid, deptno, name, salary, commission]])\n"
        + "  EnumerableTableScan(table=[[hr, emps]])";
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(sql)
        .convertContains(convert)
        .explainContains(plan)
        .returns("C=1000; EMPID=100; TWO=2\nC=500; EMPID=200; TWO=2\n");
  }

  @Test void testMatch() {
    final String sql = "select *\n"
        + "from \"hr\".\"emps\" match_recognize (\n"
        + "  order by \"empid\" desc\n"
        + "  measures \"commission\" as c,\n"
        + "    \"empid\" as empid\n"
        + "  pattern (s up)\n"
        + "  define up as up.\"commission\" < prev(up.\"commission\"))";
    final String convert = ""
        + "LogicalProject(C=[$0], EMPID=[$1])\n"
        + "  LogicalMatch(partition=[[]], order=[[0 DESC]], "
        + "outputFields=[[C, EMPID]], allRows=[false], "
        + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[('S', 'UP')], "
        + "isStrictStarts=[false], isStrictEnds=[false], subsets=[[]], "
        + "patternDefinitions=[[<(PREV(UP.$4, 0), PREV(UP.$4, 1))]], "
        + "inputFields=[[empid, deptno, name, salary, commission]])\n"
        + "    LogicalTableScan(table=[[hr, emps]])\n";
    final String plan = "PLAN="
        + "EnumerableMatch(partition=[[]], order=[[0 DESC]], "
        + "outputFields=[[C, EMPID]], allRows=[false], "
        + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[('S', 'UP')], "
        + "isStrictStarts=[false], isStrictEnds=[false], subsets=[[]], "
        + "patternDefinitions=[[<(PREV(UP.$4, 0), PREV(UP.$4, 1))]], "
        + "inputFields=[[empid, deptno, name, salary, commission]])\n"
        + "  EnumerableTableScan(table=[[hr, emps]])";
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(sql)
        .convertContains(convert)
        .explainContains(plan)
        .returns("C=1000; EMPID=100\nC=500; EMPID=200\n");
  }

  @Test void testJsonType() {
    CalciteAssert.that()
        .query("SELECT JSON_TYPE(v) AS c1\n"
            + ",JSON_TYPE(JSON_VALUE(v, 'lax $.b' ERROR ON ERROR)) AS c2\n"
            + ",JSON_TYPE(JSON_VALUE(v, 'strict $.a[0]' ERROR ON ERROR)) AS c3\n"
            + ",JSON_TYPE(JSON_VALUE(v, 'strict $.a[1]' ERROR ON ERROR)) AS c4\n"
            + "FROM (VALUES ('{\"a\": [10, true],\"b\": \"[10, true]\"}')) AS t(v)\n"
            + "limit 10")
        .returns("C1=OBJECT; C2=ARRAY; C3=INTEGER; C4=BOOLEAN\n");
  }

  @Test void testJsonDepth() {
    CalciteAssert.that()
        .query("SELECT JSON_DEPTH(v) AS c1\n"
            + ",JSON_DEPTH(JSON_VALUE(v, 'lax $.b' ERROR ON ERROR)) AS c2\n"
            + ",JSON_DEPTH(JSON_VALUE(v, 'strict $.a[0]' ERROR ON ERROR)) AS c3\n"
            + ",JSON_DEPTH(JSON_VALUE(v, 'strict $.a[1]' ERROR ON ERROR)) AS c4\n"
            + "FROM (VALUES ('{\"a\": [10, true],\"b\": \"[10, true]\"}')) AS t(v)\n"
            + "limit 10")
        .returns("C1=3; C2=2; C3=1; C4=1\n");
  }

  @Test void testJsonLength() {
    CalciteAssert.that()
        .query("SELECT JSON_LENGTH(v) AS c1\n"
            + ",JSON_LENGTH(v, 'lax $.a') AS c2\n"
            + ",JSON_LENGTH(v, 'strict $.a[0]') AS c3\n"
            + ",JSON_LENGTH(v, 'strict $.a[1]') AS c4\n"
            + "FROM (VALUES ('{\"a\": [10, true]}')) AS t(v)\n"
            + "limit 10")
        .returns("C1=1; C2=2; C3=1; C4=1\n");
  }

  @Test void testJsonPretty() {
    CalciteAssert.that()
        .query("SELECT JSON_PRETTY(v) AS c1\n"
            + "FROM (VALUES ('{\"a\": [10, true],\"b\": [10, true]}')) as t(v)\n"
            + "limit 10")
        .returns("C1={\n"
            + "  \"a\" : [ 10, true ],\n"
            + "  \"b\" : [ 10, true ]\n"
            + "}\n");
  }

  @Test void testJsonKeys() {
    CalciteAssert.that()
        .query("SELECT JSON_KEYS(v) AS c1\n"
            + ",JSON_KEYS(v, 'lax $.a') AS c2\n"
            + ",JSON_KEYS(v, 'lax $.b') AS c3\n"
            + ",JSON_KEYS(v, 'strict $.a[0]') AS c4\n"
            + ",JSON_KEYS(v, 'strict $.a[1]') AS c5\n"
            + "FROM (VALUES ('{\"a\": [10, true],\"b\": {\"c\": 30}}')) AS t(v)\n"
            + "limit 10")
        .returns("C1=[\"a\",\"b\"]; C2=null; C3=[\"c\"]; C4=null; C5=null\n");
  }

  @Test void testJsonRemove() {
    CalciteAssert.that()
        .query("SELECT JSON_REMOVE(v, '$[1]') AS c1\n"
            + "FROM (VALUES ('[\"a\", [\"b\", \"c\"], \"d\"]')) AS t(v)\n"
            + "limit 10")
        .returns("C1=[\"a\",\"d\"]\n");
  }

  @Test void testJsonStorageSize() {
    CalciteAssert.that()
        .query("SELECT\n"
            + "JSON_STORAGE_SIZE('[100, \"sakila\", [1, 3, 5], 425.05]') AS A,\n"
            + "JSON_STORAGE_SIZE('{\"a\": 10, \"b\": \"a\", \"c\": \"[1, 3, 5, 7]\"}') AS B,\n"
            + "JSON_STORAGE_SIZE('{\"a\": 10, \"b\": \"xyz\", \"c\": \"[1, 3, 5, 7]\"}') AS C,\n"
            + "JSON_STORAGE_SIZE('[100, \"json\", [[10, 20, 30], 3, 5], 425.05]') AS D\n"
            + "limit 10")
        .returns("A=29; B=35; C=37; D=36\n");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2609">[CALCITE-2609]
   * Dynamic parameters ("?") pushed to underlying JDBC schema, causing
   * error</a>.
   */
  @Test void testQueryWithParameter() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    try (Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
         Statement baseStmt = baseConnection.createStatement()) {
      baseStmt.execute("CREATE TABLE T3 (\n"
          + "ID INTEGER,\n"
          + "VALS DOUBLE)");
      baseStmt.execute("INSERT INTO T3 VALUES (1, 1.0)");
      baseStmt.execute("INSERT INTO T3 VALUES (2, null)");
      baseStmt.execute("INSERT INTO T3 VALUES (null, 2.0)");
      baseStmt.close();
      baseConnection.commit();

      Properties info = new Properties();
      final String model = "inline:"
          + "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'BASEJDBC',\n"
          + "  schemas: [\n"
          + "     {\n"
          + "       type: 'jdbc',\n"
          + "       name: 'BASEJDBC',\n"
          + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
          + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
          + "       jdbcCatalog: null,\n"
          + "       jdbcSchema: null\n"
          + "     }\n"
          + "  ]\n"
          + "}";
      info.put("model", model);

      Connection calciteConnection =
          DriverManager.getConnection("jdbc:calcite:", info);

      final String sql = "select * from t3 where vals = ?";
      try (PreparedStatement ps =
               calciteConnection.prepareStatement(sql)) {
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertThat(pmd.getParameterCount(), is(1));
        assertThat(pmd.getParameterType(1), is(Types.DOUBLE));
        ps.setDouble(1, 1.0);
        ps.executeQuery();
      }
      calciteConnection.close();
    }
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3347">[CALCITE-3347]
   * IndexOutOfBoundsException in FixNullabilityShuttle when using FilterIntoJoinRule</a>.
   */
  @Test void testSemiJoin() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select *\n"
            + " from \"foodmart\".\"employee\""
            + " where \"employee_id\" = 1 and \"last_name\" in"
            + " (select \"last_name\" from \"foodmart\".\"employee\" where \"employee_id\" = 2)")
        .runs();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3894">[CALCITE-3894]
   * SET operation between DATE and TIMESTAMP returns a wrong result</a>.
   */
  @Test public void testUnionDateTime() {
    CalciteAssert.AssertThat assertThat = CalciteAssert.that();
    String query = "select * from (\n"
        + "select \"id\" from (VALUES(DATE '2018-02-03')) \"foo\"(\"id\")\n"
        + "union\n"
        + "select \"id\" from (VALUES(TIMESTAMP '2008-03-31 12:23:34')) \"foo\"(\"id\"))";
    assertThat.query(query).returns("id=2008-03-31 12:23:34\nid=2018-02-03 00:00:00\n");
  }

=======
  private static String sums(int n, boolean c) {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (c) {
        b.append(", sum(s.\"sale").append(i).append("\")");
      } else {
        b.append(", sum(s.\"sale").append(i % 100).append("\"").append(" + ")
            .append(i).append(")");
      }
    }
    return b.toString();
  }

  // Disable checkstyle, so it doesn't complain about fields like "customer_id".
  //CHECKSTYLE: OFF

  /** A schema that contains two tables by reflection.
   *
   * <p>Here is the SQL to create equivalent tables in Oracle:
   *
   * <blockquote>
   * <pre>
   * CREATE TABLE "emps" (
   *   "empid" INTEGER NOT NULL,
   *   "deptno" INTEGER NOT NULL,
   *   "name" VARCHAR2(10) NOT NULL,
   *   "salary" NUMBER(6, 2) NOT NULL,
   *   "commission" INTEGER);
   * INSERT INTO "emps" VALUES (100, 10, 'Bill', 10000, 1000);
   * INSERT INTO "emps" VALUES (200, 20, 'Eric', 8000, 500);
   * INSERT INTO "emps" VALUES (150, 10, 'Sebastian', 7000, null);
   * INSERT INTO "emps" VALUES (110, 10, 'Theodore', 11500, 250);
   *
   * CREATE TABLE "depts" (
   *   "deptno" INTEGER NOT NULL,
   *   "name" VARCHAR2(10) NOT NULL,
   *   "employees" ARRAY OF "Employee",
   *   "location" "Location");
   * INSERT INTO "depts" VALUES (10, 'Sales', null, (-122, 38));
   * INSERT INTO "depts" VALUES (30, 'Marketing', null, (0, 52));
   * INSERT INTO "depts" VALUES (40, 'HR', null, null);
   * </pre>
   * </blockquote>
   */
  public static class HrSchema {
    @Override public String toString() {
      return "HrSchema";
    }

    public final Employee[] emps = {
      new Employee(100, 10, "Bill", 10000, 1000),
      new Employee(200, 20, "Eric", 8000, 500),
      new Employee(150, 10, "Sebastian", 7000, null),
      new Employee(110, 10, "Theodore", 11500, 250),
    };
    public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(30, "Marketing", ImmutableList.of(), new Location(0, 52)),

      new Department(40, "HR", Collections.singletonList(emps[1]), null),
    };
    public final Dependent[] dependents = {
      new Dependent(10, "Michael"),
      new Dependent(10, "Jane"),
    };
    public final Dependent[] locations = {
      new Dependent(10, "San Francisco"),
      new Dependent(20, "San Diego"),
    };

    public QueryableTable foo(int count) {
      return Smalls.generateStrings(count);
    }

    public TranslatableTable view(String s) {
      return Smalls.view(s);
    }
  }

  public static class HrSchemaBig {
    @Override public String toString() {
      return "HrSchema";
    }

    public final Employee[] emps = {
        new Employee(1, 10, "Bill", 10000, 1000),
        new Employee(2, 20, "Eric", 8000, 500),
        new Employee(3, 10, "Sebastian", 7000, null),
        new Employee(4, 10, "Theodore", 11500, 250),
        new Employee(5, 10, "Marjorie", 10000, 1000),
        new Employee(6, 20, "Guy", 8000, 500),
        new Employee(7, 10, "Dieudonne", 7000, null),
        new Employee(8, 10, "Haroun", 11500, 250),
        new Employee(9, 10, "Sarah", 10000, 1000),
        new Employee(10, 20, "Gabriel", 8000, 500),
        new Employee(11, 10, "Pierre", 7000, null),
        new Employee(12, 10, "Paul", 11500, 250),
        new Employee(13, 10, "Jacques", 100, 1000),
        new Employee(14, 20, "Khawla", 8000, 500),
        new Employee(15, 10, "Brielle", 7000, null),
        new Employee(16, 10, "Hyuna", 11500, 250),
        new Employee(17, 10, "Ahmed", 10000, 1000),
        new Employee(18, 20, "Lara", 8000, 500),
        new Employee(19, 10, "Capucine", 7000, null),
        new Employee(20, 10, "Michelle", 11500, 250),
        new Employee(21, 10, "Cerise", 10000, 1000),
        new Employee(22, 80, "Travis", 8000, 500),
        new Employee(23, 10, "Taylor", 7000, null),
        new Employee(24, 10, "Seohyun", 11500, 250),
        new Employee(25, 70, "Helen", 10000, 1000),
        new Employee(26, 50, "Patric", 8000, 500),
        new Employee(27, 10, "Clara", 7000, null),
        new Employee(28, 10, "Catherine", 11500, 250),
        new Employee(29, 10, "Anibal", 10000, 1000),
        new Employee(30, 30, "Ursula", 8000, 500),
        new Employee(31, 10, "Arturito", 7000, null),
        new Employee(32, 70, "Diane", 11500, 250),
        new Employee(33, 10, "Phoebe", 10000, 1000),
        new Employee(34, 20, "Maria", 8000, 500),
        new Employee(35, 10, "Edouard", 7000, null),
        new Employee(36, 110, "Isabelle", 11500, 250),
        new Employee(37, 120, "Olivier", 10000, 1000),
        new Employee(38, 20, "Yann", 8000, 500),
        new Employee(39, 60, "Ralf", 7000, null),
        new Employee(40, 60, "Emmanuel", 11500, 250),
        new Employee(41, 10, "Berenice", 10000, 1000),
        new Employee(42, 20, "Kylie", 8000, 500),
        new Employee(43, 80, "Natacha", 7000, null),
        new Employee(44, 100, "Henri", 11500, 250),
        new Employee(45, 90, "Pascal", 10000, 1000),
        new Employee(46, 90, "Sabrina", 8000, 500),
        new Employee(47, 8, "Riyad", 7000, null),
        new Employee(48, 5, "Andy", 11500, 250),
    };
    public final Department[] depts = {
        new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(20, "Marketing", ImmutableList.of(), new Location(0, 52)),
        new Department(30, "HR", Collections.singletonList(emps[1]), null),
        new Department(40, "Administration", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(50, "Design", ImmutableList.of(), new Location(0, 52)),
        new Department(60, "IT", Collections.singletonList(emps[1]), null),
        new Department(70, "Production", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(80, "Finance", ImmutableList.of(), new Location(0, 52)),
        new Department(90, "Accounting", Collections.singletonList(emps[1]), null),
        new Department(100, "Research", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(110, "Maintenance", ImmutableList.of(), new Location(0, 52)),
        new Department(120, "Client Support", Collections.singletonList(emps[1]), null),
    };
  }

  public static class Employee {
    public final int empid;
    public final int deptno;
    public final String name;
    public final float salary;
    public final Integer commission;

    public Employee(int empid, int deptno, String name, float salary,
        Integer commission) {
      this.empid = empid;
      this.deptno = deptno;
      this.name = name;
      this.salary = salary;
      this.commission = commission;
    }

    @Override public String toString() {
      return "Employee [empid: " + empid + ", deptno: " + deptno
          + ", name: " + name + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Employee
          && empid == ((Employee) obj).empid;
    }
  }

  public static class Department {
    public final int deptno;
    public final String name;

    @org.apache.calcite.adapter.java.Array(component = Employee.class)
    public final List<Employee> employees;
    public final Location location;

    public Department(int deptno, String name, List<Employee> employees,
        Location location) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
      this.location = location;
    }

    @Override public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + ", location: " + location + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Department
          && deptno == ((Department) obj).deptno;
    }
  }

  public static class Location {
    public final int x;
    public final int y;

    public Location(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override public String toString() {
      return "Location [x: " + x + ", y: " + y + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Location
          && x == ((Location) obj).x
          && y == ((Location) obj).y;
    }
  }

  public static class Dependent {
    public final int empid;
    public final String name;

    public Dependent(int empid, String name) {
      this.empid = empid;
      this.name = name;
    }

    @Override public String toString() {
      return "Dependent [empid: " + empid + ", name: " + name + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Dependent
          && empid == ((Dependent) obj).empid
          && Objects.equals(name, ((Dependent) obj).name);
    }
  }

  public static class Event {
    public final int eventid;
    public final Timestamp ts;

    public Event(int eventid, Timestamp ts) {
      this.eventid = eventid;
      this.ts = ts;
    }

    @Override public String toString() {
      return "Event [eventid: " + eventid + ", ts: " + ts + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Event
          && eventid == ((Event) obj).eventid;
    }
  }

  public static class FoodmartSchema {
    public final SalesFact[] sales_fact_1997 = {
      new SalesFact(100, 10),
      new SalesFact(150, 20),
    };
  }

  public static class LingualSchema {
    public final LingualEmp[] EMPS = {
      new LingualEmp(1, 10),
      new LingualEmp(2, 30)
    };
  }

  public static class LingualEmp {
    public final int EMPNO;
    public final int DEPTNO;

    public LingualEmp(int EMPNO, int DEPTNO) {
      this.EMPNO = EMPNO;
      this.DEPTNO = DEPTNO;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof LingualEmp
          && EMPNO == ((LingualEmp) obj).EMPNO;
    }
  }

  public static class FoodmartJdbcSchema extends JdbcSchema {
    public FoodmartJdbcSchema(DataSource dataSource, SqlDialect dialect,
        JdbcConvention convention, String catalog, String schema) {
      super(dataSource, dialect, convention, catalog, schema);
    }

    public final Table customer = getTable("customer");
  }

  public static class Customer {
    public final int customer_id;

    public Customer(int customer_id) {
      this.customer_id = customer_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Customer
          && customer_id == ((Customer) obj).customer_id;
    }
  }

  public static class SalesFact {
    public final int cust_id;
    public final int prod_id;

    public SalesFact(int cust_id, int prod_id) {
      this.cust_id = cust_id;
      this.prod_id = prod_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof SalesFact
          && cust_id == ((SalesFact) obj).cust_id
          && prod_id == ((SalesFact) obj).prod_id;
    }
  }

  //CHECKSTYLE: ON

  /** Abstract base class for implementations of {@link ModifiableTable}. */
  public abstract static class AbstractModifiableTable
      extends AbstractTable implements ModifiableTable {
    protected AbstractModifiableTable(String tableName) {
    }

    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      return LogicalTableModify.create(table, catalogReader, child, operation,
          updateColumnList, sourceExpressionList, flattened);
    }
  }

  /** Abstract base class for implementations of {@link ModifiableView}. */
  public abstract static class AbstractModifiableView
      extends AbstractTable implements ModifiableView {
    protected AbstractModifiableView() {
    }
  }

  /** Factory for EMP and DEPT tables. */
  public static class EmpDeptTableFactory implements TableFactory<Table> {
    public static final TryThreadLocal<List<Employee>> THREAD_COLLECTION =
        TryThreadLocal.of(null);

    public Table create(
        SchemaPlus schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final Class clazz;
      final Object[] array;
      switch (name) {
      case "EMPLOYEES":
        clazz = Employee.class;
        array = new HrSchema().emps;
        break;
      case "MUTABLE_EMPLOYEES":
        List<Employee> employees = THREAD_COLLECTION.get();
        if (employees == null) {
          employees = Collections.emptyList();
        }
        return JdbcFrontLinqBackTest.mutable(name, employees);
      case "DEPARTMENTS":
        clazz = Department.class;
        array = new HrSchema().depts;
        break;
      default:
        throw new AssertionError(name);
      }
      return new AbstractQueryableTable(clazz) {
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return ((JavaTypeFactory) typeFactory).createType(clazz);
        }

        public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
            SchemaPlus schema, String tableName) {
          return new AbstractTableQueryable<T>(queryProvider, schema, this,
              tableName) {
            public Enumerator<T> enumerator() {
              @SuppressWarnings("unchecked") final List<T> list =
                  (List) Arrays.asList(array);
              return Linq4j.enumerator(list);
            }
          };
        }
      };
    }
  }

  /** Schema factory that creates {@link MySchema} objects. */
  public static class MySchemaFactory implements SchemaFactory {
    public Schema create(
        SchemaPlus parentSchema,
        String name,
        final Map<String, Object> operand) {
      final boolean mutable =
          SqlFunctions.isNotFalse((Boolean) operand.get("mutable"));
      return new ReflectiveSchema(new HrSchema()) {
        @Override protected Map<String, Table> getTableMap() {
          // Mine the EMPS table and add it under another name e.g. ELVIS
          final Map<String, Table> tableMap = super.getTableMap();
          final Table table = tableMap.get("emps");
          final String tableName = (String) operand.get("tableName");
          return FlatLists.append(tableMap, tableName, table);
        }

        @Override public boolean isMutable() {
          return mutable;
        }
      };
    }
  }

  /** Mock driver that has a handler that stores the results of each query in
   * a temporary table. */
  public static class AutoTempDriver
      extends org.apache.calcite.jdbc.Driver {
    private final List<Object> results;

    AutoTempDriver(List<Object> results) {
      this.results = results;
    }

    @Override protected Handler createHandler() {
      return new HandlerImpl() {
        @Override public void onStatementExecute(
            AvaticaStatement statement,
            ResultSink resultSink) {
          super.onStatementExecute(statement, resultSink);
          results.add(resultSink);
        }
      };
    }
  }

  /** Mock driver that a given {@link Handler}. */
  public static class HandlerDriver extends org.apache.calcite.jdbc.Driver {
    private static final TryThreadLocal<Handler> HANDLERS =
        TryThreadLocal.of(null);

    public HandlerDriver() {
    }

    @Override protected Handler createHandler() {
      return HANDLERS.get();
    }
  }

  /** Mock driver that can execute a trivial DDL statement. */
  public static class MockDdlDriver extends org.apache.calcite.jdbc.Driver {
    public int counter;

    public MockDdlDriver() {
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
      return new Function0<CalcitePrepare>() {
        @Override public CalcitePrepare apply() {
          return new CalcitePrepareImpl() {
            @Override protected SqlParser.ConfigBuilder createParserConfig() {
              return super.createParserConfig().setParserFactory(stream ->
                  new SqlParserImpl(stream) {

                    @Override public SqlNode parseSqlStmtEof() {
                      return new SqlCall(SqlParserPos.ZERO) {
                        @Override public SqlOperator getOperator() {
                          return new SqlSpecialOperator("COMMIT",
                              SqlKind.COMMIT);
                        }

                        @Override public List<SqlNode> getOperandList() {
                          return ImmutableList.of();
                        }
                      };
                    }

                  });
            }

            @Override public void executeDdl(Context context, SqlNode node) {
              ++counter;
            }
          };
        }
      };
    }
  }

  /** Dummy table. */
  public static class MyTable {
    public String mykey = "foo";
    public Integer myvalue = 1;
  }

  /** Another dummy table. */
  public static class MyTable2 {
    public String mykey = "foo";
    public Integer myvalue = 2;
  }

  /** Schema containing dummy tables. */
  public static class MySchema {
    public MyTable[] mytable = { new MyTable() };
    public MyTable2[] mytable2 = { new MyTable2() };
  }

  /** Locales for which to test DAYNAME and MONTHNAME functions,
   * and expected results of those functions. */
  enum TestLocale {
    ROOT(Locale.ROOT.toString(), shorten("Wednesday"), shorten("Sunday"),
        shorten("January"), shorten("February"), 0),
    EN("en", "Wednesday", "Sunday", "January", "February", 0),
    FR("fr", "mercredi", "dimanche", "janvier", "f\u00e9vrier", 6),
    FR_FR("fr_FR", "mercredi", "dimanche", "janvier", "f\u00e9vrier", 6),
    FR_CA("fr_CA", "mercredi", "dimanche", "janvier", "f\u00e9vrier", 6),
    ZH_CN("zh_CN", "\u661f\u671f\u4e09", "\u661f\u671f\u65e5", "\u4e00\u6708",
        "\u4e8c\u6708", 6),
    ZH("zh", "\u661f\u671f\u4e09", "\u661f\u671f\u65e5", "\u4e00\u6708",
        "\u4e8c\u6708", 6);

    private static String shorten(String name) {
      // In root locale, for Java versions 9 and higher, day and month names
      // are shortened to 3 letters. This means root locale behaves differently
      // to English.
      return TestUtil.getJavaMajorVersion() > 8 ? name.substring(0, 3) : name;
    }

    public final String localeName;
    public final String wednesday;
    public final String sunday;
    public final String january;
    public final String february;
    public final int sundayDayOfWeek;

    TestLocale(String localeName, String wednesday, String sunday,
        String january, String february, int sundayDayOfWeek) {
      this.localeName = localeName;
      this.wednesday = wednesday;
      this.sunday = sunday;
      this.january = january;
      this.february = february;
      this.sundayDayOfWeek = sundayDayOfWeek;
    }
  }
}
