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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
  }

  @Test public void testCorrelatingVariables() {
    // reference to unqualified correlating column
    check("select * from emp where exists (\n"
        + "select * from dept where deptno = sal)");

    // reference to qualified correlating column
    check("select * from emp where exists (\n"
        + "select * from dept where deptno = emp.sal)");
  }

  @Test public void testIntervalCompare() {
    checkExpType(
        "interval '1' hour = interval '1' day",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' hour <> interval '1' hour",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' hour < interval '1' second",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' hour <= interval '1' minute",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' minute > interval '1' second",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' second >= interval '1' day",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' year >= interval '1' year",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' month = interval '1' year",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' month <> interval '1' month",
        "BOOLEAN NOT NULL");
    checkExpType(
        "interval '1' year >= interval '1' month",
        "BOOLEAN NOT NULL");

    checkWholeExpFails(
        "interval '1' second >= interval '1' year",
        "(?s).*Cannot apply '>=' to arguments of type '<INTERVAL SECOND> >= <INTERVAL YEAR>'.*");
    checkWholeExpFails("interval '1' month = interval '1' day",
        "(?s).*Cannot apply '=' to arguments of type '<INTERVAL MONTH> = <INTERVAL DAY>'.*");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-613">[CALCITE-613]
   * Implicitly convert strings in comparisons</a>. */
  @Test public void testDateCompare() {
    // can convert character value to date, time, timestamp, interval
    // provided it is on one side of a comparison operator (=, <, >, BETWEEN)
    checkExpType("date '2015-03-17' < '2015-03-18'", "BOOLEAN NOT NULL");
    checkExpType("date '2015-03-17' > '2015-03-18'", "BOOLEAN NOT NULL");
    checkExpType("date '2015-03-17' = '2015-03-18'", "BOOLEAN NOT NULL");
    checkExpType("'2015-03-17' < date '2015-03-18'", "BOOLEAN NOT NULL");
    checkExpType("date '2015-03-17' between '2015-03-16' and '2015-03-19'",
        "BOOLEAN NOT NULL");
    checkExpType("date '2015-03-17' between '2015-03-16' and '2015-03'||'-19'",
        "BOOLEAN NOT NULL");
    checkExpType("'2015-03-17' between date '2015-03-16' and date '2015-03-19'",
        "BOOLEAN NOT NULL");
    checkExpType("date '2015-03-17' between date '2015-03-16' and '2015-03-19'",
        "BOOLEAN NOT NULL");
    checkExpType("date '2015-03-17' between '2015-03-16' and date '2015-03-19'",
        "BOOLEAN NOT NULL");
    checkExpType("time '12:34:56' < '12:34:57'", "BOOLEAN NOT NULL");
    checkExpType("timestamp '2015-03-17 12:34:56' < '2015-03-17 12:34:57'",
        "BOOLEAN NOT NULL");
    checkExpType("interval '2' hour < '2:30'", "BOOLEAN NOT NULL");

    // can convert to exact and approximate numeric
    checkExpType("123 > '72'", "BOOLEAN NOT NULL");
    checkExpType("12.3 > '7.2'", "BOOLEAN NOT NULL");

    // can convert to boolean
    checkExpType("true = 'true'", "BOOLEAN NOT NULL");
    checkExpFails("^true and 'true'^",
        "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <CHAR\\(4\\)>'\\..*");
  }

  @Test public void testOverlaps() {
    checkExpType("(date '1-2-3', date '1-2-3')\n"
            + " overlaps (date '1-2-3', date '1-2-3')",
        "BOOLEAN NOT NULL");
    checkExpType("period (date '1-2-3', date '1-2-3')\n"
            + " overlaps period (date '1-2-3', date '1-2-3')",
        "BOOLEAN NOT NULL");
    checkExp(
        "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
    checkExp(
        "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')");
    checkExp(
        "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");

    checkWholeExpFails(
        "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    checkWholeExpFails(
        "(time '4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    checkWholeExpFails(
        "(time '4:5:6', time '4:5:6' ) overlaps (time '4:5:6', date '1-2-3')",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    checkWholeExpFails("1 overlaps 2",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type "
            + "'<INTEGER> OVERLAPS <INTEGER>'\\. Supported form.*");

    checkExpType("true\n"
            + "or (date '1-2-3', date '1-2-3')\n"
            + "   overlaps (date '1-2-3', date '1-2-3')\n"
            + "or false",
        "BOOLEAN NOT NULL");
    // row with 3 arguments as left argument to overlaps
    checkExpFails("true\n"
            + "or ^(date '1-2-3', date '1-2-3', date '1-2-3')\n"
            + "   overlaps (date '1-2-3', date '1-2-3')^\n"
            + "or false",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    // row with 3 arguments as right argument to overlaps
    checkExpFails("true\n"
            + "or ^(date '1-2-3', date '1-2-3')\n"
            + "  overlaps (date '1-2-3', date '1-2-3', date '1-2-3')^\n"
            + "or false",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    checkExpFails("^period (date '1-2-3', date '1-2-3')\n"
            + "   overlaps (date '1-2-3', date '1-2-3', date '1-2-3')^",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");
    checkExpFails("true\n"
            + "or ^(1, 2) overlaps (2, 3)^\n"
            + "or false",
        "(?s).*Cannot apply 'OVERLAPS' to arguments of type .*");

    // Other operators with similar syntax
    String[] ops = {
        "overlaps", "contains", "equals", "precedes", "succeeds",
        "immediately precedes", "immediately succeeds"
    };
    for (String op : ops) {
      checkExpType("period (date '1-2-3', date '1-2-3')\n"
              + " " + op + " period (date '1-2-3', date '1-2-3')",
          "BOOLEAN NOT NULL");
      checkExpType("(date '1-2-3', date '1-2-3')\n"
              + " " + op + " (date '1-2-3', date '1-2-3')",
          "BOOLEAN NOT NULL");
    }
  }

  @Test public void testContains() {
    final String cannotApply =
        "(?s).*Cannot apply 'CONTAINS' to arguments of type .*";

    checkExpType("(date '1-2-3', date '1-2-3')\n"
            + " contains (date '1-2-3', date '1-2-3')",
        "BOOLEAN NOT NULL");
    checkExpType("period (date '1-2-3', date '1-2-3')\n"
            + " contains period (date '1-2-3', date '1-2-3')",
        "BOOLEAN NOT NULL");
    checkExp("(date '1-2-3', date '1-2-3')\n"
        + "  contains (date '1-2-3', interval '1' year)");
    checkExp("(time '1:2:3', interval '1' second)\n"
        + " contains (time '23:59:59', time '1:2:3')");
    checkExp("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6')\n"
        + " contains (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");

    // period contains point
    checkExp("(date '1-2-3', date '1-2-3')\n"
        + "  contains date '1-2-3'");
    // same, with "period" keyword
    checkExp("period (date '1-2-3', date '1-2-3')\n"
        + "  contains date '1-2-3'");
    // point contains period
    checkWholeExpFails("date '1-2-3'\n"
            + "  contains (date '1-2-3', date '1-2-3')",
        cannotApply);
    // same, with "period" keyword
    checkWholeExpFails("date '1-2-3'\n"
            + "  contains period (date '1-2-3', date '1-2-3')",
        cannotApply);
    // point contains point
    checkWholeExpFails("date '1-2-3' contains date '1-2-3'",
        cannotApply);

    checkWholeExpFails("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' )\n"
            + " contains (time '4:5:6', interval '1 2:3:4.5' day to second)",
        cannotApply);
    checkWholeExpFails("(time '4:5:6', timestamp '1-2-3 4:5:6' )\n"
            + " contains (time '4:5:6', interval '1 2:3:4.5' day to second)",
        cannotApply);
    checkWholeExpFails("(time '4:5:6', time '4:5:6' )\n"
            + "  contains (time '4:5:6', date '1-2-3')",
        cannotApply);
    checkWholeExpFails("1 contains 2",
        cannotApply);
    // row with 3 arguments
    checkExpFails("true\n"
            + "or ^(date '1-2-3', date '1-2-3', date '1-2-3')\n"
            + "  contains (date '1-2-3', date '1-2-3')^\n"
            + "or false",
        cannotApply);

    checkExpType("true\n"
            + "or (date '1-2-3', date '1-2-3')\n"
            + "  contains (date '1-2-3', date '1-2-3')\n"
            + "or false",
        "BOOLEAN NOT NULL");
    // second argument is a point
    checkExpType("true\n"
            + "or (date '1-2-3', date '1-2-3')\n"
            + "  contains date '1-2-3'\n"
            + "or false",
        "BOOLEAN NOT NULL");
    // first argument may be null, so result may be null
    checkExpType("true\n"
            + "or (date '1-2-3',\n"
            + "     case 1 when 2 then date '1-2-3' else null end)\n"
            + "  contains date '1-2-3'\n"
            + "or false",
        "BOOLEAN");
    // second argument may be null, so result may be null
    checkExpType("true\n"
            + "or (date '1-2-3', date '1-2-3')\n"
            + "  contains case 1 when 1 then date '1-2-3' else null end\n"
            + "or false",
        "BOOLEAN");
    checkExpFails("true\n"
            + "or ^period (date '1-2-3', date '1-2-3')\n"
            + "  contains period (date '1-2-3', time '4:5:6')^\n"
            + "or false",
        cannotApply);
    checkExpFails("true\n"
            + "or ^(1, 2) contains (2, 3)^\n"
            + "or false",
        cannotApply);
  }

  @Test public void testExtract() {
    // TODO: Need to have extract return decimal type for seconds
    // so we can have seconds fractions
    checkExpType("extract(year from interval '1-2' year to month)",
        "BIGINT NOT NULL");
    checkExp("extract(minute from interval '1.1' second)");
    checkExp("extract(year from DATE '2008-2-2')");

    checkWholeExpFails(
        "extract(minute from interval '11' month)",
        "(?s).*Cannot apply.*");
    checkWholeExpFails(
        "extract(year from interval '11' second)",
        "(?s).*Cannot apply.*");
  }

  @Test public void testCastToInterval() {
    checkExpType(
        "cast(interval '1' hour as varchar(20))",
        "VARCHAR(20) NOT NULL");
    checkExpType("cast(interval '1' hour as bigint)", "BIGINT NOT NULL");
    checkExpType("cast(1000 as interval hour)", "INTERVAL HOUR NOT NULL");

    checkExpType(
        "cast(interval '1' month as interval year)",
        "INTERVAL YEAR NOT NULL");
    checkExpType(
        "cast(interval '1-1' year to month as interval month)",
        "INTERVAL MONTH NOT NULL");
    checkExpType("cast(interval '1:1' hour to minute as interval day)",
        "INTERVAL DAY NOT NULL");
    checkExpType(
        "cast(interval '1:1' hour to minute as interval minute to second)",
        "INTERVAL MINUTE TO SECOND NOT NULL");

    checkWholeExpFails(
        "cast(interval '1:1' hour to minute as interval month)",
        "Cast function cannot convert value of type INTERVAL HOUR TO MINUTE to type INTERVAL MONTH");
    checkWholeExpFails(
        "cast(interval '1-1' year to month as interval second)",
        "Cast function cannot convert value of type INTERVAL YEAR TO MONTH to type INTERVAL SECOND");
  }

  @Test public void testMinusDateOperator() {
    checkExpType(
        "(CURRENT_DATE - CURRENT_DATE) HOUR",
        "INTERVAL HOUR NOT NULL");
    checkExpType("(CURRENT_DATE - CURRENT_DATE) YEAR TO MONTH",
        "INTERVAL YEAR TO MONTH NOT NULL");
    checkWholeExpFails(
        "(CURRENT_DATE - LOCALTIME) YEAR TO MONTH",
        "(?s).*Parameters must be of the same type.*");
  }

  @Test public void testBind() {
    sql("select * from emp where deptno = ?").ok();
    sql("select * from emp where deptno = ? and sal < 100000").ok();
    sql("select case when deptno = ? then 1 else 2 end from emp").ok();
    // It is not possible to infer type of ?, because SUBSTRING is overloaded
    sql("select deptno from emp group by substring(name from ^?^ for ?)")
        .fails("Illegal use of dynamic parameter");
    // In principle we could infer that ? should be a VARCHAR
    sql("select count(*) from emp group by position(^?^ in ename)")
        .fails("Illegal use of dynamic parameter");
    sql("select ^deptno^ from emp\n"
        + "group by case when deptno = ? then 1 else 2 end")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("select deptno from emp\n"
        + "group by deptno, case when deptno = ? then 1 else 2 end").ok();
    sql("select 1 from emp having sum(sal) < ?").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1310">[CALCITE-1310]
   * Infer type of arguments to BETWEEN operator</a>. */
  @Test public void testBindBetween() {
    sql("select * from emp where ename between ? and ?").ok();
    sql("select * from emp where deptno between ? and ?").ok();
    sql("select * from emp where ? between deptno and ?").ok();
    sql("select * from emp where ? between ? and deptno").ok();
    sql("select * from emp where ^?^ between ? and ?")
        .fails("Illegal use of dynamic parameter");
  }

  @Test public void testUnnest() {
    checkColumnType("select*from unnest(multiset[1])", "INTEGER NOT NULL");
    checkColumnType("select*from unnest(multiset[1, 2])", "INTEGER NOT NULL");
    checkColumnType(
        "select*from unnest(multiset[321.3, 2.33])",
        "DECIMAL(5, 2) NOT NULL");
    checkColumnType(
        "select*from unnest(multiset[321.3, 4.23e0])",
        "DOUBLE NOT NULL");
    checkColumnType(
        "select*from unnest(multiset[43.2e1, cast(null as decimal(4,2))])",
        "DOUBLE");
    checkColumnType(
        "select*from unnest(multiset[1, 2.3, 1])",
        "DECIMAL(11, 1) NOT NULL");
    checkColumnType(
        "select*from unnest(multiset['1','22','333'])",
        "CHAR(3) NOT NULL");
    checkColumnType(
        "select*from unnest(multiset['1','22','333','22'])",
        "CHAR(3) NOT NULL");
    checkFails(
        "select*from ^unnest(1)^",
        "(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
    check("select*from unnest(multiset(select*from dept))");
    check("select c from unnest(multiset(select deptno from dept)) as t(c)");
    checkFails("select c from unnest(multiset(select * from dept)) as t(^c^)",
        "List of column aliases must have same degree as table; table has 2 columns \\('DEPTNO', 'NAME'\\), whereas alias list has 1 columns");
    checkFails(
        "select ^c1^ from unnest(multiset(select name from dept)) as t(c)",
        "Column 'C1' not found in any table");
  }

  @Test public void testUnnestArray() {
    checkColumnType("select*from unnest(array[1])", "INTEGER NOT NULL");
    checkColumnType("select*from unnest(array[1, 2])", "INTEGER NOT NULL");
    checkColumnType(
        "select*from unnest(array[321.3, 2.33])",
        "DECIMAL(5, 2) NOT NULL");
    checkColumnType(
        "select*from unnest(array[321.3, 4.23e0])",
        "DOUBLE NOT NULL");
    checkColumnType(
        "select*from unnest(array[43.2e1, cast(null as decimal(4,2))])",
        "DOUBLE");
    checkColumnType(
        "select*from unnest(array[1, 2.3, 1])",
        "DECIMAL(11, 1) NOT NULL");
    checkColumnType(
        "select*from unnest(array['1','22','333'])",
        "CHAR(3) NOT NULL");
    checkColumnType(
        "select*from unnest(array['1','22','333','22'])",
        "CHAR(3) NOT NULL");
    checkColumnType(
        "select*from unnest(array['1','22',null,'22'])",
        "CHAR(2)");
    checkFails(
        "select*from ^unnest(1)^",
        "(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
    check("select*from unnest(array(select*from dept))");
    check("select*from unnest(array[1,null])");
    check("select c from unnest(array(select deptno from dept)) as t(c)");
    checkFails("select c from unnest(array(select * from dept)) as t(^c^)",
        "List of column aliases must have same degree as table; table has 2 columns \\('DEPTNO', 'NAME'\\), whereas alias list has 1 columns");
    checkFails(
        "select ^c1^ from unnest(array(select name from dept)) as t(c)",
        "Column 'C1' not found in any table");
  }

  @Test public void testArrayConstructor() {
    sql("select array[1,2] as a from (values (1))")
        .columnType("INTEGER NOT NULL ARRAY NOT NULL");
    sql("select array[1,cast(null as integer), 2] as a\n"
            + "from (values (1))")
        .columnType("INTEGER ARRAY NOT NULL");
    sql("select array[1,null,2] as a from (values (1))")
        .columnType("INTEGER ARRAY NOT NULL");
    sql("select array['1',null,'234',''] as a from (values (1))")
        .columnType("CHAR(3) ARRAY NOT NULL");
  }

  @Test public void testMultisetConstructor() {
    sql("select multiset[1,null,2] as a from (values (1))")
        .columnType("INTEGER MULTISET NOT NULL");
  }

  @Test public void testUnnestArrayColumn() {
    final String sql1 = "select d.deptno, e.*\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) as e";
    final String type = "RecordType(INTEGER NOT NULL DEPTNO,"
        + " INTEGER NOT NULL EMPNO,"
        + " VARCHAR(10) NOT NULL ENAME,"
        + " RecordType(RecordType(VARCHAR(10) NOT NULL TYPE, VARCHAR(20) NOT NULL DESC,"
        + " RecordType(VARCHAR(10) NOT NULL A, VARCHAR(10) NOT NULL B) NOT NULL OTHERS)"
        + " NOT NULL ARRAY NOT NULL SKILLS) NOT NULL DETAIL) NOT NULL";
    sql(sql1).type(type);

    // equivalent query without table alias
    final String sql1b = "select d.deptno, e.*\n"
        + "from dept_nested as d,\n"
        + " UNNEST(employees) as e";
    sql(sql1b).type(type);

    // equivalent query using CROSS JOIN
    final String sql2 = "select d.deptno, e.*\n"
        + "from dept_nested as d CROSS JOIN\n"
        + " UNNEST(d.employees) as e";
    sql(sql2).type(type);

    // equivalent query using CROSS JOIN, without table alias
    final String sql2b = "select d.deptno, e.*\n"
        + "from dept_nested as d CROSS JOIN\n"
        + " UNNEST(employees) as e";
    sql(sql2b).type(type);

    // LATERAL works left-to-right
    final String sql3 = "select d.deptno, e.*\n"
        + "from UNNEST(^d^.employees) as e, dept_nested as d";
    sql(sql3).fails("Table 'D' not found");
  }

  @Test public void testUnnestWithOrdinality() {
    checkResultType("select*from unnest(array[1, 2]) with ordinality",
        "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL ORDINALITY) NOT NULL");
    checkResultType(
        "select*from unnest(array[43.2e1, cast(null as decimal(4,2))]) with ordinality",
        "RecordType(DOUBLE EXPR$0, INTEGER NOT NULL ORDINALITY) NOT NULL");
    checkFails(
        "select*from ^unnest(1) with ordinality^",
        "(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
    check("select deptno\n"
        + "from unnest(array(select*from dept)) with ordinality\n"
        + "where ordinality < 5");
    checkFails("select c from unnest(\n"
        + "  array(select deptno from dept)) with ordinality as t(^c^)",
        "List of column aliases must have same degree as table; table has 2 "
        + "columns \\('DEPTNO', 'ORDINALITY'\\), "
        + "whereas alias list has 1 columns");
    check("select c from unnest(\n"
        + "  array(select deptno from dept)) with ordinality as t(c, d)");
    checkFails("select c from unnest(\n"
        + "  array(select deptno from dept)) with ordinality as t(^c, d, e^)",
        "List of column aliases must have same degree as table; table has 2 "
        + "columns \\('DEPTNO', 'ORDINALITY'\\), "
        + "whereas alias list has 3 columns");
    checkFails("select c\n"
        + "from unnest(array(select * from dept)) with ordinality as t(^c, d, e, f^)",
        "List of column aliases must have same degree as table; table has 3 "
        + "columns \\('DEPTNO', 'NAME', 'ORDINALITY'\\), "
        + "whereas alias list has 4 columns");
    checkFails(
        "select ^name^ from unnest(array(select name from dept)) with ordinality as t(c, o)",
        "Column 'NAME' not found in any table");
    checkFails(
        "select ^ordinality^ from unnest(array(select name from dept)) with ordinality as t(c, o)",
        "Column 'ORDINALITY' not found in any table");
  }

  @Test public void unnestMapMustNameColumnsKeyAndValueWhenNotAliased() {
    checkResultType("select * from unnest(map[1, 12, 2, 22])",
        "RecordType(INTEGER NOT NULL KEY, INTEGER NOT NULL VALUE) NOT NULL");
  }

  @Test public void testCorrelationJoin() {
    check("select *,"
        + "         multiset(select * from emp where deptno=dept.deptno) "
        + "               as empset"
        + "      from dept");
    check("select*from unnest(select multiset[8] from dept)");
    check("select*from unnest(select multiset[deptno] from dept)");
  }

  @Test public void testStructuredTypes() {
    checkColumnType(
        "values new address()",
        "ObjectSqlType(ADDRESS) NOT NULL");
    checkColumnType("select home_address from emp_address",
        "ObjectSqlType(ADDRESS) NOT NULL");
    checkColumnType("select ea.home_address.zip from emp_address ea",
        "INTEGER NOT NULL");
    checkColumnType("select ea.mailing_address.city from emp_address ea",
        "VARCHAR(20) NOT NULL");
  }

  @Test public void testLateral() {
    checkFails(
        "select * from emp, (select * from dept where ^emp^.deptno=dept.deptno)",
        "Table 'EMP' not found");

    check("select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno)");
    check("select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno) as ldt");
    check("select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno) ldt");
  }

  @Test public void testCollect() {
    check("select collect(deptno) from emp");
    check("select collect(multiset[3]) from emp");
    sql("select collect(multiset[3]), ^deptno^ from emp")
        .fails("Expression 'DEPTNO' is not being grouped");
  }

  @Test public void testFusion() {
    checkFails("select ^fusion(deptno)^ from emp",
        "(?s).*Cannot apply 'FUSION' to arguments of type 'FUSION.<INTEGER>.'.*");
    check("select fusion(multiset[3]) from emp");
    // todo. FUSION is an aggregate function. test that validator can only
    // take set operators in its select list once aggregation support is
    // complete
  }

  @Test public void testCountFunction() {
    check("select count(*) from emp");
    check("select count(ename) from emp");
    check("select count(sal) from emp");
    check("select count(1) from emp");
    checkFails(
        "select ^count()^ from emp",
        "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments");
  }

  @Test public void testCountCompositeFunction() {
    check("select count(ename, deptno) from emp");
    checkFails("select count(ename, deptno, ^gender^) from emp",
        "Column 'GENDER' not found in any table");
    check("select count(ename, 1, deptno) from emp");
    check("select count(distinct ename, 1, deptno) from emp");
    checkFails("select count(deptno, *) from emp",
        "(?s).*Encountered \", \\*\" at .*");
    checkFails(
        "select count(*, deptno) from emp",
        "(?s).*Encountered \",\" at .*");
  }

  @Test public void testLastFunction() {
    check("select LAST_VALUE(sal) over (order by empno) from emp");
    check("select LAST_VALUE(ename) over (order by empno) from emp");

    check("select FIRST_VALUE(sal) over (order by empno) from emp");
    check("select FIRST_VALUE(ename) over (order by empno) from emp");

    check("select NTH_VALUE(sal, 2) over (order by empno) from emp");
    check("select NTH_VALUE(ename, 2) over (order by empno) from emp");
  }

  @Test void testMinMaxFunctions() {
    sql("SELECT MIN(true) from emp").ok();
    sql("SELECT MAX(false) from emp").ok();

    sql("SELECT MIN(sal+deptno) FROM emp").ok();
    sql("SELECT MAX(ename) FROM emp").ok();
    sql("SELECT MIN(5.5) FROM emp").ok();
    sql("SELECT MAX(5) FROM emp").ok();
  }

  @Test void testSomeEveryAndIntersectionFunctions() {
    sql("select some(sal = 100), every(sal > 0), intersection(multiset[1,2]) from emp").ok();
    sql("select some(sal = 100), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select every(sal > 0), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("select intersection(multiset[1]), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  @Test void testAnyValueFunction() {
    sql("SELECT any_value(ename) from emp").ok();
  }

  @Test void testFunctionalDistinct() {
    sql("select count(distinct sal) from emp").ok();
    sql("select COALESCE(^distinct^ sal) from emp")
        .fails("DISTINCT/ALL not allowed with COALESCE function");
  }

  @Test void testColumnNotFound() {
    sql("select ^b0^ from sales.emp")
        .fails("Column 'B0' not found in any table");
  }

  @Test void testColumnNotFound2() {
    sql("select ^b0^ from sales.emp, sales.dept")
        .fails("Column 'B0' not found in any table");
  }

  @Test void testColumnNotFound3() {
    sql("select e.^b0^ from sales.emp as e")
        .fails("Column 'B0' not found in table 'E'");
  }

  @Test void testSelectDistinct() {
    sql("SELECT DISTINCT deptno FROM emp").ok();
    sql("SELECT DISTINCT deptno, sal FROM emp").ok();
    sql("SELECT DISTINCT deptno FROM emp GROUP BY deptno").ok();
    sql("SELECT DISTINCT ^deptno^ FROM emp GROUP BY sal")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT avg(sal) from emp").ok();
    sql("SELECT DISTINCT ^deptno^, avg(sal) from emp")
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT deptno, sal from emp GROUP BY sal, deptno").ok();
    sql("SELECT deptno FROM emp GROUP BY deptno HAVING deptno > 55").ok();
    sql("SELECT DISTINCT deptno, 33 FROM emp\n"
        + "GROUP BY deptno HAVING deptno > 55").ok();
    sql("SELECT DISTINCT deptno, 33 FROM emp HAVING ^deptno^ > 55")
        .fails("Expression 'DEPTNO' is not being grouped");
    // same query under a different conformance finds a different error first
    sql("SELECT DISTINCT ^deptno^, 33 FROM emp HAVING deptno > 55")
        .withConformance(SqlConformanceEnum.LENIENT)
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT 33 FROM emp HAVING ^deptno^ > 55")
        .fails("Expression 'DEPTNO' is not being grouped")
        .withConformance(SqlConformanceEnum.LENIENT)
        .fails("Expression 'DEPTNO' is not being grouped");
    sql("SELECT DISTINCT * from emp").ok();
    sql("SELECT DISTINCT ^*^ from emp GROUP BY deptno")
        .fails("Expression 'EMP\\.EMPNO' is not being grouped");

    // similar validation for SELECT DISTINCT and GROUP BY
    sql("SELECT deptno FROM emp GROUP BY deptno ORDER BY deptno, ^empno^")
        .fails("Expression 'EMPNO' is not being grouped");
    sql("SELECT DISTINCT deptno from emp ORDER BY deptno, ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");
    sql("SELECT DISTINCT deptno from emp ORDER BY deptno + 2").ok();

    // The ORDER BY clause works on what is projected by DISTINCT - even if
    // GROUP BY is present.
    sql("SELECT DISTINCT deptno FROM emp GROUP BY deptno, empno ORDER BY deptno, ^empno^")
        .fails("Expression 'EMPNO' is not in the select clause");

    // redundant distinct; same query is in unitsql/optimizer/distinct.sql
    sql("select distinct * from (\n"
        + "  select distinct deptno from emp) order by 1").ok();

    sql("SELECT DISTINCT 5, 10+5, 'string' from emp").ok();
  }

  @Test void testSelectWithoutFrom() {
    sql("^select 2+2^")
        .withConformance(SqlConformanceEnum.DEFAULT)
        .ok();
    sql("^select 2+2^")
        .withConformance(SqlConformanceEnum.ORACLE_10)
        .fails("SELECT must have a FROM clause");
    sql("^select 2+2^")
        .withConformance(SqlConformanceEnum.STRICT_2003)
        .fails("SELECT must have a FROM clause");
  }

  @Test public void testTableExtend() {
    checkResultType("select * from dept extend (x int not null)",
        "RecordType(INTEGER NOT NULL DEPTNO, VARCHAR(10) NOT NULL NAME, INTEGER NOT NULL X) NOT NULL");
    checkResultType("select deptno + x as z\n"
            + "from dept extend (x int not null) as x\n"
            + "where x > 10",
        "RecordType(INTEGER NOT NULL Z) NOT NULL");
  }

  @Test void testTableExtend() {
    sql("select * from dept extend (x int not null)")
        .type("RecordType(INTEGER NOT NULL DEPTNO, VARCHAR(10) NOT NULL NAME, "
            + "INTEGER NOT NULL X) NOT NULL");
    sql("select deptno + x as z\n"
        + "from dept extend (x int not null) as x\n"
        + "where x > 10")
        .type("RecordType(INTEGER NOT NULL Z) NOT NULL");
  }

  @Test void testExplicitTable() {
    final String empRecordType =
        "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL";
    checkResultType("select * from (table emp)", empRecordType);
    checkResultType("table emp", empRecordType);
    checkFails("table ^nonexistent^", "Object 'NONEXISTENT' not found");
    checkFails("table ^sales.nonexistent^",
        "Object 'NONEXISTENT' not found within 'SALES'");
    checkFails("table ^nonexistent.foo^", "Object 'NONEXISTENT' not found");
  }

  @Test void testCollectionTable() {
    sql("select * from table(ramp(3))")
        .type("RecordType(INTEGER NOT NULL I) NOT NULL");

    sql("select * from table(^ramp('3')^)")
        .withTypeCoercion(false)
        .fails("Cannot apply 'RAMP' to arguments of type "
            + "'RAMP\\(<CHAR\\(1\\)>\\)'\\. "
            + "Supported form\\(s\\): 'RAMP\\(<NUMERIC>\\)'");

    sql("select * from table(ramp('3'))")
        .type("RecordType(INTEGER NOT NULL I) NOT NULL");

    sql("select * from table(^nonExistentRamp('3')^)")
        .fails("No match found for function signature NONEXISTENTRAMP\\(<CHARACTER>\\)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1309">[CALCITE-1309]
   * Support LATERAL TABLE</a>. */
  @Test void testCollectionTableWithLateral() {
    final String expectedType = "RecordType(INTEGER NOT NULL DEPTNO, "
        + "VARCHAR(10) NOT NULL NAME, "
        + "INTEGER NOT NULL I) NOT NULL";
    sql("select * from dept, lateral table(ramp(dept.deptno))")
        .type(expectedType);
    sql("select * from dept cross join lateral table(ramp(dept.deptno))")
        .type(expectedType);
    sql("select * from dept join lateral table(ramp(dept.deptno)) on true")
        .type(expectedType);

    final String expectedType2 = "RecordType(INTEGER NOT NULL DEPTNO, "
        + "VARCHAR(10) NOT NULL NAME, "
        + "INTEGER I) NOT NULL";
    sql("select * from dept left join lateral table(ramp(dept.deptno)) on true")
        .type(expectedType2);

    sql("select * from dept, lateral table(^ramp(dept.name)^)")
        .withTypeCoercion(false)
        .fails("(?s)Cannot apply 'RAMP' to arguments of type 'RAMP\\(<VARCHAR\\(10\\)>\\)'.*");

    sql("select * from dept, lateral table(ramp(dept.name))")
        .type("RecordType(INTEGER NOT NULL DEPTNO, "
            + "VARCHAR(10) NOT NULL NAME, "
            + "INTEGER NOT NULL I) NOT NULL");

    sql("select * from lateral table(ramp(^dept^.deptno)), dept")
        .fails("Table 'DEPT' not found");
    final String expectedType3 = "RecordType(INTEGER NOT NULL I, "
        + "INTEGER NOT NULL DEPTNO, "
        + "VARCHAR(10) NOT NULL NAME) NOT NULL";
    sql("select * from lateral table(ramp(1234)), dept")
        .type(expectedType3);
  }

  @Test void testCollectionTableWithLateral2() {
    // The expression inside the LATERAL can only see tables before it in the
    // FROM clause. And it can't see itself.
    sql("select * from emp, lateral table(ramp(emp.deptno)), dept")
        .ok();
    sql("select * from emp, lateral table(ramp(^z^.i)) as z, dept")
        .fails("Table 'Z' not found");
    sql("select * from emp, lateral table(ramp(^dept^.deptno)), dept")
        .fails("Table 'DEPT' not found");
  }

  @Test public void testCollectionTableWithCursorParam() {
    checkResultType(
        "select * from table(dedup(cursor(select * from emp),'ename'))",
        "RecordType(VARCHAR(1024) NOT NULL NAME) NOT NULL");
    checkFails(
        "select * from table(dedup(cursor(select * from ^bloop^),'ename'))",
        "Object 'BLOOP' not found");
  }

  @Test void testTemporalTable() {
    sql("select stream * from orders, ^products^ for system_time as of"
        + " TIMESTAMP '2011-01-02 00:00:00'")
        .fails("Table 'PRODUCTS' is not a temporal table, "
            + "can not be queried in system time period specification");

    sql("select stream * from orders, products_temporal "
        + "for system_time as of ^'2011-01-02 00:00:00'^")
        .fails("The system time period specification expects Timestamp type but is 'CHAR'");

    // verify inner join with a specific timestamp
    sql("select stream * from orders join products_temporal "
        + "for system_time as of timestamp '2011-01-02 00:00:00' "
        + "on orders.productid = products_temporal.productid").ok();

    // verify left join with a timestamp expression
    sql("select stream * from orders left join products_temporal "
        + "for system_time as of orders.rowtime "
        + "on orders.productid = products_temporal.productid").ok();

    // verify left join with a timestamp expression
    sql("select stream * from orders left join products_temporal\n"
        + "for system_time as of orders.rowtime - INTERVAL '3' DAY\n"
        + "on orders.productid = products_temporal.productid").ok();

    // verify left join with a datetime value function
    sql("select stream * from orders left join products_temporal\n"
        + "for system_time as of CURRENT_TIMESTAMP\n"
        + "on orders.productid = products_temporal.productid").ok();
  }

  @Test void testScalarSubQuery() {
    sql("SELECT  ename,(select name from dept where deptno=1) FROM emp").ok();
    sql("SELECT ename,(^select losal, hisal from salgrade where grade=1^) FROM emp")
        .fails("Cannot apply '\\$SCALAR_QUERY' to arguments of type "
            + "'\\$SCALAR_QUERY\\(<RECORDTYPE\\(INTEGER LOSAL, "
            + "INTEGER HISAL\\)>\\)'\\. Supported form\\(s\\): "
            + "'\\$SCALAR_QUERY\\(<RECORDTYPE\\(SINGLE FIELD\\)>\\)'");

    // Note that X is a field (not a record) and is nullable even though
    // EMP.NAME is NOT NULL.
    checkResultType(
        "SELECT  ename,(select name from dept where deptno=1) FROM emp",
        "RecordType(VARCHAR(20) NOT NULL ENAME, VARCHAR(10) EXPR$1) NOT NULL");

    // scalar subqery inside AS operator
    checkResultType(
        "SELECT  ename,(select name from dept where deptno=1) as X FROM emp",
        "RecordType(VARCHAR(20) NOT NULL ENAME, VARCHAR(10) X) NOT NULL");

    // scalar subqery inside + operator
    checkResultType(
        "SELECT  ename, 1 + (select deptno from dept where deptno=1) as X FROM emp",
        "RecordType(VARCHAR(20) NOT NULL ENAME, INTEGER X) NOT NULL");

    // scalar sub-query inside WHERE
    sql("select * from emp where (select true from dept)").ok();
  }

  @Disabled("not supported")
  @Test void testSubQueryInOnClause() {
    // Currently not supported. Should give validator error, but gives
    // internal error.
    sql("select * from emp as emps left outer join dept as depts\n"
        + "on emps.deptno = depts.deptno and emps.deptno = (\n"
        + "select min(deptno) from dept as depts2)").ok();
  }

  @Test void testRecordType() {
    // Have to qualify columns with table name.
    sql("SELECT ^coord^.x, coord.y FROM customer.contact")
        .fails("Table 'COORD' not found");

    sql("SELECT contact.coord.x, contact.coord.y FROM customer.contact")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");

    // Qualifying with schema is OK.
    checkResultType(
        "SELECT customer.contact.coord.x, customer.contact.email, contact.coord.y FROM customer.contact",
        "RecordType(INTEGER NOT NULL X, VARCHAR(20) NOT NULL EMAIL, INTEGER NOT NULL Y) NOT NULL");
  }

  @Test void testArrayOfRecordType() {
    sql("SELECT name, dept_nested.employees[1].^ne^ as ne from dept_nested")
        .fails("Unknown field 'NE'");
    sql("SELECT name, dept_nested.employees[1].ename as ename from dept_nested")
        .type("RecordType(VARCHAR(10) NOT NULL NAME, VARCHAR(10) ENAME) NOT NULL");
    sql("SELECT dept_nested.employees[1].detail.skills[1].desc as DESCRIPTION\n"
        + "from dept_nested")
        .type("RecordType(VARCHAR(20) DESCRIPTION) NOT NULL");
    sql("SELECT dept_nested.employees[1].detail.skills[1].others.a as oa\n"
        + "from dept_nested")
        .type("RecordType(VARCHAR(10) OA) NOT NULL");
  }

  @Test void testItemOperatorException() {
    sql("select ^name[0]^ from dept")
        .fails("Cannot apply 'ITEM' to arguments of type 'ITEM\\(<VARCHAR\\(10\\)>, "
            +  "<INTEGER>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<VALUE>\\]\n"
            + "<VARIANT>\\[<ANY>\\].*");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-497">[CALCITE-497]
   * Support optional qualifier for column name references</a>. */
  @Test void testRecordTypeElided() {
    sql("SELECT contact.^x^, contact.coord.y FROM customer.contact")
        .fails("Column 'X' not found in table 'CONTACT'");

    // Fully qualified works.
    sql("SELECT contact.coord.x, contact.coord.y FROM customer.contact")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");

    // Because the types of CONTACT_PEEK.COORD and CONTACT_PEEK.COORD_NE are
    // marked "peek", the validator can see through them.
    sql("SELECT c.x, c.coord.y, c.m, c.b FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y, INTEGER NOT NULL M,"
            + " INTEGER NOT NULL B) NOT NULL");
    sql("SELECT c.coord.x, c.coord.y, c.coord_ne.m FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y, INTEGER NOT NULL M) NOT NULL");
    sql("SELECT x, a, c.coord.y FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL X, INTEGER NOT NULL A, INTEGER NOT NULL Y) NOT NULL");

    // Because CONTACT_PEEK.COORD_NE is marked "peek no expand",
    // "select *" does not flatten it.
    sql("SELECT coord_ne.* FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL M, "
            + "RecordType:peek_no_expand(INTEGER NOT NULL A, INTEGER NOT NULL B) "
            + "NOT NULL SUB) NOT NULL");
    sql("SELECT * FROM customer.contact_peek as c")
        .type("RecordType(INTEGER NOT NULL CONTACTNO, VARCHAR(10) NOT NULL FNAME, "
            + "VARCHAR(10) NOT NULL LNAME, VARCHAR(20) NOT NULL EMAIL, INTEGER NOT NULL X, "
            + "INTEGER NOT NULL Y, RecordType:peek_no_expand(INTEGER NOT NULL M, "
            + "RecordType:peek_no_expand(INTEGER NOT NULL A, INTEGER NOT NULL B) "
            + "NOT NULL SUB) NOT NULL COORD_NE) NOT NULL");

    // Qualifying with schema is OK.
    final String sql = "SELECT customer.contact_peek.x,\n"
        + " customer.contact_peek.email, contact_peek.coord.y\n"
        + "FROM customer.contact_peek";
    sql(sql).type("RecordType(INTEGER NOT NULL X, VARCHAR(20) NOT NULL EMAIL,"
        + " INTEGER NOT NULL Y) NOT NULL");
  }

  @Test void testSample() {
    // applied to table
    sql("SELECT * FROM emp TABLESAMPLE SUBSTITUTE('foo')").ok();
    sql("SELECT * FROM emp TABLESAMPLE BERNOULLI(50)").ok();
    sql("SELECT * FROM emp TABLESAMPLE SYSTEM(50)").ok();

    // applied to query
    sql("SELECT * FROM ("
        + "SELECT deptno FROM emp "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SUBSTITUTE('foo') "
        + "WHERE x.deptno < 100").ok();

    sql("SELECT x.^empno^ FROM ("
        + "SELECT deptno FROM emp TABLESAMPLE SUBSTITUTE('bar') "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SUBSTITUTE('foo') "
        + "ORDER BY 1")
        .fails("Column 'EMPNO' not found in table 'X'");

    sql("select * from (\n"
        + "    select * from emp\n"
        + "    join dept on emp.deptno = dept.deptno\n"
        + ") tablesample substitute('SMALL')").ok();

    sql("SELECT * FROM ("
        + "SELECT deptno FROM emp "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE BERNOULLI(50) "
        + "WHERE x.deptno < 100").ok();

    sql("SELECT x.^empno^ FROM ("
        + "SELECT deptno FROM emp TABLESAMPLE BERNOULLI(50) "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE BERNOULLI(10) "
        + "ORDER BY 1")
        .fails("Column 'EMPNO' not found in table 'X'");

    sql("select * from (\n"
        + "    select * from emp\n"
        + "    join dept on emp.deptno = dept.deptno\n"
        + ") tablesample bernoulli(10)").ok();

    sql("SELECT * FROM ("
        + "SELECT deptno FROM emp "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SYSTEM(50) "
        + "WHERE x.deptno < 100").ok();

    sql("SELECT x.^empno^ FROM ("
        + "SELECT deptno FROM emp TABLESAMPLE SYSTEM(50) "
        + "UNION ALL "
        + "SELECT deptno FROM dept) AS x TABLESAMPLE SYSTEM(10) "
        + "ORDER BY 1")
        .fails("Column 'EMPNO' not found in table 'X'");

    sql("select * from (\n"
        + "    select * from emp\n"
        + "    join dept on emp.deptno = dept.deptno\n"
        + ") tablesample system(10)").ok();
  }

  @Test void testRewriteWithoutIdentifierExpansion() {
    sql("select * from dept")
        .withValidatorIdentifierExpansion(false)
        .rewritesTo("SELECT *\n"
            + "FROM `DEPT`");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1238">[CALCITE-1238]
   * Unparsing LIMIT without ORDER BY after validation</a>. */
  @Test void testRewriteWithLimitWithoutOrderBy() {
    final String sql = "select name from dept limit 2";
    final String expected = "SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithLimitWithDynamicParameters() {
    final String sql = "select name from dept offset ? rows fetch next ? rows only";
    final String expected = "SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "OFFSET ? ROWS\n"
        + "FETCH NEXT ? ROWS ONLY";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithOffsetWithoutOrderBy() {
    final String sql = "select name from dept offset 2";
    final String expected = "SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "OFFSET 2 ROWS";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithUnionFetchWithoutOrderBy() {
    final String sql =
        "select name from dept union all select name from dept limit 2";
    final String expected = "SELECT *\n"
        + "FROM (SELECT `NAME`\n"
        + "FROM `DEPT`\n"
        + "UNION ALL\n"
        + "SELECT `NAME`\n"
        + "FROM `DEPT`)\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql(sql)
        .withValidatorIdentifierExpansion(false)
        .rewritesTo(expected);
  }

  @Test void testRewriteWithIdentifierExpansion() {
    sql("select * from dept")
        .withValidatorIdentifierExpansion(true)
        .rewritesTo("SELECT `DEPT`.`DEPTNO`, `DEPT`.`NAME`\n"
            + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`");
  }

  @Test void testRewriteWithColumnReferenceExpansion() {
    // The names in the ORDER BY clause are not qualified.
    // This is because ORDER BY references columns in the SELECT clause
    // in preference to columns in tables in the FROM clause.

    sql("select name from dept where name = 'Moonracer' group by name"
        + " having sum(deptno) > 3 order by name")
        .withValidatorIdentifierExpansion(true)
        .withValidatorColumnReferenceExpansion(true)
        .rewritesTo("SELECT `DEPT`.`NAME`\n"
            + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`\n"
            + "WHERE `DEPT`.`NAME` = 'Moonracer'\n"
            + "GROUP BY `DEPT`.`NAME`\n"
            + "HAVING SUM(`DEPT`.`DEPTNO`) > 3\n"
            + "ORDER BY `NAME`");
  }

  @Test void testRewriteWithColumnReferenceExpansionAndFromAlias() {
    // In the ORDER BY clause, 'ename' is not qualified but 'deptno' and 'sal'
    // are. This is because 'ename' appears as an alias in the SELECT clause.
    // 'sal' is qualified in the ORDER BY clause, so remains qualified.

    final String sql = "select ename, sal from (select * from emp) as e"
        + " where ename = 'Moonracer' group by ename, deptno, sal"
        + " having sum(deptno) > 3 order by ename, deptno, e.sal";
    final String expected = "SELECT `E`.`ENAME`, `E`.`SAL`\n"
        + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`,"
        + " `EMP`.`MGR`, `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`,"
        + " `EMP`.`DEPTNO`, `EMP`.`SLACKER`\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`) AS `E`\n"
        + "WHERE `E`.`ENAME` = 'Moonracer'\n"
        + "GROUP BY `E`.`ENAME`, `E`.`DEPTNO`, `E`.`SAL`\n"
        + "HAVING SUM(`E`.`DEPTNO`) > 3\n"
        + "ORDER BY `ENAME`, `E`.`DEPTNO`, `E`.`SAL`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .withValidatorColumnReferenceExpansion(true)
        .rewritesTo(expected);
  }

  @Test void testCoalesceWithoutRewrite() {
    final String sql = "select coalesce(deptno, empno) from emp";
    final String expected1 = "SELECT COALESCE(`EMP`.`DEPTNO`, `EMP`.`EMPNO`)\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`";
    final String expected2 = "SELECT COALESCE(`DEPTNO`, `EMPNO`)\n"
        + "FROM `EMP`";
    sql(sql)
        .withValidatorCallRewrite(false)
        .rewritesTo(tester.getValidator().shouldExpandIdentifiers()
            ? expected1 : expected2);
  }

  @Test void testCoalesceWithRewrite() {
    final String sql = "select coalesce(deptno, empno) from emp";
    final String expected1 = "SELECT CASE WHEN `EMP`.`DEPTNO` IS NOT NULL"
        + " THEN `EMP`.`DEPTNO` ELSE `EMP`.`EMPNO` END\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`";
    final String expected2 = "SELECT CASE WHEN `DEPTNO` IS NOT NULL"
        + " THEN `DEPTNO` ELSE `EMPNO` END\n"
        + "FROM `EMP`";
    sql(sql)
        .withValidatorCallRewrite(true)
        .rewritesTo(tester.getValidator().shouldExpandIdentifiers()
            ? expected1 : expected2);
  }

  @Disabled
  @Test void testValuesWithAggFuncs() {
    sql("values(^count(1)^)")
        .fails("Call to xxx is invalid\\. Direct calls to aggregate "
            + "functions not allowed in ROW definitions\\.");
  }

  @Test void testFieldOrigin() {
    tester.checkFieldOrigin("select * from emp join dept on true",
        "{CATALOG.SALES.EMP.EMPNO,"
            + " CATALOG.SALES.EMP.ENAME,"
            + " CATALOG.SALES.EMP.JOB,"
            + " CATALOG.SALES.EMP.MGR,"
            + " CATALOG.SALES.EMP.HIREDATE,"
            + " CATALOG.SALES.EMP.SAL,"
            + " CATALOG.SALES.EMP.COMM,"
            + " CATALOG.SALES.EMP.DEPTNO,"
            + " CATALOG.SALES.EMP.SLACKER,"
            + " CATALOG.SALES.DEPT.DEPTNO,"
            + " CATALOG.SALES.DEPT.NAME}");

    tester.checkFieldOrigin("select distinct emp.empno, hiredate, 1 as uno,\n"
            + " emp.empno * 2 as twiceEmpno\n"
            + "from emp join dept on true",
        "{CATALOG.SALES.EMP.EMPNO,"
            + " CATALOG.SALES.EMP.HIREDATE,"
            + " null,"
            + " null}");
  }

  @Test void testBrackets() {
    final Sql s = sql("?").withQuoting(Quoting.BRACKET);
    s.sql("select [e].EMPNO from [EMP] as [e]")
        .type("RecordType(INTEGER NOT NULL EMPNO) NOT NULL");

    s.sql("select ^e^.EMPNO from [EMP] as [e]")
        .fails("Table 'E' not found; did you mean 'e'\\?");

    s.sql("select ^x^ from (\n"
        + "  select [e].EMPNO as [x] from [EMP] as [e])")
        .fails("Column 'X' not found in any table; did you mean 'x'\\?");

    s.sql("select ^x^ from (\n"
        + "  select [e].EMPNO as [x ] from [EMP] as [e])")
        .fails("Column 'X' not found in any table");

    s.sql("select EMP.^\"x\"^ from EMP")
        .fails("(?s).*Encountered \"\\. \\\\\"\" at line .*");

    s.sql("select [x[y]] z ] from (\n"
        + "  select [e].EMPNO as [x[y]] z ] from [EMP] as [e])").type(
        "RecordType(INTEGER NOT NULL x[y] z ) NOT NULL");
  }

  @Test void testLexJava() {
    final Sql s = sql("?").withLex(Lex.JAVA);
    s.sql("select e.EMPNO from EMP as e")
        .type("RecordType(INTEGER NOT NULL EMPNO) NOT NULL");

    s.sql("select ^e^.EMPNO from EMP as E")
        .fails("Table 'e' not found; did you mean 'E'\\?");

    s.sql("select ^E^.EMPNO from EMP as e")
        .fails("Table 'E' not found; did you mean 'e'\\?");

    s.sql("select ^x^ from (\n"
        + "  select e.EMPNO as X from EMP as e)")
        .fails("Column 'x' not found in any table; did you mean 'X'\\?");

    s.sql("select ^x^ from (\n"
        + "  select e.EMPNO as Xx from EMP as e)")
        .fails("Column 'x' not found in any table");

    // double-quotes are not valid in this lexical convention
    s.sql("select EMP.^\"x\"^ from EMP")
        .fails("(?s).*Encountered \"\\. \\\\\"\" at line .*");

    // in Java mode, creating identifiers with spaces is not encouraged, but you
    // can use back-ticks if you really have to
    s.sql("select `x[y] z ` from (\n"
        + "  select e.EMPNO as `x[y] z ` from EMP as e)")
        .type("RecordType(INTEGER NOT NULL x[y] z ) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-145">[CALCITE-145]
   * Unexpected upper-casing of keywords when using java lexer</a>. */
  @Test void testLexJavaKeyword() {
    final Sql s = sql("?").withLex(Lex.JAVA);
    s.sql("select path, x from (select 1 as path, 2 as x from (values (true)))")
        .type("RecordType(INTEGER NOT NULL path, INTEGER NOT NULL x) NOT NULL");
    s.sql("select path, x from (select 1 as `path`, 2 as x from (values (true)))")
        .type("RecordType(INTEGER NOT NULL path, INTEGER NOT NULL x) NOT NULL");
    s.sql("select `path`, x from (select 1 as path, 2 as x from (values (true)))")
        .type("RecordType(INTEGER NOT NULL path, INTEGER NOT NULL x) NOT NULL");
    s.sql("select ^PATH^ from (select 1 as path from (values (true)))")
        .fails("Column 'PATH' not found in any table; did you mean 'path'\\?");
    s.sql("select t.^PATH^ from (select 1 as path from (values (true))) as t")
        .fails("Column 'PATH' not found in table 't'; did you mean 'path'\\?");
    s.sql("select t.x, t.^PATH^ from (values (true, 1)) as t(path, x)")
        .fails("Column 'PATH' not found in table 't'; did you mean 'path'\\?");

    // Built-in functions can be written in any case, even those with no args,
    // and regardless of spaces between function name and open parenthesis.
    s.sql("values (current_timestamp, floor(2.5), ceil (3.5))").ok();
    s.sql("values (CURRENT_TIMESTAMP, FLOOR(2.5), CEIL (3.5))").ok();
    s.sql("values (CURRENT_TIMESTAMP, CEIL (3.5))")
        .type("RecordType(TIMESTAMP(0) NOT NULL CURRENT_TIMESTAMP, "
            + "DECIMAL(2, 0) NOT NULL EXPR$1) NOT NULL");
  }

  @Test void testLexAndQuoting() {
    // in Java mode, creating identifiers with spaces is not encouraged, but you
    // can use double-quote if you really have to
    sql("?")
        .withLex(Lex.JAVA)
        .withQuoting(Quoting.DOUBLE_QUOTE)
        .sql("select \"x[y] z \" from (\n"
            + "  select e.EMPNO as \"x[y] z \" from EMP as e)")
        .type("RecordType(INTEGER NOT NULL x[y] z ) NOT NULL");
  }

  /** Tests using case-insensitive matching of identifiers. */
  @Test void testCaseInsensitive() {
    final Sql s = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final Sql sensitive = sql("?")
        .withQuoting(Quoting.BRACKET);

    s.sql("select EMPNO from EMP").ok();
    s.sql("select empno from emp").ok();
    s.sql("select [empno] from [emp]").ok();
    s.sql("select [E].[empno] from [emp] as e").ok();
    s.sql("select t.[x] from (\n"
        + "  select [E].[empno] as x from [emp] as e) as [t]").ok();

    // correlating variable
    s.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = [E].deptno)").ok();
    sensitive.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = ^[E]^.deptno)")
        .fails("(?s).*Table 'E' not found; did you mean 'e'\\?");

    sql("select count(1), ^empno^ from emp")
        .fails("Expression 'EMPNO' is not being grouped");
  }

  /** Tests using case-insensitive matching of user-defined functions. */
  @Test void testCaseInsensitiveUdfs() {
    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);
    final Sql insensitive = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET)
        .withOperatorTable(operatorTable);
    final Sql sensitive = sql("?")
        .withQuoting(Quoting.BRACKET)
        .withOperatorTable(operatorTable);

    // test table function lookup case-insensitively.
    insensitive.sql("select * from dept, lateral table(ramp(dept.deptno))").ok();
    insensitive.sql("select * from dept, lateral table(RAMP(dept.deptno))").ok();
    insensitive.sql("select * from dept, lateral table([RAMP](dept.deptno))").ok();
    insensitive.sql("select * from dept, lateral table([Ramp](dept.deptno))").ok();
    // test scalar function lookup case-insensitively.
    insensitive.sql("select myfun(EMPNO) from EMP").ok();
    insensitive.sql("select MYFUN(empno) from emp").ok();
    insensitive.sql("select [MYFUN]([empno]) from [emp]").ok();
    insensitive.sql("select [Myfun]([E].[empno]) from [emp] as e").ok();
    insensitive.sql("select t.[x] from (\n"
        + "  select [Myfun]([E].[empno]) as x from [emp] as e) as [t]").ok();

    // correlating variable
    insensitive.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = myfun([E].deptno))").ok();
    sensitive.sql("select * from emp as [e] where exists (\n"
        + "select 1 from dept where dept.deptno = ^[myfun]([e].deptno)^)")
        .fails("No match found for function signature myfun\\(<NUMERIC>\\).*");
  }

  /** Tests using case-sensitive matching of builtin functions. */
  @Test void testCaseSensitiveBuiltinFunction() {
    final Sql sensitive = sql("?")
        .withCaseSensitive(true)
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BRACKET)
        .withOperatorTable(SqlStdOperatorTable.instance());

    sensitive.sql("select sum(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [sum](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [SUM](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select SUM(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select Sum(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select count(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [count](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select [COUNT](EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select COUNT(EMPNO) from EMP group by ENAME, EMPNO").ok();
    sensitive.sql("select Count(EMPNO) from EMP group by ENAME, EMPNO").ok();
  }

  /** Tests using case-insensitive matching of user-defined functions. */
  @Test public void testCaseInsensitiveUdfs() {
    final SqlTester tester1 = tester
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);
    tester1.withOperatorTable(operatorTable);
    final SqlTester tester2 = tester.withQuoting(Quoting.BRACKET);
    tester2.withOperatorTable(operatorTable);

    // test table function lookup case-insensitively.
    tester1.checkQuery("select * from dept, lateral table(ramp(dept.deptno))");
    tester1.checkQuery("select * from dept, lateral table(RAMP(dept.deptno))");
    tester1.checkQuery("select * from dept, lateral table([RAMP](dept.deptno))");
    tester1.checkQuery("select * from dept, lateral table([Ramp](dept.deptno))");
    // test scalar function lookup case-insensitively.
    tester1.checkQuery("select myfun(EMPNO) from EMP");
    tester1.checkQuery("select MYFUN(empno) from emp");
    tester1.checkQuery("select [MYFUN]([empno]) from [emp]");
    tester1.checkQuery("select [Myfun]([E].[empno]) from [emp] as e");
    tester1.checkQuery("select t.[x] from (\n"
        + "  select [Myfun]([E].[empno]) as x from [emp] as e) as [t]");

    // correlating variable
    tester1.checkQuery(
        "select * from emp as [e] where exists (\n"
            + "select 1 from dept where dept.deptno = myfun([E].deptno))");
    tester2.checkQueryFails(
        "select * from emp as [e] where exists (\n"
            + "select 1 from dept where dept.deptno = ^[myfun]([e].deptno)^)",
        "No match found for function signature myfun\\(<NUMERIC>\\).*");
  }

  /** Tests using case-sensitive matching of builtin functions. */
  @Test public void testCaseSensitiveBuiltinFunction() {
    final SqlTester tester1 = tester
        .withCaseSensitive(true)
        .withQuoting(Quoting.BRACKET);
    tester1.withOperatorTable(SqlStdOperatorTable.instance());

    tester1.checkQuery("select sum(empno) from EMP group by ename, empno");
    tester1.checkQuery("select [sum](empno) from EMP group by ename, empno");
    tester1.checkQuery("select [SUM](empno) from EMP group by ename, empno");
    tester1.checkQuery("select SUM(empno) from EMP group by ename, empno");
    tester1.checkQuery("select Sum(empno) from EMP group by ename, empno");
    tester1.checkQuery("select count(empno) from EMP group by ename, empno");
    tester1.checkQuery("select [count](empno) from EMP group by ename, empno");
    tester1.checkQuery("select [COUNT](empno) from EMP group by ename, empno");
    tester1.checkQuery("select COUNT(empno) from EMP group by ename, empno");
    tester1.checkQuery("select Count(empno) from EMP group by ename, empno");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-319">[CALCITE-319]
   * Table aliases should follow case-sensitivity policy</a>. */
  @Test void testCaseInsensitiveTableAlias() {
    final Sql s = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final Sql sensitive = sql("?").withQuoting(Quoting.BRACKET);

    // Table aliases should follow case-sensitivity preference.
    //
    // In MySQL, table aliases are case-insensitive:
    // mysql> select `D`.day from DAYS as `d`, DAYS as `D`;
    // ERROR 1066 (42000): Not unique table/alias: 'D'
    s.sql("select count(*) from dept as [D], ^dept as [d]^")
        .fails("Duplicate relation name 'd' in FROM clause");
    sensitive.sql("select count(*) from dept as [D], dept as [d]").ok();
    sensitive.sql("select count(*) from dept as [D], ^dept as [D]^")
        .fails("Duplicate relation name 'D' in FROM clause");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1305">[CALCITE-1305]
   * Case-insensitive table aliases and GROUP BY</a>. */
  @Test void testCaseInsensitiveTableAliasInGroupBy() {
    final Sql s = sql("?")
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED);
    s.sql("select deptno, count(*) from EMP AS emp\n"
        + "group by eMp.deptno").ok();
    s.sql("select deptno, count(*) from EMP AS EMP\n"
        + "group by eMp.deptno").ok();
    s.sql("select deptno, count(*) from EMP\n"
        + "group by eMp.deptno").ok();
    s.sql("select * from EMP where exists (\n"
        + "  select 1 from dept\n"
        + "  group by eMp.deptno)").ok();
    s.sql("select deptno, count(*) from EMP group by DEPTNO").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1549">[CALCITE-1549]
   * Improve error message when table or column not found</a>. */
  @Test void testTableNotFoundDidYouMean() {
    // No table in default schema
    tester.checkQueryFails("select * from ^unknownTable^",
        "Object 'UNKNOWNTABLE' not found");

    // Similar table exists in default schema
    tester.checkQueryFails("select * from ^\"Emp\"^",
        "Object 'Emp' not found within 'SALES'; did you mean 'EMP'\\?");

    // Schema correct, but no table in specified schema
    tester.checkQueryFails("select * from ^sales.unknownTable^",
        "Object 'UNKNOWNTABLE' not found within 'SALES'");
    // Similar table exists in specified schema
    tester.checkQueryFails("select * from ^sales.\"Emp\"^",
        "Object 'Emp' not found within 'SALES'; did you mean 'EMP'\\?");

    // No schema found
    tester.checkQueryFails("select * from ^unknownSchema.unknownTable^",
        "Object 'UNKNOWNSCHEMA' not found");
    // Similar schema found
    tester.checkQueryFails("select * from ^\"sales\".emp^",
        "Object 'sales' not found; did you mean 'SALES'\\?");
    tester.checkQueryFails("select * from ^\"saLes\".\"eMp\"^",
        "Object 'saLes' not found; did you mean 'SALES'\\?");

    // Spurious after table
    tester.checkQueryFails("select * from ^emp.foo^",
        "Object 'FOO' not found within 'SALES\\.EMP'");
    tester.checkQueryFails("select * from ^sales.emp.foo^",
        "Object 'FOO' not found within 'SALES\\.EMP'");

    // Alias not found
    sql("select ^aliAs^.\"name\"\n"
        + "from sales.emp as \"Alias\"")
        .fails("Table 'ALIAS' not found; did you mean 'Alias'\\?");
    // Alias not found, fully-qualified
    tester.checkQueryFails("select ^sales.\"emp\"^.\"name\" from sales.emp",
        "Table 'SALES\\.emp' not found; did you mean 'EMP'\\?");
  }

  @Test void testColumnNotFoundDidYouMean() {
    // Column not found
    sql("select ^\"unknownColumn\"^ from emp")
        .fails("Column 'unknownColumn' not found in any table");
    // Similar column in table, unqualified table name
    sql("select ^\"empNo\"^ from emp")
        .fails("Column 'empNo' not found in any table; did you mean 'EMPNO'\\?");
    // Similar column in table, table name qualified with schema
    sql("select ^\"empNo\"^ from sales.emp")
        .fails("Column 'empNo' not found in any table; did you mean 'EMPNO'\\?");
    // Similar column in table, table name qualified with catalog and schema
    sql("select ^\"empNo\"^ from catalog.sales.emp")
        .fails("Column 'empNo' not found in any table; did you mean 'EMPNO'\\?");
    // With table alias
    sql("select e.^\"empNo\"^ from catalog.sales.emp as e")
        .fails("Column 'empNo' not found in table 'E'; did you mean 'EMPNO'\\?");
    // With fully-qualified table alias
    sql("select catalog.sales.emp.^\"empNo\"^\n"
        + "from catalog.sales.emp")
        .fails("Column 'empNo' not found in table 'CATALOG\\.SALES\\.EMP'; "
            + "did you mean 'EMPNO'\\?");
    // Similar column in table; multiple tables
    sql("select ^\"name\"^ from emp, dept")
        .fails("Column 'name' not found in any table; did you mean 'NAME'\\?");
    // Similar column in table; table and a query
    sql("select ^\"name\"^ from emp,\n"
        + "  (select * from dept) as d")
        .fails("Column 'name' not found in any table; did you mean 'NAME'\\?");
    // Similar column in table; table and an un-aliased query
    sql("select ^\"name\"^ from emp, (select * from dept)")
        .fails("Column 'name' not found in any table; did you mean 'NAME'\\?");
    // Similar column in table, multiple tables
    sql("select ^\"deptno\"^ from emp,\n"
        + "  (select deptno as \"deptNo\" from dept)")
        .fails("Column 'deptno' not found in any table; "
            + "did you mean 'DEPTNO', 'deptNo'\\?");
    sql("select ^\"deptno\"^ from emp,\n"
        + "  (select * from dept) as t(\"deptNo\", name)")
        .fails("Column 'deptno' not found in any table; "
            + "did you mean 'DEPTNO', 'deptNo'\\?");
  }

  /** Tests matching of built-in operator names. */
  @Test void testUnquotedBuiltInFunctionNames() {
    final Sql mysql = sql("?")
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BACK_TICK)
        .withCaseSensitive(false);
    final Sql oracle = sql("?")
        .withUnquotedCasing(Casing.TO_UPPER)
        .withCaseSensitive(true);

    // Built-in functions are always case-insensitive.
    oracle.sql("select count(*), sum(deptno), floor(2.5) from dept").ok();
    oracle.sql("select COUNT(*), FLOOR(2.5) from dept").ok();
    oracle.sql("select cOuNt(*), FlOOr(2.5) from dept").ok();
    oracle.sql("select cOuNt (*), FlOOr (2.5) from dept").ok();
    oracle.sql("select current_time from dept").ok();
    oracle.sql("select Current_Time from dept").ok();
    oracle.sql("select CURRENT_TIME from dept").ok();

    mysql.sql("select sum(deptno), floor(2.5) from dept").ok();
    mysql.sql("select count(*), sum(deptno), floor(2.5) from dept").ok();
    mysql.sql("select COUNT(*), FLOOR(2.5) from dept").ok();
    mysql.sql("select cOuNt(*), FlOOr(2.5) from dept").ok();
    mysql.sql("select cOuNt (*), FlOOr (2.5) from dept").ok();
    mysql.sql("select current_time from dept").ok();
    mysql.sql("select Current_Time from dept").ok();
    mysql.sql("select CURRENT_TIME from dept").ok();

    // MySQL assumes that a quoted function name is not a built-in.
    //
    // mysql> select `sum`(`day`) from days;
    // ERROR 1630 (42000): FUNCTION foodmart.sum does not exist. Check the
    //   'Function Name Parsing and Resolution' section in the Reference Manual
    // mysql> select `SUM`(`day`) from days;
    // ERROR 1630 (42000): FUNCTION foodmart.SUM does not exist. Check the
    //   'Function Name Parsing and Resolution' section in the Reference Manual
    // mysql> select SUM(`day`) from days;
    // +------------+
    // | SUM(`day`) |
    // +------------+
    // |         28 |
    // +------------+
    // 1 row in set (0.00 sec)
    //
    // We do not follow MySQL in this regard. `count` is preserved in
    // lower-case, and is matched case-insensitively because it is a built-in.
    // So, the query succeeds.
    oracle.sql("select \"count\"(*) from dept").ok();
    mysql.sql("select `count`(*) from dept").ok();
  }

  /** Sanity check: All built-ins are upper-case. We rely on this. */
  @Test void testStandardOperatorNamesAreUpperCase() {
    for (SqlOperator op : SqlStdOperatorTable.instance().getOperatorList()) {
      final String name = op.getName();
      switch (op.getSyntax()) {
      case SPECIAL:
      case INTERNAL:
        break;
      default:
        assertThat(name.toUpperCase(Locale.ROOT), equalTo(name));
        break;
      }
    }
  }

  private static int prec(SqlOperator op) {
    return Math.max(op.getLeftPrec(), op.getRightPrec());
  }

  /** Tests that operators, sorted by precedence, are in a sane order. Each
   * operator has a {@link SqlOperator#getLeftPrec() left} and
   * {@link SqlOperator#getRightPrec()} right} precedence, but we would like
   * the order to remain the same even if we tweak particular operators'
   * precedences. If you need to update the expected output, you might also
   * need to change
   * <a href="http://calcite.apache.org/docs/reference.html#operator-precedence">
   * the documentation</a>. */
  @Test void testOperatorsSortedByPrecedence() {
    final StringBuilder b = new StringBuilder();
    final Comparator<SqlOperator> comparator = (o1, o2) -> {
      int c = Integer.compare(prec(o1), prec(o2));
      if (c != 0) {
        return -c;
      }
      c = o1.getName().compareTo(o2.getName());
      if (c != 0) {
        return c;
      }
      return o1.getSyntax().compareTo(o2.getSyntax());
    };
    final List<SqlOperator> operators =
        SqlStdOperatorTable.instance().getOperatorList();
    int p = -1;
    for (SqlOperator op : Ordering.from(comparator).sortedCopy(operators)) {
      final String type;
      switch (op.getSyntax()) {
      case FUNCTION:
      case FUNCTION_ID:
      case FUNCTION_STAR:
      case INTERNAL:
        continue;
      case PREFIX:
        type = "pre";
        break;
      case POSTFIX:
        type = "post";
        break;
      case BINARY:
        if (op.getLeftPrec() < op.getRightPrec()) {
          type = "left";
        } else {
          type = "right";
        }
        break;
      default:
        if (op instanceof SqlSpecialOperator) {
          type = "-";
        } else {
          continue;
        }
      }
      if (prec(op) != p) {
        b.append('\n');
        p = prec(op);
      }
      b.append(op.getName())
          .append(' ')
          .append(type)
          .append('\n');
    }
    final String expected = "\n"
        + "ARRAY -\n"
        + "ARRAY -\n"
        + "COLUMN_LIST -\n"
        + "CURSOR -\n"
        + "LATERAL -\n"
        + "MAP -\n"
        + "MAP -\n"
        + "MULTISET -\n"
        + "MULTISET -\n"
        + "ROW -\n"
        + "TABLE -\n"
        + "UNNEST -\n"
        + "\n"
        + "CURRENT_VALUE -\n"
        + "DEFAULT -\n"
        + "DOT -\n"
        + "ITEM -\n"
        + "NEXT_VALUE -\n"
        + "PATTERN_EXCLUDE -\n"
        + "PATTERN_PERMUTE -\n"
        + "WITHIN GROUP left\n"
        + "\n"
        + "PATTERN_QUANTIFIER -\n"
        + "\n"
        + " left\n"
        + "$LiteralChain -\n"
        + "+ pre\n"
        + "- pre\n"
        + "FINAL pre\n"
        + "RUNNING pre\n"
        + "\n"
        + "| left\n"
        + "\n"
        + "% left\n"
        + "* left\n"
        + "/ left\n"
        + "/INT left\n"
        + "|| left\n"
        + "\n"
        + "+ left\n"
        + "+ -\n"
        + "- left\n"
        + "- -\n"
        + "DATETIME_MINUS -\n"
        + "DATETIME_PLUS -\n"
        + "EXISTS pre\n"
        + "\n"
        + "< ALL left\n"
        + "< SOME left\n"
        + "<= ALL left\n"
        + "<= SOME left\n"
        + "<> ALL left\n"
        + "<> SOME left\n"
        + "= ALL left\n"
        + "= SOME left\n"
        + "> ALL left\n"
        + "> SOME left\n"
        + ">= ALL left\n"
        + ">= SOME left\n"
        + "BETWEEN ASYMMETRIC -\n"
        + "BETWEEN SYMMETRIC -\n"
        + "IN left\n"
        + "LIKE -\n"
        + "NOT BETWEEN ASYMMETRIC -\n"
        + "NOT BETWEEN SYMMETRIC -\n"
        + "NOT IN left\n"
        + "NOT LIKE -\n"
        + "NOT SIMILAR TO -\n"
        + "SIMILAR TO -\n"
        + "\n"
        + "$IS_DIFFERENT_FROM left\n"
        + "< left\n"
        + "<= left\n"
        + "<> left\n"
        + "= left\n"
        + "> left\n"
        + ">= left\n"
        + "CONTAINS left\n"
        + "EQUALS left\n"
        + "IMMEDIATELY PRECEDES left\n"
        + "IMMEDIATELY SUCCEEDS left\n"
        + "IS DISTINCT FROM left\n"
        + "IS NOT DISTINCT FROM left\n"
        + "MEMBER OF left\n"
        + "NOT SUBMULTISET OF left\n"
        + "OVERLAPS left\n"
        + "PRECEDES left\n"
        + "SUBMULTISET OF left\n"
        + "SUCCEEDS left\n"
        + "\n"
        + "IS A SET post\n"
        + "IS EMPTY post\n"
        + "IS FALSE post\n"
        + "IS NOT A SET post\n"
        + "IS NOT EMPTY post\n"
        + "IS NOT FALSE post\n"
        + "IS NOT NULL post\n"
        + "IS NOT TRUE post\n"
        + "IS NOT UNKNOWN post\n"
        + "IS NULL post\n"
        + "IS TRUE post\n"
        + "IS UNKNOWN post\n"
        + "\n"
        + "NOT pre\n"
        + "\n"
        + "AND left\n"
        + "\n"
        + "OR left\n"
        + "\n"
        + "=> -\n"
        + "AS -\n"
        + "DESC post\n"
        + "OVER left\n"
        + "TABLESAMPLE -\n"
        + "\n"
        + "INTERSECT left\n"
        + "INTERSECT ALL left\n"
        + "MULTISET INTERSECT ALL left\n"
        + "MULTISET INTERSECT DISTINCT left\n"
        + "NULLS FIRST post\n"
        + "NULLS LAST post\n"
        + "\n"
        + "EXCEPT left\n"
        + "EXCEPT ALL left\n"
        + "MULTISET EXCEPT ALL left\n"
        + "MULTISET EXCEPT DISTINCT left\n"
        + "MULTISET UNION ALL left\n"
        + "MULTISET UNION DISTINCT left\n"
        + "UNION left\n"
        + "UNION ALL left\n"
        + "\n"
        + "$throw -\n"
        + "FILTER left\n"
        + "Reinterpret -\n"
        + "TABLE pre\n"
        + "VALUES -\n"
        + "\n"
        + "CALL pre\n"
        + "ESCAPE -\n"
        + "NEW pre\n";
    assertThat(b.toString(), is(expected));
  }

  /** Tests that it is an error to insert into the same column twice, even using
   * case-insensitive matching. */
  @Test public void testCaseInsensitiveInsert() {
    final SqlTester tester1 = tester
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    tester1.checkQueryFails("insert into EMP ([EMPNO], deptno, ^[empno]^)\n"
            + " values (1, 1, 1)",
        "Target column 'EMPNO' is assigned more than once");
  }

  /** Tests referencing columns from a sub-query that has duplicate column
   * names. (The standard says it should be an error, but we don't right
   * now.) */
  @Test public void testCaseInsensitiveSubQuery() {
    final SqlTester insensitive = tester
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final SqlTester sensitive = tester
        .withCaseSensitive(true)
        .withQuoting(Quoting.BRACKET);
    String sql = "select [e] from (\n"
        + "select empno as [e], deptno as d, 1 as [e] from EMP)";
    sensitive.checkQuery(sql);
    insensitive.checkQuery(sql);
    String sql1 = "select e from (\n"
        + "select empno as [e], deptno as d, 1 as [E] from EMP)";
    insensitive.checkQuery(sql1);
    sensitive.checkQuery(sql1);
  }

  /** Tests using case-insensitive matching of table names. */
  @Test public void testCaseInsensitiveTables() {
    final SqlTester tester1 = tester.withLex(Lex.SQL_SERVER);
    tester1.checkQuery("select eMp.* from (select * from emp) as EmP");
    tester1.checkQueryFails("select ^eMp^.* from (select * from emp as EmP)",
        "Unknown identifier 'eMp'");
    tester1.checkQuery("select eMp.* from (select * from emP) as EmP");
    tester1.checkQuery("select eMp.empNo from (select * from emP) as EmP");
    tester1.checkQuery("select empNo from (select Empno from emP) as EmP");
    tester1.checkQuery("select empNo from (select Empno from emP)");
  }

  @Test public void testInsert() {
    tester.checkQuery("insert into empnullables (empno, ename)\n"
        + "values (1, 'Ambrosia')");
    tester.checkQuery("insert into empnullables (empno, ename)\n"
        + "select 1, 'Ardy' from (values 'a')");
    final String sql = "insert into emp\n"
        + "values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,\n"
        + "  1, false)";
    tester.checkQuery(sql);
    final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno, slacker)\n"
        + "select 1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00',\n"
        + "  1, 1, 1, false\n"
        + "from (values 'a')";
    tester.checkQuery(sql2);
    tester.checkQuery("insert into empnullables (ename, empno, deptno)\n"
        + "values ('Pat', 1, null)");

    final String sql3 = "insert into empnullables (\n"
        + "  empno, ename, job, hiredate)\n"
        + "values (1, 'Jim', 'Baker', timestamp '1970-01-01 00:00:00')";
    tester.checkQuery(sql3);

    tester.checkQuery("insert into empnullables (empno, ename)\n"
        + "select 1, 'b' from (values 'a')");

    tester.checkQuery("insert into empnullables (empno, ename)\n"
        + "values (1, 'Karl')");
  }

  @Test public void testInsertSubset() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    final String sql1 = "insert into empnullables\n"
        + "values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00')";
    pragmaticTester.checkQuery(sql1);
    final String sql2 = "insert into empnullables\n"
        + "values (1, 'nom', null, 0, null)";
    pragmaticTester.checkQuery(sql2);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1510">[CALCITE-1510]
   * INSERT/UPSERT should allow fewer values than columns</a>,
   * check for default value only when target field is null. */
  @Test public void testInsertShouldNotCheckForDefaultValue() {
    final int c =
        CountingFactory.THREAD_CALL_COUNT.get().get();
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    final String sql1 = "insert into emp values(1, 'nom', 'job', 0, "
        + "timestamp '1970-01-01 00:00:00', 1, 1, 1, false)";
    pragmaticTester.checkQuery(sql1);
    assertThat("Should not check for default value if column is in INSERT",
        CountingFactory.THREAD_CALL_COUNT.get().get(), is(c));

    // Now add a list of target columns, keeping the query otherwise the same.
    final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno, slacker)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, 1, false)";
    pragmaticTester.checkQuery(sql2);
    assertThat("Should not check for default value if column is in INSERT",
        CountingFactory.THREAD_CALL_COUNT.get().get(), is(c));

    // Now remove SLACKER, which is NOT NULL, from the target list.
    final String sql3 = "insert into ^emp^ (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, 1)";
    pragmaticTester.checkQueryFails(sql3,
        "Column 'SLACKER' has no default value and does not allow NULLs");
    assertThat("Should not check for default value, even if if column is missing"
            + "from INSERT and nullable",
        CountingFactory.THREAD_CALL_COUNT.get().get(),
        is(c));

    // Now remove DEPTNO, which has a default value, from the target list.
    // Will generate an extra call to newColumnDefaultValue at sql-to-rel time,
    // just not yet.
    final String sql4 = "insert into ^emp^ (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, slacker)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, false)";
    pragmaticTester.checkQuery(sql4);
    assertThat("Missing DEFAULT column generates a call to factory",
        CountingFactory.THREAD_CALL_COUNT.get().get(),
        is(c));
  }

  @Test public void testInsertView() {
    tester.checkQuery("insert into empnullables_20 (ename, empno, comm)\n"
        + "values ('Karl', 1, 1)");
  }

  @Test public void testInsertSubsetView() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    pragmaticTester.checkQuery("insert into empnullables_20\n"
        + "values (1, 'Karl')");
  }

  @Test public void testInsertModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into EMP_MODIFIABLEVIEW (empno, ename, job)\n"
        + "values (1, 'Arthur', 'clown')").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2 (empno, ename, job, extra)\n"
        + "values (1, 'Arthur', 'clown', true)").ok();
  }

  @Test public void testInsertSubsetModifiableView() {
    final Sql s = sql("?").withExtendedCatalog2003();
    s.sql("insert into EMP_MODIFIABLEVIEW2\n"
        + "values ('Arthur', 1)").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2\n"
        + "values ('Arthur', 1, 'Knight', 20, false, 99999, true, timestamp '1370-01-01 00:00:00',"
        + " 1, 100)").ok();
  }

  @Test public void testInsertBind() {
    // VALUES
    final String sql0 = "insert into empnullables (empno, ename, deptno)\n"
        + "values (?, ?, ?)";
    sql(sql0).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, INTEGER ?2)");

    // multiple VALUES
    final String sql1 = "insert into empnullables (empno, ename, deptno)\n"
        + "values (?, 'Pat', 1), (2, ?, ?), (3, 'Tod', ?), (4, 'Arthur', null)";
    sql(sql1).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, INTEGER ?2, INTEGER ?3)");

    // VALUES with expression
    sql("insert into empnullables (ename, empno) values (?, ? + 1)")
        .ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");

    // SELECT
    sql("insert into empnullables (ename, empno) select ?, ? from (values (1))")
        .ok().bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");

    // WITH
    final String sql3 = "insert into empnullables (ename, empno)\n"
        + "with v as (values ('a'))\n"
        + "select ?, ? from (values (1))";
    sql(sql3).ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");

    // UNION
    final String sql2 = "insert into empnullables (ename, empno)\n"
        + "select ?, ? from (values (1))\n"
        + "union all\n"
        + "select ?, ? from (values (time '1:2:3'))";
    final String expected2 = "RecordType(VARCHAR(20) ?0, INTEGER ?1,"
        + " VARCHAR(20) ?2, INTEGER ?3)";
    sql(sql2).ok().bindType(expected2);
  }


  @Test public void testInsertWithExtendedColumns() {
    final String sql0 = "insert into empnullables\n"
        + " (empno, ename, \"f.dc\" varchar(10))\n"
        + "values (?, ?, ?)";
    sql(sql0)
        .tester(EXTENDED_CATALOG_TESTER_LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2)")
        .tester(EXTENDED_CATALOG_TESTER_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");

    final String sql1 = "insert into empnullables\n"
        + " (empno, ename, dynamic_column double not null)\n"
        + "values (?, ?, ?)";
    sql(sql1)
        .tester(EXTENDED_CATALOG_TESTER_LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, DOUBLE ?2)")
        .tester(EXTENDED_CATALOG_TESTER_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");

    final String sql2 = "insert into struct.t_extend\n"
        + " (f0.c0, f1.c1, \"F2\".\"C2\" varchar(20) not null)\n"
        + "values (?, ?, ?)";
    sql(sql2)
        .tester(EXTENDED_CATALOG_TESTER_LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)")
        .tester(EXTENDED_CATALOG_TESTER_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");
  }

  @Test public void testInsertBindSubset() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    // VALUES
    final String sql0 = "insert into empnullables \n"
        + "values (?, ?, ?)";
    sql(sql0).tester(pragmaticTester).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2)");

    // multiple VALUES
    final String sql1 = "insert into empnullables\n"
        + "values (?, 'Pat', 'Tailor'), (2, ?, ?),\n"
        + " (3, 'Tod', ?), (4, 'Arthur', null)";
    sql(sql1).tester(pragmaticTester).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2, VARCHAR(10) ?3)");

    // VALUES with expression
    sql("insert into empnullables values (? + 1, ?)")
        .tester(pragmaticTester)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // SELECT
    sql("insert into empnullables select ?, ? from (values (1))")
        .tester(pragmaticTester)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // WITH
    final String sql3 = "insert into empnullables \n"
        + "with v as (values ('a'))\n"
        + "select ?, ? from (values (1))";
    sql(sql3).tester(pragmaticTester).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // UNION
    final String sql2 = "insert into empnullables \n"
        + "select ?, ? from (values (1))\n"
        + "union all\n"
        + "select ?, ? from (values (time '1:2:3'))";
    final String expected2 = "RecordType(INTEGER ?0, VARCHAR(20) ?1,"
        + " INTEGER ?2, VARCHAR(20) ?3)";
    sql(sql2).tester(pragmaticTester).ok().bindType(expected2);
  }

  @Test public void testInsertBindView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (mgr, empno, ename)"
        + " values (?, ?, ?)";
    sql(sql).withExtendedCatalog().ok()
        .bindType("RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)");
  }

  @Test public void testInsertModifiableViewPassConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename, extra)"
        + " values (20, 100, 'Lex', true)").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2 (empno, ename, extra)"
        + " values (100, 'Lex', true)").ok();

    final Sql s2 = sql("?").withExtendedCatalog2003();
    s2.sql("insert into EMP_MODIFIABLEVIEW2 values ('Edward', 20)").ok();
  }

  @Test public void testInsertModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename)"
        + " values (^21^, 100, 'Lex')";
    final String error0 = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename)"
        + " values (^19+1^, 100, 'Lex')";
    final String error1 = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql1).fails(error1);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2\n"
        + "values ('Arthur', 1, 'Knight', ^27^, false, 99999, true,"
        + "timestamp '1370-01-01 00:00:00', 1, 100)";
    final String error2 = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql2).fails(error2);
  }

  @Test public void testUpdateModifiableViewPassConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW2"
        + " set deptno = 20, empno = 99"
        + " where ename = 'Lex'").ok();
    s.sql("update EMP_MODIFIABLEVIEW2"
        + " set empno = 99"
        + " where ename = 'Lex'").ok();
  }

  @Test public void testUpdateModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "update EMP_MODIFIABLEVIEW2"
        + " set deptno = ^21^, empno = 99"
        + " where ename = 'Lex'";
    final String error = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error);

    final String sql1 = "update EMP_MODIFIABLEVIEW2"
        + " set deptno = ^19 + 1^, empno = 99"
        + " where ename = 'Lex'";
    s.sql(sql1).fails(error);
  }

  @Test public void testInsertFailNullability() {
    tester.checkQueryFails(
        "insert into ^empnullables^ (ename) values ('Kevin')",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    tester.checkQueryFails(
        "insert into ^empnullables^ (empno) values (10)",
        "Column 'ENAME' has no default value and does not allow NULLs");
    tester.checkQueryFails(
        "insert into empnullables (empno, ename, deptno) ^values (5, null, 5)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertSubsetFailNullability() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    pragmaticTester.checkQueryFails("insert into ^empnullables^ values (1)",
        "Column 'ENAME' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empnullables ^values (null, 'Liam')^",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empnullables ^values (45, null, 5)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertViewFailNullability() {
    tester.checkQueryFails(
        "insert into ^empnullables_20^ (ename) values ('Jake')",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    tester.checkQueryFails(
        "insert into ^empnullables_20^ (empno) values (9)",
        "Column 'ENAME' has no default value and does not allow NULLs");
    tester.checkQueryFails(
        "insert into empnullables_20 (empno, ename, mgr) ^values (5, null, 5)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertSubsetViewFailNullability() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    pragmaticTester.checkQueryFails("insert into ^empnullables_20^ values (1)",
        "Column 'ENAME' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empnullables_20 ^values (null, 'Liam')^",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empnullables_20 ^values (45, null)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindFailNullability() {
    tester.checkQueryFails("insert into ^emp^ (ename) values (?)",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    tester.checkQueryFails(
        "insert into ^emp^ (empno) values (?)",
        "Column 'ENAME' has no default value and does not allow NULLs");
    tester.checkQueryFails(
        "insert into emp (empno, ename, deptno) ^values (?, null, 5)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindSubsetFailNullability() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    pragmaticTester.checkQueryFails("insert into ^empnullables^ values (?)",
        "Column 'ENAME' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empnullables ^values (null, ?)^",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empnullables ^values (?, null)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertSubsetDisallowed() {
    tester.checkQueryFails("insert into ^emp^ values (1)",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(1\\)");
    tester.checkQueryFails("insert into ^emp^ values (null)",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(1\\)");
    tester.checkQueryFails("insert into ^emp^ values (1, 'Kevin')",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(2\\)");
  }

  @Test public void testInsertSubsetViewDisallowed() {
    tester.checkQueryFails("insert into ^emp_20^ values (1)",
        "Number of INSERT target columns \\(8\\) does not equal number of source items \\(1\\)");
    tester.checkQueryFails("insert into ^emp_20^ values (null)",
        "Number of INSERT target columns \\(8\\) does not equal number of source items \\(1\\)");
    tester.checkQueryFails("insert into ^emp_20^ values (?, ?)",
        "Number of INSERT target columns \\(8\\) does not equal number of source items \\(2\\)");
  }

  @Test public void testInsertBindSubsetDisallowed() {
    tester.checkQueryFails("insert into ^emp^ values (?)",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(1\\)");
    tester.checkQueryFails("insert into ^emp^ values (?, ?)",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(2\\)");
  }

  @Test public void testSelectExtendedColumnDuplicate() {
    sql("select deptno, extra from emp (extra int, \"extra\" boolean)").ok();
    sql("select deptno, extra from emp (extra int, \"extra\" int)").ok();
    sql("select deptno, extra from emp (extra int, ^extra^ int)")
        .fails("Duplicate name 'EXTRA' in column list");
    sql("select deptno, extra from emp (extra int, ^extra^ boolean)")
        .fails("Duplicate name 'EXTRA' in column list");
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select deptno, extra\n"
        + "from EMP_MODIFIABLEVIEW (extra int, ^extra^ int)";
    s.sql(sql0).fails("Duplicate name 'EXTRA' in column list");

    final String sql1 = "select deptno, extra from EMP_MODIFIABLEVIEW"
        + " (extra int, ^extra^ boolean)";
    s.sql(sql1).fails("Duplicate name 'EXTRA' in column list");
  }

  @Test public void testSelectViewFailExcludedColumn() {
    final String sql = "select ^deptno^, empno from EMP_MODIFIABLEVIEW";
    final String error = "Column 'DEPTNO' not found in any table";
    sql(sql).withExtendedCatalog().fails(error);
  }

  @Test public void testSelectViewExtendedColumnCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (SAL int)\n"
        + " where SAL = 20").ok();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, \"Sal\", HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"Sal\" VARCHAR)\n"
        + " where SAL = 20").ok();
  }

  @Test public void testSelectViewExtendedColumnExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2 extend (EXTRA boolean)\n"
        + " where SAL = 20").ok();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, EXTRA,"
        + " \"EXtra\"\n"
        + " from EMP_MODIFIABLEVIEW2 extend (\"EXtra\" VARCHAR)\n"
        + " where SAL = 20").ok();
  }

  @Test public void testSelectViewExtendedColumnUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMP_MODIFIABLEVIEW3 extend (COMM int)\n"
        + " where SAL = 20").ok();
    s.sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, \"comM\"\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"comM\" BOOLEAN)\n"
        + " where SAL = 20").ok();
  }

  @Test public void testSelectExtendedColumnCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
            + " from EMPDEFAULTS extend (COMM int)\n"
            + " where SAL = 20").ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM, \"ComM\"\n"
        + " from EMPDEFAULTS extend (\"ComM\" int)\n"
        + " where SAL = 20").ok();
  }

  @Test public void testSelectExtendedColumnFailCollision() {
    tester.checkQueryFails("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
            + " from EMPDEFAULTS extend (^COMM^ boolean)\n"
            + " where SAL = 20",
        "Cannot assign to target field 'COMM' of type INTEGER from source field 'COMM' of type BOOLEAN");
    tester.checkQueryFails("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
            + " from EMPDEFAULTS extend (^EMPNO^ integer)\n"
            + " where SAL = 20",
        "Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL from source field 'EMPNO' of type INTEGER");
    tester.checkQueryFails("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
            + " from EMPDEFAULTS extend (^\"EMPNO\"^ integer)\n"
            + " where SAL = 20",
        "Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL from source field 'EMPNO' of type INTEGER");
  }

  @Test public void testSelectViewExtendedColumnFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, EXTRA\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^SLACKER^ integer)\n"
        + " where SAL = 20";
    final String error0 = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'SLACKER' of type INTEGER";
    s.sql(sql0).fails(error0);

    final String sql1 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, COMM\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^EMPNO^ integer)\n"
        + " where SAL = 20";
    final String error1 = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type INTEGER";
    s.sql(sql1).fails(error1);
  }

  @Test public void testSelectViewExtendedColumnFailExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, EXTRA\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^EXTRA^ integer)\n"
        + " where SAL = 20";
    final String error = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXTRA' of type INTEGER";
    s.sql(sql0).fails(error);

    final String sql1 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, EXTRA\n"
        + "from EMP_MODIFIABLEVIEW2 extend (^\"EXTRA\"^ integer)\n"
        + " where SAL = 20";
    s.sql(sql1).fails(error);
  }

  @Test public void testSelectViewExtendedColumnFailUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, COMM\n"
        + "from EMP_MODIFIABLEVIEW3 extend (^COMM^ boolean)\n"
        + "where SAL = 20";
    final String error = "Cannot assign to target field 'COMM' of type INTEGER"
        + " from source field 'COMM' of type BOOLEAN";
    s.sql(sql0).fails(error);

    final String sql1 = "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE,"
        + " MGR, COMM\n"
        + "from EMP_MODIFIABLEVIEW3 extend (^\"COMM\"^ boolean)\n"
        + " where SAL = 20";
    s.sql(sql1).fails(error);
  }

  @Test public void testSelectFailCaseSensitivity() {
    tester.checkQueryFails("select ^\"empno\"^, ename, deptno from EMP",
        "Column 'empno' not found in any table; did you mean 'EMPNO'\\?");
    tester.checkQueryFails("select ^\"extra\"^, ename, deptno from EMP (extra boolean)",
        "Column 'extra' not found in any table; did you mean 'EXTRA'\\?");
    tester.checkQueryFails("select ^extra^, ename, deptno from EMP (\"extra\" boolean)",
        "Column 'EXTRA' not found in any table; did you mean 'extra'\\?");
  }

  @Test public void testInsertFailCaseSensitivity() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW"
        + " (^\"empno\"^, ename, deptno)"
        + " values (45, 'Jake', 5)";
    s.sql(sql0).fails("Unknown target column 'empno'");

    final String sql1 = "insert into EMP_MODIFIABLEVIEW (\"extra\" int)"
        + " (^extra^, ename, deptno)"
        + " values (45, 'Jake', 5)";
    s.sql(sql1).fails("Unknown target column 'EXTRA'");

    final String sql2 = "insert into EMP_MODIFIABLEVIEW (extra int)"
        + " (^\"extra\"^, ename, deptno)"
        + " values (45, 'Jake', 5)";
    s.sql(sql2).fails("Unknown target column 'extra'");
  }

  @Test public void testInsertFailExcludedColumn() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = ""
        + "insert into EMP_MODIFIABLEVIEW (empno, ename, ^deptno^)"
        + " values (45, 'Jake', 5)";
    s.sql(sql).fails("Unknown target column 'DEPTNO'");
  }

  @Test public void testInsertBindViewFailExcludedColumn() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = "insert into EMP_MODIFIABLEVIEW (empno, ename, ^deptno^)"
        + " values (?, ?, ?)";
    s.sql(sql).fails("Unknown target column 'DEPTNO'");
  }

  @Test public void testInsertWithCustomInitializerExpressionFactory() {
    tester.checkQuery("insert into empdefaults (deptno) values (1)");
    tester.checkQuery("insert into empdefaults (ename, empno) values ('Quan', 50)");
    tester.checkQueryFails("insert into empdefaults (ename, deptno) ^values (null, 1)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
    tester.checkQueryFails("insert into ^empdefaults^ values (null, 'Tod')",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(2\\)");
  }

  @Test public void testInsertSubsetWithCustomInitializerExpressionFactory() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    pragmaticTester.checkQuery("insert into empdefaults values (101)");
    pragmaticTester.checkQuery("insert into empdefaults values (101, 'Coral')");
    pragmaticTester.checkQueryFails("insert into empdefaults ^values (null, 'Tod')^",
        "Column 'EMPNO' has no default value and does not allow NULLs");
    pragmaticTester.checkQueryFails("insert into empdefaults ^values (78, null)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindWithCustomInitializerExpressionFactory() {
    sql("insert into empdefaults (deptno) values (?)").ok()
        .bindType("RecordType(INTEGER ?0)");
    sql("insert into empdefaults (ename, empno) values (?, ?)").ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");
    tester.checkQueryFails("insert into empdefaults (ename, deptno) ^values (null, ?)^",
        "Column 'ENAME' has no default value and does not allow NULLs");
    tester.checkQueryFails("insert into ^empdefaults^ values (null, ?)",
        "Number of INSERT target columns \\(9\\) does not equal number of source items \\(2\\)");
  }

  @Test public void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    sql("insert into empdefaults values (101, ?)").tester(pragmaticTester).ok()
        .bindType("RecordType(VARCHAR(20) ?0)");
    pragmaticTester.checkQueryFails("insert into empdefaults ^values (null, ?)^",
        "Column 'EMPNO' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindWithCustomColumnResolving() {
    final SqlTester pragmaticTester =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    final String sql = "insert into struct.t\n"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    final String expected = "RecordType(VARCHAR(20) ?0, VARCHAR(20) ?1,"
        + " INTEGER ?2, BOOLEAN ?3, INTEGER ?4, INTEGER ?5, INTEGER ?6,"
        + " INTEGER ?7, INTEGER ?8)";
    sql(sql).ok().bindType(expected);

    final String sql2 =
        "insert into struct.t_nullables (c0, c2, c1) values (?, ?, ?)";
    final String expected2 =
        "RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)";
    sql(sql2).tester(pragmaticTester).ok().bindType(expected2);

    final String sql3 =
        "insert into struct.t_nullables (f1.c0, f1.c2, f0.c1) values (?, ?, ?)";
    final String expected3 =
        "RecordType(INTEGER ?0, INTEGER ?1, INTEGER ?2)";
    sql(sql3).tester(pragmaticTester).ok().bindType(expected3);

    sql("insert into struct.t_nullables (c0, ^c4^, c1) values (?, ?, ?)")
        .tester(pragmaticTester)
        .fails("Unknown target column 'C4'");
    sql("insert into struct.t_nullables (^a0^, c2, c1) values (?, ?, ?)")
        .tester(pragmaticTester)
        .fails("Unknown target column 'A0'");
    final String sql4 = "insert into struct.t_nullables (\n"
        + "  f1.c0, ^f0.a0^, f0.c1) values (?, ?, ?)";
    sql(sql4).tester(pragmaticTester)
        .fails("Unknown target column 'F0.A0'");
    final String sql5 = "insert into struct.t_nullables (\n"
        + "  f1.c0, f1.c2, ^f1.c0^) values (?, ?, ?)";
    sql(sql5).tester(pragmaticTester)
        .fails("Target column '\"F1\".\"C0\"' is assigned more than once");
  }

  @Test public void testUpdateBind() {
    final String sql = "update emp\n"
        + "set ename = ?\n"
        + "where deptno = ?";
    sql(sql).ok().bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");
  }

  @Test public void testDeleteBind() {
    final String sql = "delete from emp\n"
        + "where deptno = ?\n"
        + "or ename = ?";
    sql(sql).ok().bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");
  }

  @Test public void testStream() {
    sql("select stream * from orders").ok();
    sql("select stream * from ^emp^")
        .fails(cannotConvertToStream("EMP"));
    sql("select * from ^orders^")
        .fails(cannotConvertToRelation("ORDERS"));
  }

  @Test public void testStreamWhere() {
    sql("select stream * from orders where productId < 10").ok();
    sql("select stream * from ^emp^ where deptno = 10")
        .fails(cannotConvertToStream("EMP"));
    sql("select stream * from ^emp^ as e where deptno = 10")
        .fails(cannotConvertToStream("E"));
    sql("select stream * from (^select * from emp as e1^) as e\n"
        + "where deptno = 10")
        .fails("Cannot convert table 'E' to stream");
    sql("select * from ^orders^ where productId > 10")
        .fails(cannotConvertToRelation("ORDERS"));
  }

  @Test public void testStreamGroupBy() {
    sql("select stream rowtime, productId, count(*) as c\n"
        + "from orders\n"
        + "group by productId, rowtime").ok();
    sql("select stream floor(rowtime to hour) as rowtime, productId,\n"
        + " count(*) as c\n"
        + "from orders\n"
        + "group by floor(rowtime to hour), productId").ok();
    sql("select stream productId, count(*) as c\n"
        + "from orders\n"
        + "^group by productId^")
        .fails(STR_AGG_REQUIRES_MONO);
    sql("select stream ^count(*)^ as c\n"
        + "from orders")
        .fails(STR_AGG_REQUIRES_MONO);
    sql("select stream count(*) as c\n"
        + "from orders ^group by ()^")
        .fails(STR_AGG_REQUIRES_MONO);
  }

  @Test public void testStreamHaving() {
    sql("select stream rowtime, productId, count(*) as c\n"
        + "from orders\n"
        + "group by productId, rowtime\n"
        + "having count(*) > 5").ok();
    sql("select stream floor(rowtime to hour) as rowtime, productId,\n"
        + " count(*) as c\n"
        + "from orders\n"
        + "group by floor(rowtime to hour), productId\n"
        + "having false").ok();
    sql("select stream productId, count(*) as c\n"
        + "from orders\n"
        + "^group by productId^\n"
        + "having count(*) > 5")
        .fails(STR_AGG_REQUIRES_MONO);
    sql("select stream 1\n"
        + "from orders\n"
        + "having ^count(*) > 3^")
        .fails(STR_AGG_REQUIRES_MONO);
  }

  /** Tests that various expressions are monotonic. */
  @Test public void testMonotonic() {
    sql("select stream floor(rowtime to hour) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream ceil(rowtime to minute) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(minute from rowtime) from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);
    sql("select stream (rowtime - timestamp '1970-01-01 00:00:00') hour from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream\n"
        + "cast((rowtime - timestamp '1970-01-01 00:00:00') hour as integer)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream\n"
        + "cast((rowtime - timestamp '1970-01-01 00:00:00') hour as integer) / 15\n"
        + "from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream\n"
        + "mod(cast((rowtime - timestamp '1970-01-01 00:00:00') hour as integer), 15)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // constant
    sql("select stream 1 - 2 from orders")
        .monotonic(SqlMonotonicity.CONSTANT);
    sql("select stream 1 + 2 from orders")
        .monotonic(SqlMonotonicity.CONSTANT);

    // extract(YEAR) is monotonic, extract(other time unit) is not
    sql("select stream extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(month from rowtime) from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // <monotonic> - constant
    sql("select stream extract(year from rowtime) - 3 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) * 5 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) * -5 from orders")
        .monotonic(SqlMonotonicity.DECREASING);

    // <monotonic> / constant
    sql("select stream extract(year from rowtime) / -5 from orders")
        .monotonic(SqlMonotonicity.DECREASING);
    sql("select stream extract(year from rowtime) / 5 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) / 0 from orders")
        .monotonic(SqlMonotonicity.CONSTANT); // +inf is constant!

    // constant / <monotonic> is not monotonic (we don't know whether sign of
    // expression ever changes)
    sql("select stream 5 / extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // <monotonic> * constant
    sql("select stream extract(year from rowtime) * -5 from orders")
        .monotonic(SqlMonotonicity.DECREASING);
    sql("select stream extract(year from rowtime) * 5 from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream extract(year from rowtime) * 0 from orders")
        .monotonic(SqlMonotonicity.CONSTANT); // 0 is constant!

    // constant * <monotonic>
    sql("select stream -5 * extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.DECREASING);
    sql("select stream 5 * extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.INCREASING);
    sql("select stream 0 * extract(year from rowtime) from orders")
        .monotonic(SqlMonotonicity.CONSTANT);

    // <monotonic> - <monotonic>
    sql("select stream\n"
        + "extract(year from rowtime) - extract(year from rowtime)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.NOT_MONOTONIC);

    // <monotonic> + <monotonic>
    sql("select stream\n"
        + "extract(year from rowtime) + extract(year from rowtime)\n"
        + "from orders")
        .monotonic(SqlMonotonicity.INCREASING);
  }

  @Test public void testStreamUnionAll() {
    sql("select orderId\n"
        + "from ^orders^\n"
        + "union all\n"
        + "select orderId\n"
        + "from shipments")
        .fails(cannotConvertToRelation("ORDERS"));
    sql("select stream orderId\n"
        + "from orders\n"
        + "union all\n"
        + "^select orderId\n"
        + "from shipments^")
        .fails(STR_SET_OP_INCONSISTENT);
    sql("select empno\n"
        + "from emp\n"
        + "union all\n"
        + "^select stream orderId\n"
        + "from orders^")
        .fails(STR_SET_OP_INCONSISTENT);
    sql("select stream orderId\n"
        + "from orders\n"
        + "union all\n"
        + "select stream orderId\n"
        + "from shipments").ok();
    sql("select stream rowtime, orderId\n"
        + "from orders\n"
        + "union all\n"
        + "select stream rowtime, orderId\n"
        + "from shipments\n"
        + "order by rowtime").ok();
  }

  @Test public void testStreamValues() {
    sql("select stream * from (^values 1^) as e")
        .fails(cannotConvertToStream("E"));
    sql("select stream orderId from orders\n"
        + "union all\n"
        + "^values 1^")
        .fails(STR_SET_OP_INCONSISTENT);
    sql("values 1, 2\n"
        + "union all\n"
        + "^select stream orderId from orders^\n")
        .fails(STR_SET_OP_INCONSISTENT);
  }

  @Test public void testStreamOrderBy() {
    sql("select stream *\n"
        + "from orders\n"
        + "order by rowtime").ok();
    sql("select stream *\n"
        + "from orders\n"
        + "order by floor(rowtime to hour)").ok();
    sql("select stream floor(rowtime to minute), productId\n"
        + "from orders\n"
        + "order by floor(rowtime to hour)").ok();
    sql("select stream floor(rowtime to minute), productId\n"
        + "from orders\n"
        + "order by floor(rowtime to minute), productId desc").ok();
    sql("select stream *\n"
        + "from orders\n"
        + "order by ^productId^, rowtime")
        .fails(STR_ORDER_REQUIRES_MONO);
    sql("select stream *\n"
        + "from orders\n"
        + "order by ^rowtime desc^")
        .fails(STR_ORDER_REQUIRES_MONO);
    sql("select stream *\n"
        + "from orders\n"
        + "order by floor(rowtime to hour), rowtime desc").ok();
  }

  @Test public void testStreamJoin() {
    sql("select stream \n"
        + "orders.rowtime as rowtime, orders.orderId as orderId, products.supplierId as supplierId \n"
        + "from orders join products on orders.productId = products.productId").ok();
    sql("^select stream *\n"
        + "from products join suppliers on products.supplierId = suppliers.supplierId^")
        .fails(cannotStreamResultsForNonStreamingInputs("PRODUCTS, SUPPLIERS"));
  }

  @Test public void testDummy() {
    // (To debug individual statements, paste them into this method.)
  }

  @Test public void testCustomColumnResolving() {
    checkCustomColumnResolving("T");
  }

  @Test public void testCustomColumnResolvingWithView() {
    checkCustomColumnResolving("T_10");
  }

  private void checkCustomColumnResolving(String table) {
    // Table STRUCT.T is defined as: (
    //   K0 VARCHAR(20) NOT NULL,
    //   C1 VARCHAR(20) NOT NULL,
    //   RecordType:PEEK_FIELDS_DEFAULT(
    //     C0 INTEGER NOT NULL,
    //     C1 INTEGER NOT NULL) F0,
    //   RecordType:PEEK_FIELDS(
    //      C0 INTEGER,
    //      C2 INTEGER NOT NULL,
    //      A0 INTEGER NOT NULL) F1,
    //   RecordType:PEEK_FIELDS(
    //      C3 INTEGER NOT NULL,
    //      A0 BOOLEAN NOT NULL) F2)
    //
    // The labels 'PEEK_FIELDS_DEFAULT' and 'PEEK_FIELDS' mean that F0, F1 and
    // F2 can all be transparent. F0 has default struct priority; F1 and F2 have
    // lower priority.

    sql("select * from struct." + table).ok();

    // Resolve K0 as top-level column K0.
    sql("select k0 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL K0) NOT NULL");

    // Resolve C2 as secondary-level column F1.C2.
    sql("select c2 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve F1.C2 as fully qualified column F1.C2.
    sql("select f1.c2 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve C1 as top-level column C1 as opposed to F0.C1.
    sql("select c1 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Resolve C0 as secondary-level column F0.C0 as opposed to F1.C0, since F0
    // has the default priority.
    sql("select c0 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C0) NOT NULL");

    // Resolve F1.C0 as fully qualified column F1.C0 (as evidenced by "INTEGER"
    // rather than "INTEGER NOT NULL")
    sql("select f1.c0 from struct." + table)
        .type("RecordType(INTEGER C0) NOT NULL");

    // Fail ambiguous column reference A0, since F1.A0 and F2.A0 both exist with
    // the same resolving priority.
    sql("select ^a0^ from struct." + table)
        .fails("Column 'A0' is ambiguous");

    // Resolve F2.A0 as fully qualified column F2.A0.
    sql("select f2.a0 from struct." + table)
        .type("RecordType(BOOLEAN NOT NULL A0) NOT NULL");

    // Resolve T0.K0 as top-level column K0, since T0 is recognized as the table
    // alias.
    sql("select t0.k0 from struct." + table + " t0")
        .type("RecordType(VARCHAR(20) NOT NULL K0) NOT NULL");

    // Resolve T0.C2 as secondary-level column F1.C2, since T0 is recognized as
    // the table alias here.
    sql("select t0.c2 from struct." + table + " t0")
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve F0.C2 as secondary-level column F1.C2, since F0 is recognized as
    // the table alias here.
    sql("select f0.c2 from struct." + table + " f0")
        .type("RecordType(INTEGER NOT NULL C2) NOT NULL");

    // Resolve F0.C1 as top-level column C1 as opposed to F0.C1, since F0 is
    // recognized as the table alias here.
    sql("select f0.c1 from struct." + table + " f0")
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Resolve C1 as inner INTEGER column not top-level VARCHAR column.
    sql("select f0.f0.c1 from struct." + table + " f0")
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Resolve <table>.C1 as top-level column C1 as opposed to F0.C1, since <table> is
    // recognized as the table name.
    sql("select " + table + ".c1 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Alias "f0" obscures table name "<table>"
    sql("select ^" + table + "^.c1 from struct." + table + " f0")
        .fails("Table '" + table + "' not found");

    // Resolve STRUCT.<table>.C1 as top-level column C1 as opposed to F0.C1, since
    // STRUCT.<table> is recognized as the schema and table name.
    sql("select struct." + table + ".c1 from struct." + table)
        .type("RecordType(VARCHAR(20) NOT NULL C1) NOT NULL");

    // Table alias "f0" obscures table name "STRUCT.<table>"
    sql("select ^struct." + table + "^.c1 from struct." + table + " f0")
        .fails("Table 'STRUCT." + table + "' not found");

    // Resolve F0.F0.C1 as secondary-level column F0.C1, since the first F0 is
    // recognized as the table alias here.
    sql("select f0.f0.c1 from struct." + table + " f0")
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Resolve <table>.F0.C1 as secondary-level column F0.C1, since <table> is
    // recognized as the table name.
    sql("select " + table + ".f0.c1 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Table alias obscures
    sql("select ^" + table + ".f0^.c1 from struct." + table + " f0")
        .fails("Table '" + table + ".F0' not found");

    // Resolve STRUCT.<table>.F0.C1 as secondary-level column F0.C1, since
    // STRUCT.<table> is recognized as the schema and table name.
    sql("select struct." + table + ".f0.c1 from struct." + table)
        .type("RecordType(INTEGER NOT NULL C1) NOT NULL");

    // Table alias "f0" obscures table name "STRUCT.<table>"
    sql("select ^struct." + table + ".f0^.c1 from struct." + table + " f0")
        .fails("Table 'STRUCT." + table + ".F0' not found");

    // Resolve struct type F1 with wildcard.
    sql("select f1.* from struct." + table)
        .type("RecordType(INTEGER NOT NULL A0, INTEGER C0,"
            + " INTEGER NOT NULL C2) NOT NULL");

    // Resolve struct type F1 with wildcard.
    sql("select " + table + ".f1.* from struct." + table)
        .type("RecordType(INTEGER NOT NULL A0, INTEGER C0,"
            + " INTEGER NOT NULL C2) NOT NULL");

    // Fail non-existent column B0.
    sql("select ^b0^ from struct." + table)
        .fails("Column 'B0' not found in any table");

    // A column family can only be referenced with a star expansion.
    sql("select ^f1^ from struct." + table)
        .fails("Column 'F1' not found in any table");

    // If we fail to find a column, give an error based on the shortest prefix
    // that fails.
    sql("select " + table + ".^f0.notFound^.a.b.c.d from struct." + table)
        .fails("Column 'F0\\.NOTFOUND' not found in table '" + table + "'");
    sql("select " + table + ".^f0.notFound^ from struct." + table)
        .fails("Column 'F0\\.NOTFOUND' not found in table '" + table + "'");
    sql("select " + table + ".^f0.c1.notFound^ from struct." + table)
        .fails("Column 'F0\\.C1\\.NOTFOUND' not found in table '" + table + "'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Dynamic Table / Dynamic Star support</a>. */
  @Test public void testAmbiguousDynamicStar() throws Exception {
    final String sql = "select ^n_nation^\n"
        + "from (select * from \"DYNAMIC\".NATION),\n"
        + " (select * from \"DYNAMIC\".CUSTOMER)";
    sql(sql).fails("Column 'N_NATION' is ambiguous");
  }

  @Test public void testAmbiguousDynamicStar2() throws Exception {
    final String sql = "select ^n_nation^\n"
        + "from (select * from \"DYNAMIC\".NATION, \"DYNAMIC\".CUSTOMER)";
    sql(sql).fails("Column 'N_NATION' is ambiguous");
  }

  @Test public void testAmbiguousDynamicStar3() throws Exception {
    final String sql = "select ^nc.n_nation^\n"
        + "from (select * from \"DYNAMIC\".NATION, \"DYNAMIC\".CUSTOMER) as nc";
    sql(sql).fails("Column 'N_NATION' is ambiguous");
  }

  @Test public void testAmbiguousDynamicStar4() throws Exception {
    final String sql = "select n.n_nation\n"
        + "from (select * from \"DYNAMIC\".NATION) as n,\n"
        + " (select * from \"DYNAMIC\".CUSTOMER)";
    sql(sql).type("RecordType(ANY N_NATION) NOT NULL");
  }

  /** When resolve column reference, regular field has higher priority than
   * dynamic star columns. */
  @Test public void testDynamicStar2() throws Exception {
    final String sql = "select newid from (\n"
        + "  select *, NATION.N_NATION + 100 as newid\n"
        + "  from \"DYNAMIC\".NATION, \"DYNAMIC\".CUSTOMER)";
    sql(sql).type("RecordType(ANY NEWID) NOT NULL");
  }

  @Test public void testStreamTumble() {
    // TUMBLE
    sql("select stream tumble_end(rowtime, interval '2' hour) as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId").ok();
    sql("select stream ^tumble(rowtime, interval '2' hour)^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId")
        .fails("Group function 'TUMBLE' can only appear in GROUP BY clause");
    // TUMBLE with align argument
    sql("select stream\n"
        + "  tumble_end(rowtime, interval '2' hour, time '00:12:00') as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour, time '00:12:00')").ok();
    // TUMBLE_END without corresponding TUMBLE
    sql("select stream\n"
        + "  ^tumble_end(rowtime, interval '2' hour, time '00:13:00')^ as rowtime\n"
        + "from orders\n"
        + "group by floor(rowtime to hour)")
        .fails("Call to auxiliary group function 'TUMBLE_END' must have "
            + "matching call to group function 'TUMBLE' in GROUP BY clause");
    // Arguments to TUMBLE_END are slightly different to arguments to TUMBLE
    sql("select stream\n"
        + "  ^tumble_start(rowtime, interval '2' hour, time '00:13:00')^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour, time '00:12:00')")
        .fails("Call to auxiliary group function 'TUMBLE_START' must have "
            + "matching call to group function 'TUMBLE' in GROUP BY clause");
    // Even though align defaults to TIME '00:00:00', we need structural
    // equivalence, not semantic equivalence.
    sql("select stream\n"
        + "  ^tumble_end(rowtime, interval '2' hour, time '00:00:00')^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour)")
        .fails("Call to auxiliary group function 'TUMBLE_END' must have "
            + "matching call to group function 'TUMBLE' in GROUP BY clause");
    // TUMBLE query produces no monotonic column - OK
    sql("select stream productId\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId").ok();
    sql("select stream productId\n"
        + "from orders\n"
        + "^group by productId,\n"
        + "  tumble(timestamp '1990-03-04 12:34:56', interval '2' hour)^")
        .fails(STR_AGG_REQUIRES_MONO);
  }

  @Test public void testStreamHop() {
    // HOP
    sql("select stream\n"
        + "  hop_start(rowtime, interval '1' hour, interval '3' hour) as rowtime,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour)").ok();
    sql("select stream\n"
        + "  ^hop_start(rowtime, interval '1' hour, interval '2' hour)^,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour)")
        .fails("Call to auxiliary group function 'HOP_START' must have "
            + "matching call to group function 'HOP' in GROUP BY clause");
    // HOP with align
    sql("select stream\n"
        + "  hop_start(rowtime, interval '1' hour, interval '3' hour,\n"
        + "    time '12:34:56') as rowtime,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour,\n"
        + "    time '12:34:56')").ok();
  }

  @Test public void testStreamSession() {
    // SESSION
    sql("select stream session_start(rowtime, interval '1' hour) as rowtime,\n"
        + "  session_end(rowtime, interval '1' hour),\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by session(rowtime, interval '1' hour)").ok();
  }

  @Test public void testInsertExtendedColumn() {
    sql("insert into empdefaults(extra BOOLEAN, note VARCHAR)"
        + " (deptno, empno, ename, extra, note) values (1, 10, '2', true, 'ok')").ok();
    sql("insert into emp(\"rank\" INT, extra BOOLEAN)"
        + " values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,"
        + "  1, false, 100, false)").ok();
  }

  @Test public void testInsertBindExtendedColumn() {
    sql("insert into empdefaults(extra BOOLEAN, note VARCHAR)"
        + " (deptno, empno, ename, extra, note) values (1, 10, '2', ?, 'ok')").ok();
    sql("insert into emp(\"rank\" INT, extra BOOLEAN)"
        + " values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,"
        + "  1, false, ?, ?)").ok();
  }

  @Test public void testInsertExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR) (deptno, empno, ename, extra2, note)\n"
        + "values (20, 10, '2', true, 'ok')";
    s.sql(sql0).ok();
    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(\"rank\" INT,"
        + " extra2 BOOLEAN)\n"
        + "values ('nom', 1, 'job', 20, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1, false)";
    s.sql(sql1).ok();
  }

  @Test public void testInsertBindExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN, note VARCHAR)"
        + " (deptno, empno, ename, extra2, note) values (20, 10, '2', true, ?)").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2(\"rank\" INT, extra2 BOOLEAN)"
        + " values ('nom', 1, 'job', 20, true, 0, false, timestamp '1970-01-01 00:00:00', 1, 1,"
        + "  ?, false)").ok();
  }

  @Test public void testInsertExtendedColumnModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR) (deptno, empno, ename, extra2, note)\n"
        + "values (^1^, 10, '2', true, 'ok')";
    final String error = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR) (deptno, empno, ename, extra2, note)\n"
        + "values (^?^, 10, '2', true, 'ok')";
    s.sql(sql1).fails(error);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(\"rank\" INT,"
        + " extra2 BOOLEAN)\n"
        + "values ('nom', 1, 'job', ^0^, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1, false)";
    s.sql(sql2).fails(error);
  }

  @Test public void testInsertExtendedColumnModifiableViewFailColumnCount() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into ^EMP_MODIFIABLEVIEW2(\"rank\" INT, extra2 BOOLEAN)^"
        + " values ('nom', 1, 'job', 0, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1)";
    final String error0 = "Number of INSERT target columns \\(12\\) does not"
        + " equal number of source items \\(11\\)";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into ^EMP_MODIFIABLEVIEW2(\"rank\" INT, extra2 BOOLEAN)^"
        + " (deptno, empno, ename, extra2, \"rank\") values (?, 10, '2', true)";
    final String error1 = "Number of INSERT target columns \\(5\\) does not"
        + " equal number of source items \\(4\\)";
    s.sql(sql1).fails(error1);
  }

  @Test public void testInsertExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(extcol INT,"
        + " ^extcol^ BOOLEAN)\n"
        + "values ('nom', 1, 'job', 0, true, 0, false,"
        + " timestamp '1970-01-01 00:00:00', 1, 1,  1)";
    final String error = "Duplicate name 'EXTCOL' in column list";
    s.sql(sql0).fails(error);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(extcol INT,"
        + " ^extcol^ BOOLEAN) (extcol) values (1)";
    s.sql(sql1).fails(error);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(extcol INT,"
        + " ^extcol^ BOOLEAN) (extcol) values (false)";
    s.sql(sql2).fails(error);

    final String sql3 = "insert into EMP(extcol INT, ^extcol^ BOOLEAN)"
        + " (extcol) values (1)";
    s.sql(sql3).fails(error);

    final String sql4 = "insert into EMP(extcol INT, ^extcol^ BOOLEAN)"
        + " (extcol) values (false)";
    s.sql(sql4).fails(error);
  }

  @Test public void testUpdateExtendedColumn() {
    sql("update empdefaults(extra BOOLEAN, note VARCHAR)"
        + " set deptno = 1, extra = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where deptno = 10").ok();
    sql("update empdefaults(extra BOOLEAN)"
        + " set extra = true, deptno = 1, ename = 'Bob'"
        + " where deptno = 10").ok();
    sql("update empdefaults(\"empNo\" VARCHAR)"
        + " set \"empNo\" = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test public void testInsertFailDataType() {
    tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003).checkQueryFails(
        "insert into empnullables ^values ('5', 'bob')^",
        "Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR\\$0' of type CHAR\\(1\\)");
    tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003).checkQueryFails(
        "insert into empnullables (^empno^, ename) values ('5', 'bob')",
        "Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR\\$0' of type CHAR\\(1\\)");
    tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003).checkQueryFails(
        "insert into empnullables(extra BOOLEAN)"
            + " (empno, ename, ^extra^) values (5, 'bob', 'true')",
        "Cannot assign to target field 'EXTRA' of type BOOLEAN"
            + " from source field 'EXPR\\$2' of type CHAR\\(4\\)");
  }

  @Ignore("CALCITE-1727")
  @Test public void testUpdateFailDataType() {
    tester.checkQueryFails("update emp"
            + " set ^empNo^ = '5', deptno = 1, ename = 'Bob'"
            + " where deptno = 10",
        "Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR$0' of type CHAR(1)");
    tester.checkQueryFails("update emp(extra boolean)"
            + " set ^extra^ = '5', deptno = 1, ename = 'Bob'"
            + " where deptno = 10",
        "Cannot assign to target field 'EXTRA' of type BOOLEAN"
            + " from source field 'EXPR$0' of type CHAR(1)");
  }

  @Ignore("CALCITE-1727")
  @Test public void testUpdateFailCaseSensitivity() {
    tester.checkQueryFails("update empdefaults"
            + " set empNo = '5', deptno = 1, ename = 'Bob'"
            + " where deptno = 10",
        "Column 'empno' not found in any table; did you mean 'EMPNO'\\?");
  }

  @Test public void testUpdateExtendedColumnFailCaseSensitivity() {
    tester.checkQueryFails("update empdefaults(\"extra\" BOOLEAN)"
            + " set ^extra^ = true, deptno = 1, ename = 'Bob'"
            + " where deptno = 10",
        "Unknown target column 'EXTRA'");
    tester.checkQueryFails("update empdefaults(extra BOOLEAN)"
            + " set ^\"extra\"^ = true, deptno = 1, ename = 'Bob'"
            + " where deptno = 10",
        "Unknown target column 'extra'");
  }

  @Test public void testUpdateBindExtendedColumn() {
    sql("update empdefaults(extra BOOLEAN, note VARCHAR)"
        + " set deptno = 1, extra = true, empno = 20, ename = 'Bob', note = ?"
        + " where deptno = 10").ok();
    sql("update empdefaults(extra BOOLEAN)"
        + " set extra = ?, deptno = 1, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test public void testUpdateExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN, note VARCHAR)"
        + " set deptno = 20, extra2 = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where ename = 'Jane'").ok();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN)"
        + " set extra2 = true, ename = 'Bob'"
        + " where ename = 'Jane'").ok();
  }

  @Test public void testUpdateBindExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN, note VARCHAR)"
        + " set deptno = 20, extra2 = true, empno = 20, ename = 'Bob', note = ?"
        + " where ename = 'Jane'").ok();
    s.sql("update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN)"
        + " set extra2 = ?, ename = 'Bob'"
        + " where ename = 'Jane'").ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewFailConstraint() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN,"
        + " note VARCHAR)\n"
        + "set deptno = ^1^, extra2 = true, empno = 20, ename = 'Bob',"
        + " note = 'legion'\n"
        + "where ename = 'Jane'";
    final String error = "Modifiable view constraint is not satisfied"
        + " for column 'DEPTNO' of base table 'EMP_MODIFIABLEVIEW2'";
    s.sql(sql0).fails(error);

    final String sql1 = "update EMP_MODIFIABLEVIEW2(extra2 BOOLEAN)"
        + " set extra2 = true, deptno = ^1^, ename = 'Bob'"
        + " where ename = 'Jane'";
    s.sql(sql1).fails(error);
  }

  @Test public void testUpdateExtendedColumnCollision() {
    sql("update empdefaults(empno INTEGER NOT NULL, deptno INTEGER)"
        + " set deptno = 1, empno = 20, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL,"
        + " deptno INTEGER)\n"
        + "set deptno = 20, empno = 20, ename = 'Bob'\n"
        + "where empno = 10").ok();
    s.sql("update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL,"
        + " \"deptno\" BOOLEAN)\n"
        + "set \"deptno\" = true, empno = 20, ename = 'Bob'\n"
        + "where empno = 10").ok();
  }

  @Test public void testUpdateExtendedColumnFailCollision() {
    tester.checkQueryFails("update empdefaults(^empno^ BOOLEAN, deptno INTEGER)"
            + " set deptno = 1, empno = false, ename = 'Bob'"
            + " where deptno = 10",
        "Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN");
  }

  @Ignore("CALCITE-1727")
  @Test public void testUpdateExtendedColumnFailCollision2() {
    tester.checkQueryFails("update empdefaults(^\"deptno\"^ BOOLEAN)"
            + " set \"deptno\" = 1, empno = 1, ename = 'Bob'"
            + " where deptno = 10",
        "Cannot assign to target field 'deptno' of type BOOLEAN NOT NULL from source field 'deptno' of type INTEGER");
  }

  @Test public void testUpdateExtendedColumnModifiableViewFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = "update EMP_MODIFIABLEVIEW3(^empno^ BOOLEAN,"
        + " deptno INTEGER)\n"
        + "set deptno = 1, empno = false, ename = 'Bob'\n"
        + "where deptno = 10";
    final String error = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN";
    s.sql(sql).fails(error);
  }

  @Test public void testUpdateExtendedColumnModifiableViewFailExtendedCollision() {
    final String error = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXTRA' of type INTEGER";
    final String sql = "update EMP_MODIFIABLEVIEW2(^extra^ INTEGER,"
        + " deptno INTEGER)\n"
        + "set deptno = 20, empno = 20, ename = 'Bob', extra = 5\n"
        + "where empno = 10";
    sql(sql).withExtendedCatalog().fails(error);
  }

  @Test public void testUpdateExtendedColumnModifiableViewFailUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql = "update EMP_MODIFIABLEVIEW3(^comm^ BOOLEAN,"
        + " deptno INTEGER)\n"
        + "set deptno = 1, empno = 20, ename = 'Bob', comm = true\n"
        + "where deptno = 10";
    final String error = "Cannot assign to target field 'COMM' of type"
        + " INTEGER from source field 'COMM' of type BOOLEAN";
    s.sql(sql).fails(error);
  }

  @Test public void testUpdateExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "update emp(comm BOOLEAN, ^comm^ INTEGER)\n"
        + "set deptno = 1, empno = 20, ename = 'Bob', comm = 1\n"
        + "where deptno = 10";
    final String error = "Duplicate name 'COMM' in column list";
    s.sql(sql0).fails(error);

    final String sql1 = "update EMP_MODIFIABLEVIEW3(comm BOOLEAN,"
        + " ^comm^ INTEGER)\n"
        + "set deptno = 1, empno = 20, ename = 'Bob', comm = true\n"
        + "where deptno = 10";
    s.sql(sql1).fails(error);
  }

  @Test public void testInsertExtendedColumnCollision() {
    sql("insert into EMPDEFAULTS(^comm^ INTEGER) (empno, ename, job, comm)\n"
            + "values (1, 'Arthur', 'clown', 5)").ok();
  }

  @Test public void testInsertExtendedColumnModifiableViewCollision() {
    final String sql = "insert into EMP_MODIFIABLEVIEW3(^sal^ INTEGER)\n"
        + " (empno, ename, job, sal)\n"
        + "values (1, 'Arthur', 'clown', 5)";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test public void testInsertExtendedColumnModifiableViewExtendedCollision() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(^extra^ BOOLEAN)"
        + " (empno, ename, job, extra)\n"
        + "values (1, 'Arthur', 'clown', true)";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test public void testInsertExtendedColumnModifiableViewUnderlyingCollision() {
    final String sql = "insert into EMP_MODIFIABLEVIEW3(^comm^ INTEGER)\n"
        + " (empno, ename, job, comm)\n"
        + "values (1, 'Arthur', 'clown', 5)";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test public void testInsertExtendedColumnFailCollision() {
    tester.checkQueryFails("insert into EMPDEFAULTS(^comm^ BOOLEAN)"
            + " (empno, ename, job, comm)\n"
            + "values (1, 'Arthur', 'clown', true)",
        "Cannot assign to target field 'COMM' of type INTEGER"
            + " from source field 'COMM' of type BOOLEAN");
    tester.checkQueryFails("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
            + " (empno, ename, job, ^comm^)\n"
            + "values (1, 'Arthur', 'clown', true)",
        "Cannot assign to target field 'COMM' of type INTEGER"
            + " from source field 'EXPR\\$3' of type BOOLEAN");
    tester.checkQueryFails("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
            + " (empno, ename, job, ^\"comm\"^)\n"
            + "values (1, 'Arthur', 'clown', 1)",
        "Cannot assign to target field 'comm' of type BOOLEAN"
            + " from source field 'EXPR\\$3' of type INTEGER");
  }

  @Test public void testInsertExtendedColumnModifiableViewFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(^slacker^ INTEGER)"
        + " (empno, ename, job, slacker) values (1, 'Arthur', 'clown', true)";
    final String error0 = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'SLACKER' of type INTEGER";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER)"
        + " (empno, ename, job, ^slacker^) values (1, 'Arthur', 'clown', 1)";
    final String error1 = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'EXPR\\$3' of type INTEGER";
    s.sql(sql1).fails(error1);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER)"
        + " (empno, ename, job, ^\"slacker\"^)\n"
        + "values (1, 'Arthur', 'clown', true)";
    final String error2 = "Cannot assign to target field 'slacker' of type"
        + " INTEGER from source field 'EXPR\\$3' of type BOOLEAN";
    s.sql(sql2).fails(error2);
  }

  @Test public void testInsertExtendedColumnModifiableViewFailExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "insert into EMP_MODIFIABLEVIEW2(^extra^ INTEGER)"
        + " (empno, ename, job, extra) values (1, 'Arthur', 'clown', true)";
    final String error0 = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXTRA' of type INTEGER";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW2(\"extra\" INTEGER)"
        + " (empno, ename, job, ^extra^) values (1, 'Arthur', 'clown', 1)";
    final String error1 = "Cannot assign to target field 'EXTRA' of type"
        + " BOOLEAN from source field 'EXPR\\$3' of type INTEGER";
    s.sql(sql1).fails(error1);

    final String sql2 = "insert into EMP_MODIFIABLEVIEW2(\"extra\" INTEGER)"
        + " (empno, ename, job, ^\"extra\"^)\n"
        + "values (1, 'Arthur', 'clown', true)";
    final String error2 = "Cannot assign to target field 'extra' of type"
        + " INTEGER from source field 'EXPR\\$3' of type BOOLEAN";
    s.sql(sql2).fails(error2);
  }

  @Test public void testInsertExtendedColumnModifiableViewFailUnderlyingCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String error0 = "Cannot assign to target field 'COMM' of type"
        + " INTEGER from source field 'COMM' of type BOOLEAN";
    final String sql0 = "insert into EMP_MODIFIABLEVIEW3(^comm^ BOOLEAN)"
        + " (empno, ename, job, comm) values (1, 'Arthur', 'clown', true)";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into EMP_MODIFIABLEVIEW3(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^comm^) values (1, 'Arthur', 'clown', 5)";
    final String error1 = "Unknown target column 'COMM'";
    s.sql(sql1).fails(error1);


    final String sql2 = "insert into EMP_MODIFIABLEVIEW3(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^\"comm\"^) values (1, 'Arthur', 'clown', 1)";
    final String error2 = "Cannot assign to target field 'comm' of type"
        + " BOOLEAN from source field 'EXPR\\$3' of type INTEGER";
    s.sql(sql2).fails(error2);
  }

  @Test public void testDelete() {
    sql("delete from empdefaults where deptno = 10").ok();
  }

  @Test public void testDeleteExtendedColumn() {
    sql("delete from empdefaults(extra BOOLEAN) where deptno = 10").ok();
    sql("delete from empdefaults(extra BOOLEAN) where extra = false").ok();
  }

  @Test public void testDeleteBindExtendedColumn() {
    sql("delete from empdefaults(extra BOOLEAN) where deptno = ?").ok();
    sql("delete from empdefaults(extra BOOLEAN) where extra = ?").ok();
  }

  @Test public void testDeleteModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("delete from EMP_MODIFIABLEVIEW2 where deptno = 10").ok();
    s.sql("delete from EMP_MODIFIABLEVIEW2 where deptno = 20").ok();
    s.sql("delete from EMP_MODIFIABLEVIEW2 where empno = 30").ok();
  }

  @Test public void testDeleteExtendedColumnModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("delete from EMP_MODIFIABLEVIEW2(extra BOOLEAN) where sal > 10")
        .ok();
    s.sql("delete from EMP_MODIFIABLEVIEW2(note BOOLEAN) where note = 'fired'")
        .ok();
  }

  @Test public void testDeleteExtendedColumnCollision() {
    final String sql =
        "delete from emp(empno INTEGER NOT NULL) where sal > 10";
    sql(sql).withExtendedCatalog().ok();
  }

  @Test public void testDeleteExtendedColumnModifiableViewCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "delete from EMP_MODIFIABLEVIEW2("
        + "empno INTEGER NOT NULL) where sal > 10";
    s.sql(sql0).ok();
    final String sql1 = "delete from EMP_MODIFIABLEVIEW2(\"empno\" INTEGER)\n"
        + "where sal > 10";
    s.sql(sql1).ok();
    final String sql2 = "delete from EMP_MODIFIABLEVIEW2(extra BOOLEAN)\n"
        + "where sal > 10";
    s.sql(sql2).ok();
    final String sql3 = "delete from EMP_MODIFIABLEVIEW2(\"extra\" VARCHAR)\n"
        + "where sal > 10";
    s.sql(sql3).ok();
    final String sql4 = "delete from EMP_MODIFIABLEVIEW3(comm INTEGER)\n"
        + "where sal > 10";
    s.sql(sql4).ok();
    final String sql5 = "delete from EMP_MODIFIABLEVIEW3(\"comm\" BIGINT)\n"
        + "where sal > 10";
    s.sql(sql5).ok();
  }

  @Test public void testDeleteExtendedColumnFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "delete from EMP_MODIFIABLEVIEW2(^empno^ BOOLEAN)\n"
        + "where sal > 10";
    final String error0 = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN";
    s.sql(sql0).fails(error0);
    final String sql1 = "delete from EMP_MODIFIABLEVIEW2(^empno^ INTEGER)\n"
        + "where sal > 10";
    final String error = "Cannot assign to target field 'EMPNO' of type"
        + " INTEGER NOT NULL from source field 'EMPNO' of type INTEGER";
    s.sql(sql1).fails(error);
    final String sql2 = "delete from EMP_MODIFIABLEVIEW2(^\"EMPNO\"^ INTEGER)"
        + " where sal > 10";
    s.sql(sql2).fails(error);
    s.sql(sql1).fails(error);
  }

  @Test public void testDeleteExtendedColumnModifiableViewFailCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String sql0 = "delete from EMP_MODIFIABLEVIEW(^deptno^ BOOLEAN)\n"
        + "where sal > 10";
    final String error = "Cannot assign to target field 'DEPTNO' of type"
        + " INTEGER from source field 'DEPTNO' of type BOOLEAN";
    s.sql(sql0).fails(error);
    final String sql1 = "delete from EMP_MODIFIABLEVIEW(^\"DEPTNO\"^ BOOLEAN)"
        + " where sal > 10";
    s.sql(sql1).fails(error);
  }

  @Test public void testDeleteExtendedColumnModifiableViewFailExtendedCollision() {
    final Sql s = sql("?").withExtendedCatalog();
    final String error = "Cannot assign to target field 'SLACKER' of type"
        + " BOOLEAN from source field 'SLACKER' of type INTEGER";
    final String sql0 = "delete from EMP_MODIFIABLEVIEW(^slacker^ INTEGER)\n"
        + "where sal > 10";
    s.sql(sql0).fails(error);
    final String sql1 = "delete from EMP_MODIFIABLEVIEW(^\"SLACKER\"^ INTEGER)"
        + " where sal > 10";
    s.sql(sql1).fails(error);
  }

  @Test public void testDeleteExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    sql("delete from emp (extra VARCHAR, ^extra^ VARCHAR)")
        .fails("Duplicate name 'EXTRA' in column list");
    s.sql("delete from EMP_MODIFIABLEVIEW (extra VARCHAR, ^extra^ VARCHAR)"
            + " where extra = 'test'")
        .fails("Duplicate name 'EXTRA' in column list");
    s.sql("delete from EMP_MODIFIABLEVIEW (extra VARCHAR, ^\"EXTRA\"^ VARCHAR)"
            + " where extra = 'test'")
        .fails("Duplicate name 'EXTRA' in column list");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1804">[CALCITE-1804]
   * Cannot assign NOT NULL array to nullable array</a>. */
  @Test public void testArrayAssignment() {
    final SqlTypeFactoryImpl typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType bigintNullable =
        typeFactory.createTypeWithNullability(bigint, true);
    final RelDataType bigintNotNull =
        typeFactory.createTypeWithNullability(bigint, false);
    final RelDataType date = typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType dateNotNull =
        typeFactory.createTypeWithNullability(date, false);
    assertThat(SqlTypeUtil.canAssignFrom(bigintNullable, bigintNotNull),
        is(true));
    assertThat(SqlTypeUtil.canAssignFrom(bigintNullable, dateNotNull),
        is(false));
    final RelDataType bigintNullableArray =
        typeFactory.createArrayType(bigintNullable, -1);
    final RelDataType bigintArrayNullable =
        typeFactory.createTypeWithNullability(bigintNullableArray, true);
    final RelDataType bigintNotNullArray =
        new ArraySqlType(bigintNotNull, false);
    assertThat(SqlTypeUtil.canAssignFrom(bigintArrayNullable, bigintNotNullArray),
        is(true));
    final RelDataType dateNotNullArray =
        new ArraySqlType(dateNotNull, false);
    assertThat(SqlTypeUtil.canAssignFrom(bigintArrayNullable, dateNotNullArray),
        is(false));
  }

  @Test public void testSelectRolledUpColumn() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in SELECT";

    sql("select ^slackingmin^ from emp_r")
            .fails(error);

    sql("select a from (select ^slackingmin^ from emp_r)")
            .fails(error);

    sql("select ^slackingmin^ as b from emp_r")
            .fails(error);

    sql("select empno, ^slackingmin^ from emp_r")
            .fails(error);

    sql("select slackingmin from (select empno as slackingmin from emp_r)").ok();

    sql("select ^emp_r.slackingmin^ from emp_r")
            .fails(error);

    sql("select ^sales.emp_r.slackingmin^ from sales.emp_r")
            .fails(error);

    sql("select ^sales.emp_r.slackingmin^ from emp_r")
            .fails(error);

    sql("select ^catalog.sales.emp_r.slackingmin^ from emp_r")
            .fails(error);

    sql("select (select ^slackingmin^ from emp_r), a from (select empno as a from emp_r)")
            .fails(error);

    sql("select (((^slackingmin^))) from emp_r")
            .fails(error);

    sql("select ^slackingmin^ from nest.emp_r")
            .fails(error);

    sql("with emp_r as (select 1 as slackingmin) select slackingmin from emp_r")
            .ok();

    sql("with emp_r as (select ^slackingmin^ from emp_r) select slackingmin from emp_r")
            .fails(error);

    sql("with emp_r1 as (select 1 as slackingmin) select emp_r1.slackingmin from emp_r, emp_r1")
            .ok();

    sql("with emp_r1 as (select 1 as slackingmin) select ^emp_r.slackingmin^ from emp_r, emp_r1")
            .fails(error);
  }

  @Test public void testSelectAggregateOnRolledUpColumn() {
    final String maxError = "Rolled up column 'SLACKINGMIN' is not allowed in MAX";
    final String plusError = "Rolled up column 'SLACKINGMIN' is not allowed in PLUS";

    sql("select max(^slackingmin^) from emp_r")
            .fails(maxError);

    sql("select count(slackingmin) from emp_r").ok();

    sql("select count(empno, deptno, slackingmin) from emp_r").ok();

    sql("select sum(slackingmin) from emp_r").ok();

    sql("select empno, min(slackingmin) from emp_r group by empno").ok();

    sql("select count(distinct slackingmin) from emp_r").ok();

    sql("select sum(empno + ^slackingmin^) from emp_r")
            .fails(plusError);

    sql("select max(^slackingmin^) over t as a "
            + "from emp_r window t as (partition by empno order by empno)")
            .fails(maxError);
  }

  @Test public void testRolledUpColumnInWhere() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in GREATER_THAN";

    // Fire these slackers!!
    sql("select empno from emp_r where slacker and ^slackingmin^ > 60")
            .fails(error);

    sql("select sum(slackingmin) filter (where slacker and ^slackingmin^ > 60) from emp_r")
            .fails(error);
  }

  @Test public void testRolledUpColumnInHaving() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in SUM";

    sql("select deptno, sum(slackingmin) from emp_r group "
            + "by deptno having sum(^slackingmin^) > 1000")
            .fails(error);
  }

  @Test public void testRollUpInWindow() {
    final String partitionError = "Rolled up column 'SLACKINGMIN' is not allowed in PARTITION BY";
    final String orderByError = "Rolled up column 'SLACKINGMIN' is not allowed in ORDER BY";

    sql("select empno, sum(slackingmin) over (partition by ^slackingmin^) from emp_r")
            .fails(partitionError);

    sql("select empno, sum(slackingmin) over (partition by empno, ^slackingmin^) from emp_r")
            .fails(partitionError);

    sql("select empno, sum(slackingmin) over (partition by empno order by ^slackingmin^) "
            + "from emp_r")
            .fails(orderByError);

    sql("select empno, sum(slackingmin) over slackingmin "
            + "from emp_r window slackingmin as (partition by ^slackingmin^)")
            .fails(partitionError);

    sql("select sum(slackingmin) over t "
            + "from emp_r window t as (partition by empno order by ^slackingmin^, empno)")
            .fails(orderByError);

    sql("select sum(slackingmin) over t as a "
            + "from emp_r window t as (partition by empno order by ^slackingmin^, empno)")
            .fails(orderByError);
  }

  @Test public void testRollUpInGroupBy() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in GROUP BY";

    sql("select empno, count(distinct empno) from emp_r group by empno, ^slackingmin^")
            .fails(error);

    sql("select empno from emp_r group by grouping sets (empno, ^slackingmin^)")
            .fails(error);
  }

  @Test public void testRollUpInOrderBy() {
    final String error = "Rolled up column 'SLACKINGMIN' is not allowed in ORDER BY";

    sql("select empno from emp_r order by ^slackingmin^ asc")
            .fails(error);

    sql("select slackingmin from (select empno as slackingmin from emp_r) order by slackingmin")
            .ok();

    sql("select empno, sum(slackingmin) from emp_r group by empno order by sum(slackingmin)").ok();
  }

  @Test public void testRollUpInJoin() {
    final String onError = "Rolled up column 'SLACKINGMIN' is not allowed in ON";
    final String usingError = "Rolled up column 'SLACKINGMIN' is not allowed in USING";
    final String selectError = "Rolled up column 'SLACKINGMIN' is not allowed in SELECT";

    sql("select * from (select deptno, ^slackingmin^ from emp_r) join dept using (deptno)")
            .fails(selectError);

    sql("select * from dept as a join (select deptno, ^slackingmin^ from emp_r) using (deptno)")
            .fails(selectError);

    sql("select * from emp_r as a join dept_r as b using (deptno, ^slackingmin^)")
            .fails(usingError);

    // Even though the emp_r.slackingmin column will be under the SqlNode for '=',
    // The error should say it happened in 'ON' instead
    sql("select * from emp_r join dept_r on (^emp_r.slackingmin^ = dept_r.slackingmin)")
            .fails(onError);
  }
}

  @Test void testJsonValueExpressionOperator() {
    expr("'{}' format json").ok();
    expr("'{}' format json encoding utf8").ok();
    expr("'{}' format json encoding utf16").ok();
    expr("'{}' format json encoding utf32").ok();
    expr("'{}' format json")
        .columnType("ANY NOT NULL");
    expr("'null' format json")
        .columnType("ANY NOT NULL");
    expr("cast(null as varchar) format json")
        .columnType("ANY");
    expr("null format json")
        .columnType("ANY");
    expr("^null^ format json")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");
  }

  @Test void testJsonExists() {
    expr("json_exists('{}', 'lax $')").ok();
    expr("json_exists('{}', 'lax $')")
        .columnType("BOOLEAN");
  }

  @Test void testJsonValue() {
    expr("json_value('{\"foo\":\"bar\"}', 'lax $.foo')").ok();
    expr("json_value('{\"foo\":\"bar\"}', 123)").ok();
    expr("json_value('{\"foo\":\"bar\"}', 'lax $.foo')")
        .columnType("VARCHAR(2000)");
    expr("json_value('{\"foo\":100}', 'lax $.foo')")
        .columnType("VARCHAR(2000)");
    expr("json_value('{\"foo\":100}', 'lax $.foo'"
        + "returning integer)")
        .columnType("INTEGER");
    expr("json_value('{\"foo\":100}', 'lax $.foo'"
        + "returning integer default 0 on empty default 0 on error)")
        .columnType("INTEGER");
    expr("json_value('{\"foo\":100}', 'lax $.foo'"
        + "returning integer default null on empty default null on error)")
        .columnType("INTEGER");

    expr("json_value('{\"foo\":true}', 'lax $.foo'"
        + "returning boolean default 100 on empty default 100 on error)")
        .columnType("BOOLEAN");

    // test type inference of default value
    expr("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on empty)")
        .columnType("VARCHAR(2000)");
    expr("json_value('{\"foo\":100}', 'lax $.foo' returning boolean"
        + " default 100 on empty)")
        .columnType("BOOLEAN");
  }

  @Test void testJsonQuery() {
    expr("json_query('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_query('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' WITH WRAPPER)")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' EMPTY OBJECT ON EMPTY)")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' EMPTY ARRAY ON ERROR)")
        .columnType("VARCHAR(2000)");
    expr("json_query('{\"foo\":\"bar\"}', 'strict $' EMPTY OBJECT ON EMPTY "
        + "EMPTY ARRAY ON ERROR EMPTY ARRAY ON EMPTY NULL ON ERROR)")
        .columnType("VARCHAR(2000)");
  }

  @Test void testJsonArray() {
    expr("json_array()").ok();
    expr("json_array('foo', 'bar')").ok();
    expr("json_array('foo', 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonArrayAgg() {
    sql("select json_arrayagg(ename) from emp").ok();
    expr("json_arrayagg('foo')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonObject() {
    expr("json_object()").ok();
    expr("json_object('foo': 'bar')").ok();
    expr("json_object('foo': 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
    expr("^json_object(100: 'bar')^")
        .fails("(?s).*Expected a character type*");
  }

  @Test void testJsonPretty() {
    sql("select json_pretty(ename) from emp").ok();
    expr("json_pretty('{\"foo\":\"bar\"}')").ok();
    expr("json_pretty('{\"foo\":\"bar\"}')")
        .columnType("VARCHAR(2000)");
    sql("select json_pretty(^NULL^) from emp")
        .withTypeCoercion(false)
        .fails("(?s).*Illegal use of .NULL.*");
    sql("select json_pretty(NULL) from emp").ok();

    if (!Bug.CALCITE_2869_FIXED) {
      // the case should throw an error but currently validation
      // is done during sql-to-rel process.
      //
      // see StandardConvertletTable.JsonOperatorValueExprConvertlet
      return;
    }
    sql("select json_pretty(^1^) from emp")
        .fails("(.*)JSON_VALUE_EXPRESSION(.*)");
  }

  @Test void testJsonStorageSize() {
    sql("select json_storage_size(ename) from emp").ok();
    expr("json_storage_size('{\"foo\":\"bar\"}')").ok();
    expr("json_storage_size('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");
  }

  @Test void testJsonType() {
    sql("select json_type(ename) from emp").ok();
    expr("json_type('{\"foo\":\"bar\"}')").ok();
    expr("json_type('{\"foo\":\"bar\"}')")
        .columnType("VARCHAR(20)");

    if (!Bug.CALCITE_2869_FIXED) {
      return;
    }
    sql("select json_type(^1^) from emp")
        .fails("(.*)JSON_VALUE_EXPRESSION(.*)");
  }

  @Test void testJsonDepth() {
    sql("select json_depth(ename) from emp").ok();
    expr("json_depth('{\"foo\":\"bar\"}')").ok();
    expr("json_depth('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");

    if (!Bug.CALCITE_2869_FIXED) {
      return;
    }
  }

  @Test void testJsonLength() {
    expr("json_length('{\"foo\":\"bar\"}')").ok();
    expr("json_length('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_length('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");
    expr("json_length('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("INTEGER");
    expr("json_length('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("INTEGER");
  }

  @Test void testJsonKeys() {
    expr("json_keys('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_keys('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("VARCHAR(2000)");
    expr("json_keys('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("VARCHAR(2000)");
  }

  @Test void testJsonRemove() {
    expr("json_remove('{\"foo\":\"bar\"}', '$')").ok();
    expr("json_remove('{\"foo\":\"bar\"}', '$')")
        .columnType("VARCHAR(2000)");
    expr("json_remove('{\"foo\":\"bar\"}', 1, '2', 3)")
        .columnType("VARCHAR(2000)");
    expr("json_remove('{\"foo\":\"bar\"}', 1, 2, 3)")
        .columnType("VARCHAR(2000)");
    sql("select ^json_remove('{\"foo\":\"bar\"}')^")
        .fails("(?s).*Invalid number of arguments.*");
  }

  @Test void testJsonObjectAgg() {
    sql("select json_objectagg(ename: empno) from emp").ok();
    sql("select json_objectagg(empno: ename) from emp").ok();
    sql("select ^json_objectagg(empno: ename)^ from emp")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    expr("json_objectagg('foo': 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonPredicate() {
    expr("'{}' is json")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is json value")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is json object")
        .columnType("BOOLEAN NOT NULL");
    expr("'[]' is json array")
        .columnType("BOOLEAN NOT NULL");
    expr("'100' is json scalar")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is not json")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is not json value")
        .columnType("BOOLEAN NOT NULL");
    expr("'{}' is not json object")
        .columnType("BOOLEAN NOT NULL");
    expr("'[]' is not json array")
        .columnType("BOOLEAN NOT NULL");
    expr("'100' is not json scalar")
        .columnType("BOOLEAN NOT NULL");
    expr("100 is json value")
        .columnType("BOOLEAN NOT NULL");
    expr("^100 is json value^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
  }

  @Test void testRegexpReplace() {
    final SqlOperatorTable oracleTable =
        SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
            SqlLibrary.STANDARD, SqlLibrary.ORACLE);

    expr("REGEXP_REPLACE('a b c', 'a', 'X')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 2)")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3)")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def GHI', '[a-z]+', 'X', 1, 3, 'c')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
    // Implicit type coercion.
    expr("REGEXP_REPLACE(null, '(-)', '###')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR");
    expr("REGEXP_REPLACE('100-200', null, '###')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR");
    expr("REGEXP_REPLACE('100-200', '(-)', null)")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', '2')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', '1', '3')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
    // The last argument to REGEXP_REPLACE should be specific character, but with
    // implicit type coercion, the validation still passes.
    expr("REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', '1', '3', '1')")
        .withOperatorTable(oracleTable)
        .columnType("VARCHAR NOT NULL");
  }

  @Test void testInvalidFunctionCall() {
    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);

    // With implicit type coercion.
    expr("^unknown_udf(1, 2)^")
        .fails("(?s).*No match found for function signature "
            + "UNKNOWN_UDF\\(<NUMERIC>, <NUMERIC>\\).*");
    expr("^power(cast(1 as timestamp), cast(2 as timestamp))^")
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER\\(<TIMESTAMP\\(0\\)>, <TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(cast('124' as timestamp))^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'MYFUN' to arguments of type "
            + "'MYFUN\\(<TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(1, 2)^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*No match found for function signature "
            + "MYFUN\\(<NUMERIC>, <NUMERIC>\\).*");

    // Without implicit type coercion.
    expr("^unknown_udf(1, 2)^")
        .withTypeCoercion(false)
        .fails("(?s).*No match found for function signature "
            + "UNKNOWN_UDF\\(<NUMERIC>, <NUMERIC>\\).*");
    expr("^power(cast(1 as timestamp), cast(2 as timestamp))^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER\\(<TIMESTAMP\\(0\\)>, <TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(cast('124' as timestamp))^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'MYFUN' to arguments of type "
            + "'MYFUN\\(<TIMESTAMP\\(0\\)>\\)'.*");
    expr("^myFUN(1, 2)^")
        .withCaseSensitive(true)
        .withOperatorTable(operatorTable)
        .withTypeCoercion(false)
        .fails("(?s).*No match found for function signature "
            + "MYFUN\\(<NUMERIC>, <NUMERIC>\\).*");
  }

  @Test void testValidatorReportsOriginalQueryUsingReader()
      throws Exception {
    final String sql = "select a from b";
    final SqlParser.Config config = configBuilder().build();
    final String messagePassingSqlString;
    try {
      final SqlParser sqlParserReader = SqlParser.create(sql, config);
      final SqlNode node = sqlParserReader.parseQuery();
      final SqlValidator validator = tester.getValidator();
      final SqlNode x = validator.validate(node);
      fail("expecting an error, got " + x);
      return;
    } catch (CalciteContextException error) {
      // we want the exception to report column and line
      assertThat(error.getMessage(),
          is("At line 1, column 15: Object 'B' not found"));
      messagePassingSqlString = error.getMessage();
      // the error does not contain the original query (even using a String)
      assertNull(error.getOriginalStatement());
    }

    // parse SQL text using a java.io.Reader and not a java.lang.String
    try {
      final SqlParser sqlParserReader =
          SqlParser.create(new StringReader(sql), config);
      final SqlNode node = sqlParserReader.parseQuery();
      final SqlValidator validator = tester.getValidator();
      final SqlNode x = validator.validate(node);
      fail("expecting an error, got " + x);
    } catch (CalciteContextException error) {
      // we want exactly the same error as with java.lang.String input
      assertThat(error.getMessage(), is(messagePassingSqlString));
      // the error does not contain the original query (using a Reader)
      assertThat(error.getOriginalStatement(), nullValue());
    }
  }

  @Test void testValidateParameterizedExpression() throws SqlParseException {
    final SqlParser.Config config = configBuilder().build();
    final SqlValidator validator = tester.getValidator();
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType intTypeNull = typeFactory.createTypeWithNullability(intType, true);
    final Map<String, RelDataType> nameToTypeMap = new HashMap<>();
    nameToTypeMap.put("A", intType);
    nameToTypeMap.put("B", intTypeNull);
    final String expr = "a + b";
    final SqlParser parser = SqlParser.create(expr, config);
    final SqlNode sqlNode = parser.parseExpression();
    final SqlNode validated = validator.validateParameterizedExpression(sqlNode, nameToTypeMap);
    final RelDataType resultType = validator.getValidatedNodeType(validated);
    assertThat(resultType.toString(), is("INTEGER"));
  }

}
