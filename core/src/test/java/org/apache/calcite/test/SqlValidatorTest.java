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
  @Test public void testCaseInsensitiveTableAlias() {
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
  @Test public void testCaseInsensitiveTableAliasInGroupBy() {
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
    sql("select * from ^unknownTable^")
        .fails("Object 'UNKNOWNTABLE' not found");

    // Similar table exists in default schema
    sql("select * from ^\"Emp\"^")
        .fails("Object 'Emp' not found within 'SALES'; did you mean 'EMP'\\?");

    // Schema correct, but no table in specified schema
    sql("select * from ^sales.unknownTable^")
        .fails("Object 'UNKNOWNTABLE' not found within 'SALES'");
    // Similar table exists in specified schema
    sql("select * from ^sales.\"Emp\"^")
        .fails("Object 'Emp' not found within 'SALES'; did you mean 'EMP'\\?");

    // No schema found
    sql("select * from ^unknownSchema.unknownTable^")
        .fails("Object 'UNKNOWNSCHEMA.UNKNOWNTABLE' not found");
    // Similar schema found
    sql("select * from ^\"sales\".emp^")
        .fails("Object 'sales' not found; did you mean 'SALES'\\?");
    sql("select * from ^\"saLes\".\"eMp\"^")
        .fails("Object 'saLes' not found; did you mean 'SALES'\\?");

    // Spurious after table
    sql("select * from ^emp.foo^")
        .fails("Object 'FOO' not found within 'EMP'");
    sql("select * from ^sales.emp.foo^")
        .fails("Object 'FOO' not found within 'SALES\\.EMP'");

    // Alias not found
    sql("select ^aliAs^.\"name\"\n"
        + "from sales.emp as \"Alias\"")
        .fails("Table 'ALIAS' not found; did you mean 'Alias'\\?");
    // Alias not found, fully-qualified
    sql("select ^sales.\"emp\"^.\"name\" from sales.emp")
        .fails("Table 'SALES\\.emp' not found; did you mean 'EMP'\\?");
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
  @Test public void testUnquotedBuiltInFunctionNames() {
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
        + "LIKE ALL -\n"
        + "LIKE ANY -\n"
        + "LIKE SOME -\n"
        + "NEGATED POSIX REGEX CASE INSENSITIVE left\n"
        + "NEGATED POSIX REGEX CASE SENSITIVE left\n"
        + "NOT BETWEEN ASYMMETRIC -\n"
        + "NOT BETWEEN SYMMETRIC -\n"
        + "NOT IN left\n"
        + "NOT LIKE -\n"
        + "NOT LIKE ALL -\n"
        + "NOT LIKE ANY -\n"
        + "NOT LIKE SOME -\n"
        + "NOT SIMILAR TO -\n"
        + "POSIX REGEX CASE INSENSITIVE left\n"
        + "POSIX REGEX CASE SENSITIVE left\n"
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
        + "FORMAT JSON post\n"
        + "IS A SET post\n"
        + "IS EMPTY post\n"
        + "IS FALSE post\n"
        + "IS JSON ARRAY post\n"
        + "IS JSON OBJECT post\n"
        + "IS JSON SCALAR post\n"
        + "IS JSON VALUE post\n"
        + "IS NOT A SET post\n"
        + "IS NOT EMPTY post\n"
        + "IS NOT FALSE post\n"
        + "IS NOT JSON ARRAY post\n"
        + "IS NOT JSON OBJECT post\n"
        + "IS NOT JSON SCALAR post\n"
        + "IS NOT JSON VALUE post\n"
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
        + "FILTER left\n"
        + "IGNORE NULLS -\n"
        + "OVER left\n"
        + "RESPECT NULLS -\n"
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
    sql("insert into EMP ([EMPNO], deptno, ^[empno]^)\n"
        + " values (1, 1, 1)")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET)
        .fails("Target column 'EMPNO' is assigned more than once");
  }

  /** Tests referencing columns from a sub-query that has duplicate column
   * names. (The standard says it should be an error, but we don't right
   * now.) */
  @Test public void testCaseInsensitiveSubQuery() {
    final Sql insensitive = sql("?")
        .withCaseSensitive(false)
        .withQuoting(Quoting.BRACKET);
    final Sql sensitive = sql("?")
        .withCaseSensitive(true)
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BRACKET);
    String sql = "select [e] from (\n"
        + "select EMPNO as [e], DEPTNO as d, 1 as [e2] from EMP)";
    sensitive.sql(sql).ok();
    insensitive.sql(sql).ok();

    String sql1 = "select e2 from (\n"
        + "select EMPNO as [e2], DEPTNO as d, 1 as [E] from EMP)";
    insensitive.sql(sql1).ok();
    sensitive.sql(sql1).ok();
  }

  /** Tests using case-insensitive matching of table names. */
  @Test public void testCaseInsensitiveTables() {
    final Sql mssql = sql("?").withLex(Lex.SQL_SERVER);
    mssql.sql("select eMp.* from (select * from emp) as EmP").ok();
    mssql.sql("select ^eMp^.* from (select * from emp as EmP)")
        .fails("Unknown identifier 'eMp'");
    mssql.sql("select eMp.* from (select * from emP) as EmP").ok();
    mssql.sql("select eMp.empNo from (select * from emP) as EmP").ok();
    mssql.sql("select empNo from (select Empno from emP) as EmP").ok();
    mssql.sql("select empNo from (select Empno from emP)").ok();
  }

  @Test public void testInsert() {
    sql("insert into empnullables (empno, ename)\n"
        + "values (1, 'Ambrosia')").ok();
    sql("insert into empnullables (empno, ename)\n"
        + "select 1, 'Ardy' from (values 'a')").ok();

    final String sql = "insert into emp\n"
        + "values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00', 1, 1,\n"
        + "  1, false)";
    sql(sql).ok();

    final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno, slacker)\n"
        + "select 1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00',\n"
        + "  1, 1, 1, false\n"
        + "from (values 'a')";
    sql(sql2).ok();

    sql("insert into empnullables (ename, empno, deptno)\n"
        + "values ('Pat', 1, null)").ok();

    final String sql3 = "insert into empnullables (\n"
        + "  empno, ename, job, hiredate)\n"
        + "values (1, 'Jim', 'Baker', timestamp '1970-01-01 00:00:00')";
    sql(sql3).ok();

    sql("insert into empnullables (empno, ename)\n"
        + "select 1, 'b' from (values 'a')").ok();

    sql("insert into empnullables (empno, ename)\n"
        + "values (1, 'Karl')").ok();
  }

  @Test public void testInsertWithNonEqualSourceSinkFieldsNum() {
    sql("insert into ^dept^ select sid, ename, deptno "
        + "from "
        + "(select sum(empno) as sid, ename, deptno, sal "
        + "from emp group by ename, deptno, sal)")
        .fails("Number of INSERT target columns \\(2\\) "
            + "does not equal number of source items \\(3\\)");
  }

  @Test public void testInsertSubset() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    final String sql1 = "insert into empnullables\n"
        + "values (1, 'nom', 'job', 0, timestamp '1970-01-01 00:00:00')";
    s.sql(sql1).ok();

    final String sql2 = "insert into empnullables\n"
        + "values (1, 'nom', null, 0, null)";
    s.sql(sql2).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1510">[CALCITE-1510]
   * INSERT/UPSERT should allow fewer values than columns</a>,
   * check for default value only when target field is null. */
  @Test public void testInsertShouldNotCheckForDefaultValue() {
    final int c = CountingFactory.THREAD_CALL_COUNT.get().get();
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    final String sql1 = "insert into emp values(1, 'nom', 'job', 0, "
        + "timestamp '1970-01-01 00:00:00', 1, 1, 1, false)";
    s.sql(sql1).ok();
    assertThat("Should not check for default value if column is in INSERT",
        CountingFactory.THREAD_CALL_COUNT.get().get(), is(c));

    // Now add a list of target columns, keeping the query otherwise the same.
    final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno, slacker)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, 1, false)";
    s.sql(sql2).ok();
    assertThat("Should not check for default value if column is in INSERT",
        CountingFactory.THREAD_CALL_COUNT.get().get(), is(c));

    // Now remove SLACKER, which is NOT NULL, from the target list.
    final String sql3 = "insert into ^emp^ (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, deptno)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, 1)";
    s.sql(sql3)
        .fails("Column 'SLACKER' has no default value and does not allow NULLs");
    assertThat("Should not check for default value, even if if column is "
            + "missing from INSERT and nullable",
        CountingFactory.THREAD_CALL_COUNT.get().get(),
        is(c));

    // Now remove DEPTNO, which has a default value, from the target list.
    // Will generate an extra call to newColumnDefaultValue at sql-to-rel time,
    // just not yet.
    final String sql4 = "insert into ^emp^ (empno, ename, job, mgr, hiredate,\n"
        + "  sal, comm, slacker)\n"
        + "values(1, 'nom', 'job', 0,\n"
        + "  timestamp '1970-01-01 00:00:00', 1, 1, false)";
    s.sql(sql4).ok();
    assertThat("Missing DEFAULT column generates a call to factory",
        CountingFactory.THREAD_CALL_COUNT.get().get(),
        is(c));
  }

  @Test public void testInsertView() {
    sql("insert into empnullables_20 (ename, empno, comm)\n"
        + "values ('Karl', 1, 1)").ok();
  }

  @Test public void testInsertSubsetView() {
    sql("insert into empnullables_20\n"
        + "values (1, 'Karl')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
  }

  @Test public void testInsertModifiableView() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into EMP_MODIFIABLEVIEW (empno, ename, job)\n"
        + "values (1, 'Arthur', 'clown')").ok();
    s.sql("insert into EMP_MODIFIABLEVIEW2 (empno, ename, job, extra)\n"
        + "values (1, 'Arthur', 'clown', true)").ok();
  }

  @Test public void testInsertSubsetModifiableView() {
    final Sql s = sql("?").withExtendedCatalog()
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003);
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
    sql(sql0).withExtendedCatalog()
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2)")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");

    final String sql1 = "insert into empnullables\n"
        + " (empno, ename, dynamic_column double not null)\n"
        + "values (?, ?, ?)";
    sql(sql1)
        .withExtendedCatalog()
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, DOUBLE ?2)")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");

    final String sql2 = "insert into struct.t_extend\n"
        + " (f0.c0, f1.c1, \"F2\".\"C2\" varchar(20) not null)\n"
        + "values (?, ?, ?)";
    sql(sql2)
        .withExtendedCatalog()
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok()
        .bindType("RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Extended columns not allowed under "
            + "the current SQL conformance level");
  }

  @Test public void testInsertBindSubset() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    // VALUES
    final String sql0 = "insert into empnullables\n"
        + "values (?, ?, ?)";
    s.sql(sql0).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2)");

    // multiple VALUES
    final String sql1 = "insert into empnullables\n"
        + "values (?, 'Pat', 'Tailor'), (2, ?, ?),\n"
        + " (3, 'Tod', ?), (4, 'Arthur', null)";
    s.sql(sql1).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1, VARCHAR(10) ?2, "
            + "VARCHAR(10) ?3)");

    // VALUES with expression
    s.sql("insert into empnullables values (? + 1, ?)").ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // SELECT
    s.sql("insert into empnullables select ?, ? from (values (1))").ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // WITH
    final String sql3 = "insert into empnullables\n"
        + "with v as (values ('a'))\n"
        + "select ?, ? from (values (1))";
    s.sql(sql3).ok()
        .bindType("RecordType(INTEGER ?0, VARCHAR(20) ?1)");

    // UNION
    final String sql2 = "insert into empnullables\n"
        + "select ?, ? from (values (1))\n"
        + "union all\n"
        + "select ?, ? from (values (time '1:2:3'))";
    final String expected2 = "RecordType(INTEGER ?0, VARCHAR(20) ?1,"
        + " INTEGER ?2, VARCHAR(20) ?3)";
    s.sql(sql2).ok().bindType(expected2);
  }

  @Test public void testInsertBindView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (mgr, empno, ename)"
        + " values (?, ?, ?)";
    sql(sql).withExtendedCatalog().ok()
        .bindType("RecordType(INTEGER ?0, INTEGER ?1, VARCHAR(20) ?2)");
  }

  @Test public void testInsertModifiableViewPassConstraint() {
    sql("insert into EMP_MODIFIABLEVIEW2 (deptno, empno, ename, extra)"
        + " values (20, 100, 'Lex', true)")
        .withExtendedCatalog()
        .ok();
    sql("insert into EMP_MODIFIABLEVIEW2 (empno, ename, extra)"
        + " values (100, 'Lex', true)")
        .withExtendedCatalog()
        .ok();

    sql("insert into EMP_MODIFIABLEVIEW2 values ('Edward', 20)")
        .withExtendedCatalog()
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .ok();
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

  @Test public void testInsertTargetTableWithVirtualColumns() {
    final Sql s = sql("?").withExtendedCatalog();
    s.sql("insert into VIRTUALCOLUMNS.VC_T1\n"
        + "select a, b, c from VIRTUALCOLUMNS.VC_T2").ok();

    final String sql0 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', 3, 4)";
    final String error0 = "Cannot INSERT into generated column 'D'";
    s.sql(sql0).fails(error0);

    final String sql1 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', DEFAULT, DEFAULT)";
    s.sql(sql1).ok();

    final String sql2 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', DEFAULT)";
    final String error2 = "(?s).*Number of INSERT target columns \\(5\\) "
        + "does not equal number of source items \\(4\\).*";
    s.sql(sql2).fails(error2);

    final String sql3 = "insert into ^VIRTUALCOLUMNS.VC_T1^\n"
        + "values(1, 2, 'abc', DEFAULT, DEFAULT, DEFAULT)";
    final String error3 = "(?s).*Number of INSERT target columns \\(5\\) "
        + "does not equal number of source items \\(6\\).*";
    s.sql(sql3).fails(error3);

    final String sql4 = "insert into VIRTUALCOLUMNS.VC_T1\n"
        + "^values(1, '2', 'abc')^";
    final String error4 = "(?s).*Cannot assign to target field 'B' of type BIGINT "
        + "from source field 'EXPR\\$1' of type CHAR\\(1\\).*";
    s.sql(sql4).fails(error4);
  }

  @Test public void testInsertFailNullability() {
    sql("insert into ^empnullables^ (ename) values ('Kevin')")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    sql("insert into ^empnullables^ (empno) values (10)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into empnullables (empno, ename, deptno) ^values (5, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertSubsetFailNullability() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into ^empnullables^ values (1)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    s.sql("insert into empnullables ^values (null, 'Liam')^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into empnullables ^values (45, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertViewFailNullability() {
    sql("insert into ^empnullables_20^ (ename) values ('Jake')")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    sql("insert into ^empnullables_20^ (empno) values (9)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into empnullables_20 (empno, ename, mgr) ^values (5, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertSubsetViewFailNullability() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into ^empnullables_20^ values (1)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    s.sql("insert into empnullables_20 ^values (null, 'Liam')^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into empnullables_20 ^values (45, null)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindFailNullability() {
    sql("insert into ^emp^ (ename) values (?)")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    sql("insert into ^emp^ (empno) values (?)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into emp (empno, ename, deptno) ^values (?, null, 5)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindSubsetFailNullability() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into ^empnullables^ values (?)")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    s.sql("insert into empnullables ^values (null, ?)^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into empnullables ^values (?, null)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertSubsetDisallowed() {
    sql("insert into ^emp^ values (1)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp^ values (null)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp^ values (1, 'Kevin')")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test public void testInsertSubsetViewDisallowed() {
    sql("insert into ^emp_20^ values (1)")
        .fails("Number of INSERT target columns \\(8\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp_20^ values (null)")
        .fails("Number of INSERT target columns \\(8\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp_20^ values (?, ?)")
        .fails("Number of INSERT target columns \\(8\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test public void testInsertBindSubsetDisallowed() {
    sql("insert into ^emp^ values (?)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(1\\)");
    sql("insert into ^emp^ values (?, ?)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
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
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (^COMM^ boolean)\n"
        + " where SAL = 20")
        .fails("Cannot assign to target field 'COMM' of type INTEGER "
            + "from source field 'COMM' of type BOOLEAN");
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (^EMPNO^ integer)\n"
        + " where SAL = 20")
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL "
            + "from source field 'EMPNO' of type INTEGER");
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMPDEFAULTS extend (^\"EMPNO\"^ integer)\n"
        + " where SAL = 20")
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER NOT NULL "
            + "from source field 'EMPNO' of type INTEGER");
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
    sql("select ^\"empno\"^, ename, deptno from EMP")
        .fails("Column 'empno' not found in any table; did you mean 'EMPNO'\\?");
    sql("select ^\"extra\"^, ename, deptno from EMP (extra boolean)")
        .fails("Column 'extra' not found in any table; did you mean 'EXTRA'\\?");
    sql("select ^extra^, ename, deptno from EMP (\"extra\" boolean)")
        .fails("Column 'EXTRA' not found in any table; did you mean 'extra'\\?");
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
    sql("insert into empdefaults (deptno) values (1)").ok();
    sql("insert into empdefaults (ename, empno) values ('Quan', 50)").ok();
    sql("insert into empdefaults (ename, deptno) ^values (null, 1)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into ^empdefaults^ values (null, 'Tod')")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test public void testInsertSubsetWithCustomInitializerExpressionFactory() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    s.sql("insert into empdefaults values (101)").ok();
    s.sql("insert into empdefaults values (101, 'Coral')").ok();
    s.sql("insert into empdefaults ^values (null, 'Tod')^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
    s.sql("insert into empdefaults ^values (78, null)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindWithCustomInitializerExpressionFactory() {
    sql("insert into empdefaults (deptno) values (?)").ok()
        .bindType("RecordType(INTEGER ?0)");
    sql("insert into empdefaults (ename, empno) values (?, ?)").ok()
        .bindType("RecordType(VARCHAR(20) ?0, INTEGER ?1)");
    sql("insert into empdefaults (ename, deptno) ^values (null, ?)^")
        .fails("Column 'ENAME' has no default value and does not allow NULLs");
    sql("insert into ^empdefaults^ values (null, ?)")
        .fails("Number of INSERT target columns \\(9\\) does not equal "
            + "number of source items \\(2\\)");
  }

  @Test public void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
    final Sql s = sql("?").withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    s.sql("insert into empdefaults values (101, ?)").ok()
        .bindType("RecordType(VARCHAR(20) ?0)");
    s.sql("insert into empdefaults ^values (null, ?)^")
        .fails("Column 'EMPNO' has no default value and does not allow NULLs");
  }

  @Test public void testInsertBindWithCustomColumnResolving() {
    final SqlConformanceEnum pragmatic = SqlConformanceEnum.PRAGMATIC_2003;

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
    sql(sql2).withConformance(pragmatic).ok().bindType(expected2);

    final String sql3 =
        "insert into struct.t_nullables (f1.c0, f1.c2, f0.c1) values (?, ?, ?)";
    final String expected3 =
        "RecordType(INTEGER ?0, INTEGER ?1, INTEGER ?2)";
    sql(sql3).withConformance(pragmatic).ok().bindType(expected3);

    sql("insert into struct.t_nullables (c0, ^c4^, c1) values (?, ?, ?)")
        .withConformance(pragmatic)
        .fails("Unknown target column 'C4'");
    sql("insert into struct.t_nullables (^a0^, c2, c1) values (?, ?, ?)")
        .withConformance(pragmatic)
        .fails("Unknown target column 'A0'");
    final String sql4 = "insert into struct.t_nullables (\n"
        + "  f1.c0, ^f0.a0^, f0.c1) values (?, ?, ?)";
    sql(sql4).withConformance(pragmatic)
        .fails("Unknown target column 'F0.A0'");
    final String sql5 = "insert into struct.t_nullables (\n"
        + "  f1.c0, f1.c2, ^f1.c0^) values (?, ?, ?)";
    sql(sql5).withConformance(pragmatic)
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
    sql("select stream\n"
        + " orders.rowtime as rowtime, orders.orderId as orderId,\n"
        + " products.supplierId as supplierId\n"
        + "from orders\n"
        + "join products on orders.productId = products.productId").ok();
    sql("^select stream *\n"
        + "from products\n"
        + "join suppliers on products.supplierId = suppliers.supplierId^")
        .fails(cannotStreamResultsForNonStreamingInputs("PRODUCTS, SUPPLIERS"));
  }

  @Test public void testDummy() {
    // (To debug individual statements, paste them into this method.)
    final Sql s = sql("?").withTester(t -> t.withLenientOperatorLookup(true));
    s.sql("select count() from emp").ok();
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

  @Test public void testDescriptor() {
    sql("select * from table(tumble(table orders, descriptor(rowtime), interval '2' hour))").ok();
    sql("select * from table(tumble(table orders, descriptor(^column_not_exist^), "
        + "interval '2' hour))")
        .fails("Unknown identifier 'COLUMN_NOT_EXIST'");
  }

  @Test public void testTumbleTableValuedFunction() {
    sql("select * from table(\n"
        + "^tumble(table orders, descriptor(rowtime), interval '2' hour, 'test')^)")
        .fails("Invalid number of arguments to function 'TUMBLE'. Was expecting 3 arguments");
    sql("select rowtime, productid, orderid, 'window_start', 'window_end' from table(\n"
        + "tumble(table orders, descriptor(rowtime), interval '2' hour))").ok();
    sql("select * from table(\n"
        + "^tumble(table orders, descriptor(rowtime), 'test')^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <COLUMN_LIST>,"
            + " <CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE "
            + "table_name, DESCRIPTOR\\(col1, col2 \\.\\.\\.\\), datetime interval\\)");
    sql("select * from table(\n"
        + "^tumble(table orders, 'test', interval '2' hour)^)")
        .fails("Cannot apply 'TUMBLE' to arguments of type 'TUMBLE\\(<RECORDTYPE\\"
            + "(TIMESTAMP\\(0\\) ROWTIME, INTEGER PRODUCTID, INTEGER ORDERID\\)>, <CHAR\\"
            + "(4\\)>, <INTERVAL HOUR>\\)'\\. Supported form\\(s\\): TUMBLE\\(TABLE "
            + "table_name, DESCRIPTOR\\(col1, col2 \\.\\.\\.\\), datetime interval\\)");
    sql("select * from table(\n"
        + "tumble(TABLE ^tabler_not_exist^, descriptor(rowtime), interval '2' hour))")
        .fails("Object 'TABLER_NOT_EXIST' not found");
  }

  @Test public void testStreamTumble() {
    // TUMBLE
    sql("select stream tumble_end(rowtime, interval '2' hour) as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId").ok();
    sql("select stream ^tumble(rowtime, interval '2' hour)^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId")
        .fails("Group function '\\$TUMBLE' can only appear in GROUP BY clause");
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
            + "matching call to group function '\\$TUMBLE' in GROUP BY clause");
    // Arguments to TUMBLE_END are slightly different to arguments to TUMBLE
    sql("select stream\n"
        + "  ^tumble_start(rowtime, interval '2' hour, time '00:13:00')^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour, time '00:12:00')")
        .fails("Call to auxiliary group function 'TUMBLE_START' must have "
            + "matching call to group function '\\$TUMBLE' in GROUP BY clause");
    // Even though align defaults to TIME '00:00:00', we need structural
    // equivalence, not semantic equivalence.
    sql("select stream\n"
        + "  ^tumble_end(rowtime, interval '2' hour, time '00:00:00')^ as rowtime\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour)")
        .fails("Call to auxiliary group function 'TUMBLE_END' must have "
            + "matching call to group function '\\$TUMBLE' in GROUP BY clause");
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
    sql("insert into empnullables ^values ('5', 'bob')^")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR\\$0' of type CHAR\\(1\\)");
    sql("insert into empnullables (^empno^, ename) values ('5', 'bob')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR\\$0' of type CHAR\\(1\\)");
    sql("insert into empnullables(extra BOOLEAN)"
        + " (empno, ename, ^extra^) values (5, 'bob', 'true')")
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .fails("Cannot assign to target field 'EXTRA' of type BOOLEAN"
            + " from source field 'EXPR\\$2' of type CHAR\\(4\\)");
  }

  @Disabled("CALCITE-1727")
  @Test public void testUpdateFailDataType() {
    sql("update emp"
        + " set ^empNo^ = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Cannot assign to target field 'EMPNO' of type INTEGER"
            + " from source field 'EXPR$0' of type CHAR(1)");
    sql("update emp(extra boolean)"
        + " set ^extra^ = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Cannot assign to target field 'EXTRA' of type BOOLEAN"
            + " from source field 'EXPR$0' of type CHAR(1)");
  }

  @Disabled("CALCITE-1727")
  @Test public void testUpdateFailCaseSensitivity() {
    sql("update empdefaults"
        + " set empNo = '5', deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Column 'empno' not found in any table; did you mean 'EMPNO'\\?");
  }

  @Test public void testUpdateExtendedColumnFailCaseSensitivity() {
    sql("update empdefaults(\"extra\" BOOLEAN)"
        + " set ^extra^ = true, deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Unknown target column 'EXTRA'");
    sql("update empdefaults(extra BOOLEAN)"
        + " set ^\"extra\"^ = true, deptno = 1, ename = 'Bob'"
        + " where deptno = 10")
        .fails("Unknown target column 'extra'");
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
    final String sql = "update empdefaults(^empno^ BOOLEAN, deptno INTEGER)\n"
        + "set deptno = 1, empno = false, ename = 'Bob'\n"
        + "where deptno = 10";
    final String expected = "Cannot assign to target field 'EMPNO' of type "
        + "INTEGER NOT NULL from source field 'EMPNO' of type BOOLEAN";
    sql(sql).fails(expected);
  }

  @Disabled("CALCITE-1727")
  @Test public void testUpdateExtendedColumnFailCollision2() {
    final String sql = "update empdefaults(^\"deptno\"^ BOOLEAN)\n"
        + "set \"deptno\" = 1, empno = 1, ename = 'Bob'\n"
        + "where deptno = 10";
    final String expected = "Cannot assign to target field 'deptno' of type "
        + "BOOLEAN NOT NULL from source field 'deptno' of type INTEGER";
    sql(sql).fails(expected);
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
    sql("insert into EMPDEFAULTS(^comm^ BOOLEAN)"
        + " (empno, ename, job, comm)\n"
        + "values (1, 'Arthur', 'clown', true)")
        .fails("Cannot assign to target field 'COMM' of type INTEGER"
            + " from source field 'COMM' of type BOOLEAN");
    sql("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^comm^)\n"
        + "values (1, 'Arthur', 'clown', true)")
        .fails("Cannot assign to target field 'COMM' of type INTEGER"
            + " from source field 'EXPR\\$3' of type BOOLEAN");
    sql("insert into EMPDEFAULTS(\"comm\" BOOLEAN)"
        + " (empno, ename, job, ^\"comm\"^)\n"
        + "values (1, 'Arthur', 'clown', 1)")
        .fails("Cannot assign to target field 'comm' of type BOOLEAN"
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

    sql("select slackingmin from (select empno as slackingmin from emp_r)\n"
        + "order by slackingmin")
        .ok();

    sql("select empno, sum(slackingmin) from emp_r group by empno\n"
        + "order by sum(slackingmin)").ok();
  }

  @Test public void testRollUpInJoin() {
    final String onError = "Rolled up column 'SLACKINGMIN' is not allowed in ON";
    final String usingError = "Rolled up column 'SLACKINGMIN' is not allowed in USING";
    final String selectError = "Rolled up column 'SLACKINGMIN' is not allowed in SELECT";

    sql("select * from (select deptno, ^slackingmin^ from emp_r)\n"
        + " join dept using (deptno)")
        .fails(selectError);

    sql("select * from dept as a\n"
        + "join (select deptno, ^slackingmin^ from emp_r) using (deptno)")
        .fails(selectError);

    sql("select * from emp_r as a\n"
        + "join dept_r as b using (deptno, ^slackingmin^)")
        .fails(usingError);

    // Even though the emp_r.slackingmin column will be under the SqlNode for '=',
    // The error should say it happened in 'ON' instead
    sql("select * from emp_r\n"
        + "join dept_r on (^emp_r.slackingmin^ = dept_r.slackingmin)")
        .fails(onError);
  }

  @Test public void testJsonValueExpressionOperator() {
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

  @Test public void testJsonExists() {
    expr("json_exists('{}', 'lax $')").ok();
    expr("json_exists('{}', 'lax $')")
        .columnType("BOOLEAN");
  }

  @Test public void testJsonValue() {
    expr("json_value('{\"foo\":\"bar\"}', 'lax $.foo')").ok();
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

  @Test public void testJsonQuery() {
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

  @Test public void testJsonArray() {
    expr("json_array()").ok();
    expr("json_array('foo', 'bar')").ok();
    expr("json_array('foo', 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test public void testJsonArrayAgg() {
    sql("select json_arrayagg(ename) from emp").ok();
    expr("json_arrayagg('foo')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test public void testJsonObject() {
    expr("json_object()").ok();
    expr("json_object('foo': 'bar')").ok();
    expr("json_object('foo': 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
    expr("^json_object(100: 'bar')^")
        .fails("(?s).*Expected a character type*");
  }

  @Test public void testJsonPretty() {
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

  @Test public void testJsonStorageSize() {
    sql("select json_storage_size(ename) from emp").ok();
    expr("json_storage_size('{\"foo\":\"bar\"}')").ok();
    expr("json_storage_size('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");
  }

  @Test public void testJsonType() {
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

  @Test public void testJsonDepth() {
    sql("select json_depth(ename) from emp").ok();
    expr("json_depth('{\"foo\":\"bar\"}')").ok();
    expr("json_depth('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");

    if (!Bug.CALCITE_2869_FIXED) {
      return;
    }
  }

  @Test public void testJsonLength() {
    expr("json_length('{\"foo\":\"bar\"}')").ok();
    expr("json_length('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_length('{\"foo\":\"bar\"}')")
        .columnType("INTEGER");
    expr("json_length('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("INTEGER");
    expr("json_length('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("INTEGER");
  }

  @Test public void testJsonKeys() {
    expr("json_keys('{\"foo\":\"bar\"}', 'lax $')").ok();
    expr("json_keys('{\"foo\":\"bar\"}', 'lax $')")
        .columnType("VARCHAR(2000)");
    expr("json_keys('{\"foo\":\"bar\"}', 'strict $')")
        .columnType("VARCHAR(2000)");
  }

  @Test public void testJsonRemove() {
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

  @Test public void testJsonObjectAgg() {
    sql("select json_objectagg(ename: empno) from emp").ok();
    sql("select json_objectagg(empno: ename) from emp").ok();
    sql("select ^json_objectagg(empno: ename)^ from emp")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    expr("json_objectagg('foo': 'bar')")
        .columnType("VARCHAR(2000) NOT NULL");
  }

  @Test public void testJsonPredicate() {
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

  @Test public void testRegexpReplace() {
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

  @Test public void testInvalidFunctionCall() {
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

  @Test public void testValidatorReportsOriginalQueryUsingReader()
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

  @Test public void testValidateParameterizedExpression() throws SqlParseException {
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
<<<<<<< HEAD

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
=======
