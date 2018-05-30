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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.AbstractSqlTester;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * An abstract base class for implementing tests against {@link SqlValidator}.
 *
 * <p>A derived class can refine this test in two ways. First, it can add <code>
 * testXxx()</code> methods, to test more functionality.
 *
 * <p>Second, it can override the {@link #getTester} method to return a
 * different implementation of the {@link Tester} object. This encapsulates the
 * differences between test environments, for example, which SQL parser or
 * validator to use.</p>
 */
public class SqlValidatorTestCase {
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlTestFactory EXTENDED_TEST_FACTORY =
      SqlTestFactory.INSTANCE.withCatalogReader(MockCatalogReaderExtended::new);

  static final SqlTester EXTENDED_CATALOG_TESTER =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY);

  static final SqlTester EXTENDED_CATALOG_TESTER_2003 =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

  static final SqlTester EXTENDED_CATALOG_TESTER_LENIENT =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.LENIENT);

  //~ Instance fields --------------------------------------------------------

  protected SqlTester tester;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a test case.
   */
  public SqlValidatorTestCase() {
    this.tester = getTester();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns a tester. Derived classes should override this method to run the
   * same set of tests in a different testing environment.
   */
  public SqlTester getTester() {
    return new SqlValidatorTester(SqlTestFactory.INSTANCE);
  }

  /** Creates a test context with a SQL query. */
  public final Sql sql(String sql) {
    return new Sql(tester, sql, true);
  }

  public final Sql sql(String sql, boolean typeCoercion) {
    final SqlTester tester1 = tester.enableTypeCoercion(typeCoercion);
    return new Sql(tester1, sql, true);
  }

  public final Sql expr(String sql) {
    return new Sql(tester, sql, false);
  }

  public final Sql winSql(String sql, boolean typeCoercion) {
    return sql(sql, typeCoercion);
  }

  public final Sql winSql(String sql) {
    return sql(sql);
  }

  public final Sql win(String sql) {
    return sql("select * from emp " + sql);
  }

  public Sql winExp(String sql) {
    return winSql("select " + sql + " from emp window w as (order by deptno)");
  }

  public Sql winExp2(String sql) {
    return winSql("select " + sql + " from emp");
  }

  public Sql winExp2(String sql, boolean typeCoercion) {
    return winSql("select " + sql + " from emp", typeCoercion);
  }

  public void check(String sql) {
    sql(sql).ok();
  }

  @Deprecated // to be removed before 1.23
  public void checkExp(String sql) {
    tester.assertExceptionIsThrown(
        AbstractSqlTester.buildQuery(sql),
        null);
  }

  @Deprecated // to be removed before 1.23
  public final void checkFails(
      String sql,
      String expected) {
    sql(sql).fails(expected);
  }

  /**
   * Checks that a SQL query gives a particular error, or succeeds if {@code
   * expected} is null, with specified type coercion flag.
   */
  public final void checkFails(
      String sql,
      String expected,
      boolean typeCoercion) {
    sql(sql, typeCoercion).fails(expected);
  }

  /**
   * Checks that a SQL expression gives a particular error.
   */
  public final void checkExpFails(
      String sql,
      String expected) {
    tester.assertExceptionIsThrown(
        AbstractSqlTester.buildQuery(sql),
        expected);
  }

  /**
   * Checks that a SQL expression gives a particular error,
   * with specified type coercion flag.
   */
  public final void checkExpFails(
      String sql,
      String expected,
      boolean typeCoercion) {
    final SqlTester tester1 = tester.enableTypeCoercion(typeCoercion);
    tester1.assertExceptionIsThrown(
        AbstractSqlTester.buildQuery(sql),
        expected);
  }

  /**
   * Checks that a SQL expression gives a particular error, and that the
   * location of the error is the whole expression.
   */
  public final void checkWholeExpFails(
      String sql,
      String expected) {
    wholeExpr(sql).fails(expected);
  }

  /**
   * Checks that a SQL expression gives a particular error, and that the
   * location of the error is the whole expression, with specified type coercion flag.
   */
  public final void checkWholeExpFails(
      String sql,
      String expected,
      boolean typeCoercion) {
    assert sql.indexOf('^') < 0;
    checkExpFails("^" + sql + "^", expected, typeCoercion);
  }

  public final void checkExpType(
      String sql,
      String expected) {
    checkColumnType(
        AbstractSqlTester.buildQuery(sql),
        expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkColumnType(
      String sql,
      String expected) {
    sql(sql).columnType(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkResultType(
      String sql,
      String expected) {
    sql(sql).type(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkIntervalConv(
      String sql,
      String expected) {
    tester.checkIntervalConv(
        AbstractSqlTester.buildQuery(sql),
        expected);
  }

  @Deprecated // to be removed before 1.23
  protected final void assertExceptionIsThrown(
      String sql,
      String expectedMsgPattern) {
    sql(sql).fails(expectedMsgPattern);
  }

  protected final void assertExceptionIsThrown(
      String sql,
      String expectedMsgPattern,
      boolean typeCoercion) {
    assert expectedMsgPattern != null;
    final SqlTester tester1 = tester.enableTypeCoercion(typeCoercion);
    tester1.assertExceptionIsThrown(sql, expectedMsgPattern);
  }

  public void checkCharset(
      String sql,
      Charset expectedCharset) {
    sql(sql).charset(expectedCharset);
  }

  @Deprecated // to be removed before 1.23
  public void checkCollation(
      String sql,
      String expectedCollationName,
      SqlCollation.Coercibility expectedCoercibility) {
    sql(sql).collation(expectedCollationName, expectedCoercibility);
  }

  /**
     */
    Sql(SqlTester tester, String sql, boolean query) {
      this.tester = tester;
      this.sql = query ? sql : AbstractSqlTester.buildQuery(sql);
    }

    @Deprecated // to be removed before 1.23
    Sql tester(SqlTester tester) {
      return new Sql(tester, sql, true);
    }

    public Sql sql(String sql) {
      return new Sql(tester, sql, true);
    }

    Sql withExtendedCatalog() {
      return withTester(tester -> EXTENDED_CATALOG_TESTER);
    }

    public Sql withQuoting(Quoting quoting) {
      return withTester(tester -> tester.withQuoting(quoting));
    }

    Sql withLex(Lex lex) {
      return withTester(tester -> tester.withLex(lex));
    }

    Sql withConformance(SqlConformance conformance) {
      return withTester(tester -> tester.withConformance(conformance));
    }

    Sql withTypeCoercion(boolean typeCoercion) {
      return withTester(tester -> tester.enableTypeCoercion(typeCoercion));
    }

    Sql withWhole(boolean whole) {
      Preconditions.checkArgument(sql.indexOf('^') < 0);
      return new Sql(tester, sql, query, whole);
    }

    Sql ok() {
      tester.assertExceptionIsThrown(toSql(false), null);
      return this;
    }

    /**
     * Checks that a SQL expression gives a particular error.
     */
    Sql fails(@Nonnull String expected) {
      Objects.requireNonNull(expected);
      tester.assertExceptionIsThrown(toSql(true), expected);
      return this;
    }

    /**
     * Checks that a SQL expression fails, giving an {@code expected} error,
     * if {@code b} is true, otherwise succeeds.
     */
    Sql failsIf(boolean b, String expected) {
      if (b) {
        fails(expected);
      } else {
        ok();
      }
      return this;
    }

    /**
     * Checks that a query returns a row of the expected type. For example,
     *
     * <blockquote>
     *   <code>sql("select empno, name from emp")<br>
     *     .type("{EMPNO INTEGER NOT NULL, NAME VARCHAR(10) NOT NULL}");</code>
     * </blockquote>
     *
     * @param expectedType Expected row type
     */
    public Sql type(String expectedType) {
      tester.checkResultType(sql, expectedType);
      return this;
    }

    /**
     * Checks that a query returns a single column, and that the column has the
     * expected type. For example,
     *
     * <blockquote>
     * <code>sql("SELECT empno FROM Emp").columnType("INTEGER NOT NULL");</code>
     * </blockquote>
     *
     * @param expectedType Expected type, including nullability
     */
    public Sql columnType(String expectedType) {
      tester.checkColumnType(toSql(false), expectedType);
      return this;
    }

    public Sql monotonic(SqlMonotonicity expectedMonotonicity) {
      tester.checkMonotonic(toSql(false), expectedMonotonicity);
      return this;
    }

    public Sql bindType(final String bindType) {
      tester.check(sql, null, parameterRowType ->
          assertThat(parameterRowType.toString(), is(bindType)),
          result -> { });
      return this;
    }

    /** Removes the carets from the SQL string. Useful if you want to run
     * a test once at a conformance level where it fails, then run it again
     * at a conformance level where it succeeds. */
    public Sql sansCarets() {
      return new Sql(tester, sql.replace("^", ""), true);
    }
  }
}
