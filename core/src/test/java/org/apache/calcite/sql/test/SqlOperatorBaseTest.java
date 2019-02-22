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
package org.apache.calcite.sql.test;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
>>>>>>> ebe036aed (DX-7801 [CALCITE-3752] Add PIVOT operator to SQL)
=======
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SqlLimitsTest;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Throwables;
>>>>>>> df3383b8b ([CALCITE-2259] Allow Java 8 syntax)

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
>>>>>>> df3383b8b ([CALCITE-2259] Allow Java 8 syntax)
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Contains unit tests for all operators. Each of the methods is named after an
 * operator.
 *
 * <p>The class is abstract. It contains a test for every operator, but does not
 * provide a mechanism to execute the tests: parse, validate, and execute
 * expressions on the operators. This is left to a {@link SqlTester} object
 * which the derived class must provide.</p>
 *
 * <p>Different implementations of {@link SqlTester} are possible, such as:</p>
 *
 * <ul>
 * <li>Execute against a real farrago database
 * <li>Execute in pure java (parsing and validation can be done, but expression
 * evaluation is not possible)
 * <li>Generate a SQL script.
 * <li>Analyze which operators are adequately tested.
 * </ul>
 *
 * <p>A typical method will be named after the operator it is testing (say
 * <code>testSubstringFunc</code>). It first calls
 * {@link SqlTester#setFor(org.apache.calcite.sql.SqlOperator, org.apache.calcite.sql.test.SqlTester.VmName...)}
 * to declare which operator it is testing.
 *
 * <blockquote>
 * <pre><code>
 * public void testSubstringFunc() {
 *     tester.setFor(SqlStdOperatorTable.substringFunc);
 *     tester.checkScalar("sin(0)", "0");
 *     tester.checkScalar("sin(1.5707)", "1");
 * }</code></pre>
 * </blockquote>
 *
 * <p>The rest of the method contains calls to the various {@code checkXxx}
 * methods in the {@link SqlTester} interface. For an operator
 * to be adequately tested, there need to be tests for:
 *
 * <ul>
 * <li>Parsing all of its the syntactic variants.
 * <li>Deriving the type of in all combinations of arguments.
 *
 * <ul>
 * <li>Pay particular attention to nullability. For example, the result of the
 * "+" operator is NOT NULL if and only if both of its arguments are NOT
 * NULL.</li>
 * <li>Also pay attention to precision/scale/length. For example, the maximum
 * length of the "||" operator is the sum of the maximum lengths of its
 * arguments.</li>
 * </ul>
 * </li>
 * <li>Executing the function. Pay particular attention to corner cases such as
 * null arguments or null results.</li>
 * </ul>
 */
public abstract class SqlOperatorBaseTest {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getTestTracer(SqlOperatorBaseTest.class);

  // TODO: Change message when Fnl3Fixed to something like
  // "Invalid character for cast: PC=0 Code=22018"
  public static final String INVALID_CHAR_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "Overflow during calculation or cast: PC=0 Code=22003"
  public static final String OUT_OF_RANGE_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "Division by zero: PC=0 Code=22012"
  public static final String DIVISION_BY_ZERO_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "String right truncation: PC=0 Code=22001"
  public static final String STRING_TRUNC_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "Invalid datetime format: PC=0 Code=22007"
  public static final String BAD_DATETIME_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // Error messages when an invalid time unit is given as
  // input to extract for a particular input type.
  public static final String INVALID_EXTRACT_UNIT_CONVERTLET_ERROR =
      "Extract.*from.*type data is not supported";

  public static final String INVALID_EXTRACT_UNIT_VALIDATION_ERROR =
      "Cannot apply 'EXTRACT' to arguments of type .*'\n.*";

  public static final String LITERAL_OUT_OF_RANGE_MESSAGE =
      "(?s).*Numeric literal.*out of range.*";

  public static final String INVALID_ARGUMENTS_NUMBER =
      "Invalid number of arguments to function .* Was expecting .* arguments";

  public static final boolean TODO = false;

  /**
   * Regular expression for a SQL TIME(0) value.
   */
  public static final Pattern TIME_PATTERN =
      Pattern.compile(
          "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

  /**
   * Regular expression for a SQL TIMESTAMP(0) value.
   */
  public static final Pattern TIMESTAMP_PATTERN =
      Pattern.compile(
          "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] "
              + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

  /**
   * Regular expression for a SQL DATE value.
   */
  public static final Pattern DATE_PATTERN =
      Pattern.compile(
          "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

  public static final String[] NUMERIC_TYPE_NAMES = {
      "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
      "DECIMAL(5, 2)", "REAL", "FLOAT", "DOUBLE"
  };

  // REVIEW jvs 27-Apr-2006:  for Float and Double, MIN_VALUE
  // is the smallest positive value, not the smallest negative value
  public static final String[] MIN_NUMERIC_STRINGS = {
      Long.toString(Byte.MIN_VALUE),
      Long.toString(Short.MIN_VALUE),
      Long.toString(Integer.MIN_VALUE),
      Long.toString(Long.MIN_VALUE),
      "-999.99",

      // NOTE jvs 26-Apr-2006:  Win32 takes smaller values from win32_values.h
      "1E-37", /*Float.toString(Float.MIN_VALUE)*/
      "2E-307", /*Double.toString(Double.MIN_VALUE)*/
      "2E-307" /*Double.toString(Double.MIN_VALUE)*/,
  };

  public static final String[] MIN_OVERFLOW_NUMERIC_STRINGS = {
      Long.toString(Byte.MIN_VALUE - 1),
      Long.toString(Short.MIN_VALUE - 1),
      Long.toString((long) Integer.MIN_VALUE - 1),
      new BigDecimal(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString(),
      "-1000.00",
      "1e-46",
      "1e-324",
      "1e-324"
  };

  public static final String[] MAX_NUMERIC_STRINGS = {
      Long.toString(Byte.MAX_VALUE),
      Long.toString(Short.MAX_VALUE),
      Long.toString(Integer.MAX_VALUE),
      Long.toString(Long.MAX_VALUE), "999.99",

      // NOTE jvs 26-Apr-2006:  use something slightly less than MAX_VALUE
      // because roundtripping string to approx to string doesn't preserve
      // MAX_VALUE on win32
      "3.4028234E38", /*Float.toString(Float.MAX_VALUE)*/
      "1.79769313486231E308", /*Double.toString(Double.MAX_VALUE)*/
      "1.79769313486231E308" /*Double.toString(Double.MAX_VALUE)*/
  };

  public static final String[] MAX_OVERFLOW_NUMERIC_STRINGS = {
      Long.toString(Byte.MAX_VALUE + 1),
      Long.toString(Short.MAX_VALUE + 1),
      Long.toString((long) Integer.MAX_VALUE + 1),
      (new BigDecimal(Long.MAX_VALUE)).add(BigDecimal.ONE).toString(),
      "1000.00",
      "1e39",
      "-1e309",
      "1e309"
  };
  private static final boolean[] FALSE_TRUE = {false, true};
  private static final SqlTester.VmName VM_FENNEL = SqlTester.VmName.FENNEL;
  private static final SqlTester.VmName VM_JAVA = SqlTester.VmName.JAVA;
  private static final SqlTester.VmName VM_EXPAND = SqlTester.VmName.EXPAND;
  protected static final TimeZone UTC_TZ = TimeZone.getTimeZone("GMT");
  // time zone for the LOCAL_{DATE,TIME,TIMESTAMP} functions
  protected static final TimeZone LOCAL_TZ = TimeZone.getDefault();
  // time zone for the CURRENT{DATE,TIME,TIMESTAMP} functions
  protected static final TimeZone CURRENT_TZ = LOCAL_TZ;

  private static final Pattern INVALID_ARG_FOR_POWER = Pattern.compile(
      "(?s).*Invalid argument\\(s\\) for 'POWER' function.*");

  private static final Pattern CODE_2201F = Pattern.compile(
      "(?s).*could not calculate results for the following row.*PC=5 Code=2201F.*");

  /**
   * Whether DECIMAL type is implemented.
   */
  public static final boolean DECIMAL = false;

  private final boolean enable;

  protected final SqlTester tester;

  // same with tester but without implicit type coercion.
  protected final SqlTester strictTester;
  /**
   * Tests that CAST fails when given a value just outside the valid range for
   * that type. For example,
   *
   * <ul>
   * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
   * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
   * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
   * precision.
   * </ul>
   */
  @Test void testLiteralAtLimit() {
    tester.setFor(SqlStdOperatorTable.CAST);
    if (!enable) {
      return;
    }
    final List<RelDataType> types =
        SqlLimitsTest.getTypes(tester.getValidator().getTypeFactory());
    for (RelDataType type : types) {
      for (Object o : getValues((BasicSqlType) type, true)) {
        SqlLiteral literal =
            type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
        SqlString literalString =
            literal.toSqlString(AnsiSqlDialect.DEFAULT);
        final String expr =
            "CAST(" + literalString
                + " AS " + type + ")";
        try {
          tester.checkType(
              expr,
              type.getFullTypeString());

          if (type.getSqlTypeName() == SqlTypeName.BINARY) {
            // Casting a string/binary values may change the value.
            // For example, CAST(X'AB' AS BINARY(2)) yields
            // X'AB00'.
          } else {
            tester.checkScalar(
                expr + " = " + literalString,
                true,
                "BOOLEAN NOT NULL");
          }
        } catch (Error e) {
          System.out.println("Failed for expr=[" + expr + "]");
          throw e;
        } catch (RuntimeException e) {
          System.out.println("Failed for expr=[" + expr + "]");
          throw e;
        }
      }
    }
  }

  /**
   * Tests that CAST fails when given a value just outside the valid range for
   * that type. For example,
   *
   * <ul>
   * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
   * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
   * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
   * precision.
   * </ul>
   */
  @Test void testLiteralBeyondLimit() {
    tester.setFor(SqlStdOperatorTable.CAST);
    final List<RelDataType> types =
        SqlLimitsTest.getTypes(tester.getValidator().getTypeFactory());
    for (RelDataType type : types) {
      for (Object o : getValues((BasicSqlType) type, false)) {
        SqlLiteral literal =
            type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
        SqlString literalString =
            literal.toSqlString(AnsiSqlDialect.DEFAULT);

        if ((type.getSqlTypeName() == SqlTypeName.BIGINT)
            || ((type.getSqlTypeName() == SqlTypeName.DECIMAL)
            && (type.getPrecision() == 19))) {
          // Values which are too large to be literals fail at
          // validate time.
          tester.checkFails(
              "CAST(^" + literalString + "^ AS " + type + ")",
              "Numeric literal '.*' out of range",
              false);
        } else if (
            (type.getSqlTypeName() == SqlTypeName.CHAR)
                || (type.getSqlTypeName() == SqlTypeName.VARCHAR)
                || (type.getSqlTypeName() == SqlTypeName.BINARY)
                || (type.getSqlTypeName() == SqlTypeName.VARBINARY)) {
          // Casting overlarge string/binary values do not fail -
          // they are truncated. See testCastTruncates().
        } else {
          if (Bug.CALCITE_2539_FIXED) {
            // Value outside legal bound should fail at runtime (not
            // validate time).
            //
            // NOTE: Because Java and Fennel calcs give
            // different errors, the pattern hedges its bets.
            tester.checkFails(
                "CAST(" + literalString + " AS " + type + ")",
                "(?s).*(Overflow during calculation or cast\\.|Code=22003).*",
                true);
          }
        }
      }
    }
  }

  @Test void testCastTruncates() {
    tester.setFor(SqlStdOperatorTable.CAST);
    tester.checkScalar("CAST('ABCD' AS CHAR(2))", "AB", "CHAR(2) NOT NULL");
    tester.checkScalar("CAST('ABCD' AS VARCHAR(2))", "AB",
        "VARCHAR(2) NOT NULL");
    tester.checkScalar("CAST('ABCD' AS VARCHAR)", "ABCD", "VARCHAR NOT NULL");
    tester.checkScalar("CAST(CAST('ABCD' AS VARCHAR) AS VARCHAR(3))", "ABC",
        "VARCHAR(3) NOT NULL");

    tester.checkScalar("CAST(x'ABCDEF12' AS BINARY(2))", "abcd",
        "BINARY(2) NOT NULL");
    tester.checkScalar("CAST(x'ABCDEF12' AS VARBINARY(2))", "abcd",
        "VARBINARY(2) NOT NULL");
    tester.checkScalar("CAST(x'ABCDEF12' AS VARBINARY)", "abcdef12",
        "VARBINARY NOT NULL");
    tester.checkScalar("CAST(CAST(x'ABCDEF12' AS VARBINARY) AS VARBINARY(3))",
        "abcdef", "VARBINARY(3) NOT NULL");

    if (!enable) {
      return;
    }
    tester.checkBoolean(
        "CAST(X'' AS BINARY(3)) = X'000000'",
        true);
    tester.checkBoolean("CAST(X'' AS BINARY(3)) = X''", false);
  }

  /** Test that calls all operators with all possible argument types, and for
<<<<<<< HEAD
   * each type, with a set of tricky values.
   *
   * This is not really a unit test since there are no assertions;
   * it either succeeds or fails in the preparation of the operator case
   * and not when actually testing (validating/executing) the call.
   *
   * Nevertheless the log messages conceal many problems which potentially need
   * to be fixed especially cases where the query passes from the validation stage
   * and fails at runtime.
   * */
  @Disabled("Too slow and not really a unit test")
  @Tag("slow")
  @Test void testArgumentBounds() {
=======
   * each type, with a set of tricky values. */
  @Test public void testArgumentBounds() {
    if (!CalciteSystemProperty.TEST_SLOW.value()) {
      return;
    }
    final SqlValidatorImpl validator = (SqlValidatorImpl) tester.getValidator();
    final SqlValidatorScope scope = validator.getEmptyScope();
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final Builder builder = new Builder(typeFactory);
    builder.add0(SqlTypeName.BOOLEAN, true, false);
    builder.add0(SqlTypeName.TINYINT, 0, 1, -3, Byte.MAX_VALUE, Byte.MIN_VALUE);
    builder.add0(SqlTypeName.SMALLINT, 0, 1, -4, Short.MAX_VALUE,
        Short.MIN_VALUE);
    builder.add0(SqlTypeName.INTEGER, 0, 1, -2, Integer.MIN_VALUE,
        Integer.MAX_VALUE);
    builder.add0(SqlTypeName.BIGINT, 0, 1, -5, Integer.MAX_VALUE,
        Long.MAX_VALUE, Long.MIN_VALUE);
    builder.add1(SqlTypeName.VARCHAR, 11, "", " ", "hello world");
    builder.add1(SqlTypeName.CHAR, 5, "", "e", "hello");
    builder.add0(SqlTypeName.TIMESTAMP, 0L, DateTimeUtils.MILLIS_PER_DAY);

    Set<SqlOperator> operatorsToSkip = new HashSet<>();
    if (!Bug.CALCITE_3243_FIXED) {
      // TODO: Remove entirely the if block when the bug is fixed
      // REVIEW zabetak 12-August-2019: It may still make sense to avoid the
      // JSON functions since for most of the values above they are expected
      // to raise an error and due to the big number of operands they accept
      // they increase significantly the running time of the method.
      operatorsToSkip.add(SqlStdOperatorTable.JSON_VALUE);
      operatorsToSkip.add(SqlStdOperatorTable.JSON_QUERY);
    }
    // Skip since ClassCastException is raised in SqlOperator#unparse
    // since the operands of the call do not have the expected type.
    // Moreover, the values above do not make much sense for this operator.
    operatorsToSkip.add(SqlStdOperatorTable.WITHIN_GROUP);
    operatorsToSkip.add(SqlStdOperatorTable.TRIM); // can't handle the flag argument
    operatorsToSkip.add(SqlStdOperatorTable.EXISTS);
    for (SqlOperator op : SqlStdOperatorTable.instance().getOperatorList()) {
      if (operatorsToSkip.contains(op)) {
        continue;
      }
      switch (op.getSyntax()) {
      case SPECIAL:
        continue;
      }
      final SqlOperandTypeChecker typeChecker =
          op.getOperandTypeChecker();
      if (typeChecker == null) {
        continue;
      }
      final SqlOperandCountRange range =
          typeChecker.getOperandCountRange();
      for (int n = range.getMin(), max = range.getMax(); n <= max; n++) {
        final List<List<ValueType>> argValues =
            Collections.nCopies(n, builder.values);
        for (final List<ValueType> args : Linq4j.product(argValues)) {
          SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
          int nullCount = 0;
          for (ValueType arg : args) {
            if (arg.value == null) {
              ++nullCount;
            }
            nodeList.add(arg.node);
          }
          final SqlCall call = op.createCall(nodeList);
          final SqlCallBinding binding =
              new SqlCallBinding(validator, scope, call);
          if (!typeChecker.checkOperandTypes(binding, false)) {
            continue;
          }
          final SqlPrettyWriter writer = new SqlPrettyWriter();
          op.unparse(writer, call, 0, 0);
          final String s = writer.toSqlString().toString();
          if (s.startsWith("OVERLAY(")
              || s.contains(" / 0")
              || s.matches("MOD\\(.*, 0\\)")) {
            continue;
          }
          final Strong.Policy policy = Strong.policy(op.kind);
          try {
            if (nullCount > 0 && policy == Strong.Policy.ANY) {
              tester.checkNull(s);
            } else {
              final String query;
              if (op instanceof SqlAggFunction) {
                if (op.requiresOrder()) {
                  query = "SELECT " + s + " OVER () FROM (VALUES (1))";
                } else {
                  query = "SELECT " + s + " FROM (VALUES (1))";
                }
              } else {
                query = AbstractSqlTester.buildQuery(s);
              }
              tester.check(query, SqlTests.ANY_TYPE_CHECKER,
                  SqlTests.ANY_PARAMETER_CHECKER, result -> { });
            }
          } catch (Throwable e) {
            // Logging the top-level throwable directly makes the message
            // difficult to read since it either contains too much information
            // or very few details.
            Throwable cause = findMostDescriptiveCause(e);
            LOGGER.info("Failed: " + s + ": " + cause);
          }
        }
      }
    }
  }

  private Throwable findMostDescriptiveCause(Throwable ex) {
    if (ex instanceof CalciteException
        || ex instanceof CalciteContextException
        || ex instanceof SqlParseException) {
      return ex;
    }
    Throwable cause = ex.getCause();
    if (cause != null) {
      return findMostDescriptiveCause(cause);
    }
    return ex;
  }

  private List<Object> getValues(BasicSqlType type, boolean inBound) {
    List<Object> values = new ArrayList<Object>();
    for (boolean sign : FALSE_TRUE) {
      for (SqlTypeName.Limit limit : SqlTypeName.Limit.values()) {
        Object o = type.getLimit(sign, limit, !inBound);
        if (o == null) {
          continue;
        }
        if (!values.contains(o)) {
          values.add(o);
        }
      }
    }
    return values;
  }

  // TODO: Test other stuff

  /**
   * Result checker that considers a test to have succeeded if it throws an
   * exception that matches one of a list of patterns.
   */
  private static class ExceptionResultChecker
      implements SqlTester.ResultChecker {
    private final Pattern[] patterns;

    ExceptionResultChecker(Pattern... patterns) {
      this.patterns = patterns;
    }

    public void checkResult(ResultSet result) throws Exception {
      Throwable thrown = null;
      try {
        result.next();
        fail("expected exception");
      } catch (SQLException e) {
        thrown = e;
      }
      final String stack = Throwables.getStackTraceAsString(thrown);
      for (Pattern pattern : patterns) {
        if (pattern.matcher(stack).matches()) {
          return;
        }
      }
      fail("Stack did not match any pattern; " + stack);
    }
  }

  /**
   * Result checker that considers a test to have succeeded if it returns a
   * particular value or throws an exception that matches one of a list of
   * patterns.
   *
   * <p>Sounds peculiar, but is necessary when eager and lazy behaviors are
   * both valid.
   */
  private static class ValueOrExceptionResultChecker
      implements SqlTester.ResultChecker {
    private final Object expected;
    private final Pattern[] patterns;

    ValueOrExceptionResultChecker(
        Object expected, Pattern... patterns) {
      this.expected = expected;
      this.patterns = patterns;
    }

    public void checkResult(ResultSet result) throws Exception {
      Throwable thrown = null;
      try {
        if (!result.next()) {
          // empty result is OK
          return;
        }
        final Object actual = result.getObject(1);
        assertEquals(expected, actual);
      } catch (SQLException e) {
        thrown = e;
      }
      if (thrown != null) {
        final String stack = Throwables.getStackTraceAsString(thrown);
        for (Pattern pattern : patterns) {
          if (pattern.matcher(stack).matches()) {
            return;
          }
        }
        fail("Stack did not match any pattern; " + stack);
      }
    }
  }

  public static SqlTester tester() {
    return new TesterImpl(SqlTestFactory.INSTANCE);
  }

  /**
   * Implementation of {@link org.apache.calcite.sql.test.SqlTester} based on a
   * JDBC connection.
   */
  protected static class TesterImpl extends SqlRuntimeTester {
    public TesterImpl(SqlTestFactory testFactory) {
      super(testFactory, UnaryOperator.identity());
    }

    @Override public void check(String query, TypeChecker typeChecker,
        ParameterChecker parameterChecker, ResultChecker resultChecker) {
      super.check(query, typeChecker, parameterChecker, resultChecker);
      //noinspection unchecked
      final CalciteAssert.ConnectionFactory connectionFactory =
          (CalciteAssert.ConnectionFactory)
              getFactory().get("connectionFactory");
      try (Connection connection = connectionFactory.createConnection();
           Statement statement = connection.createStatement()) {
        final ResultSet resultSet =
            statement.executeQuery(query);
        resultChecker.checkResult(resultSet);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
    @Override protected SqlTester with(SqlTestFactory factory) {
      return new TesterImpl(factory);
=======
    @Override protected TesterImpl with(final String name, final Object value) {
      return new TesterImpl(factory.with(name, value));
=======
    @Override protected SqlTester with(SqlTestFactory factory) {
      return new TesterImpl(factory);
    }
  }

  /** A type, a value, and its {@link SqlNode} representation. */
  static class ValueType {
    final RelDataType type;
    final Object value;
    final SqlNode node;

    ValueType(RelDataType type, Object value) {
      this.type = type;
      this.value = value;
      this.node = literal(type, value);
    }

    private SqlNode literal(RelDataType type, Object value) {
      if (value == null) {
        return SqlStdOperatorTable.CAST.createCall(
            SqlParserPos.ZERO,
            SqlLiteral.createNull(SqlParserPos.ZERO),
            SqlTypeUtil.convertTypeToSpec(type));
      }
      switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return SqlLiteral.createBoolean((Boolean) value, SqlParserPos.ZERO);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return SqlLiteral.createExactNumeric(
            value.toString(), SqlParserPos.ZERO);
      case CHAR:
      case VARCHAR:
        return SqlLiteral.createCharString(value.toString(), SqlParserPos.ZERO);
      case TIMESTAMP:
        TimestampString ts = TimestampString.fromMillisSinceEpoch((Long) value);
        return SqlLiteral.createTimestamp(ts, type.getPrecision(),
            SqlParserPos.ZERO);
      default:
        throw new AssertionError(type);
      }
    }
  }

  /** Builds lists of types and sample values. */
  static class Builder {
    final RelDataTypeFactory typeFactory;
    final List<RelDataType> types = new ArrayList<>();
    final List<ValueType> values = new ArrayList<>();

    Builder(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    public void add0(SqlTypeName typeName, Object... values) {
      add(typeFactory.createSqlType(typeName), values);
    }

    public void add1(SqlTypeName typeName, int precision, Object... values) {
      add(typeFactory.createSqlType(typeName, precision), values);
    }

    private void add(RelDataType type, Object[] values) {
      types.add(type);
      for (Object value : values) {
        this.values.add(new ValueType(type, value));
      }
      this.values.add(new ValueType(type, null));
    }
  }

  /** Runs an OVERLAPS test with a given set of literal values. */
  class OverlapChecker {
    final String[] values;

    OverlapChecker(String... values) {
      this.values = values;
    }

    public void isTrue(String s) {
      tester.checkBoolean(sub(s), Boolean.TRUE);
    }

    public void isFalse(String s) {
      tester.checkBoolean(sub(s), Boolean.FALSE);
    }

    private String sub(String s) {
      return s.replace("$0", values[0])
          .replace("$1", values[1])
          .replace("$2", values[2])
          .replace("$3", values[3]);
    }
  }
}
