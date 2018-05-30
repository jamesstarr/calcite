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
>>>>>>> 0f9894602 (Use real properties, not their names, in tests)
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
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
=======
>>>>>>> ebe036aed (DX-7801 [CALCITE-3752] Add PIVOT operator to SQL)
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
import com.google.common.collect.ImmutableList;

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
>>>>>>> 8782d9063 ([CALCITE-2928] When resolving user-defined functions (UDFs), use the case-sensitivity of the current connection (Danny Chan))
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
    tester.checkAgg("collect(x)", values, Arrays.asList("[0, 2, 2]"), (double) 0);
    tester.checkAgg("collect(x) within group(order by x desc)", values,
        Collections.singletonList("[2, 2, 0]"), (double) 0);
    Object result1 = -3;
    if (!enable) {
      return;
    }
    tester.checkAgg("collect(CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        result1, (double) 0);
    Object result = -1;
    tester.checkAgg("collect(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, result, (double) 0);
    tester.checkAgg("collect(DISTINCT x)", values, 2, (double) 0);
  }


  @Test public void testFusionFunc() {
    tester.setFor(SqlStdOperatorTable.FUSION, VM_FENNEL, VM_JAVA);
  }

  @Test public void testYear() {
    tester.setFor(
        SqlStdOperatorTable.YEAR,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "year(date '2008-1-23')",
        "2008",
        "BIGINT NOT NULL");
    tester.checkNull("year(cast(null as date))");
  }

  @Test public void testQuarter() {
    tester.setFor(
        SqlStdOperatorTable.QUARTER,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "quarter(date '2008-1-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-2-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-3-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-4-23')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-5-23')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-6-23')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-7-23')",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-8-23')",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-9-23')",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-10-23')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-11-23')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-12-23')",
        "4",
        "BIGINT NOT NULL");
    tester.checkNull("quarter(cast(null as date))");
  }

  @Test public void testMonth() {
    tester.setFor(
        SqlStdOperatorTable.MONTH,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "month(date '2008-1-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkNull("month(cast(null as date))");
  }

  @Test public void testWeek() {
    tester.setFor(
        SqlStdOperatorTable.WEEK,
        VM_FENNEL,
        VM_JAVA);
    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "week(date '2008-1-23')",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails(
          "week(cast(null as date))",
          "cannot translate call EXTRACT.*",
          true);
    }
  }

  @Test public void testDayOfYear() {
    tester.setFor(
        SqlStdOperatorTable.DAYOFYEAR,
        VM_FENNEL,
        VM_JAVA);
    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "dayofyear(date '2008-1-23')",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails(
          "dayofyear(cast(null as date))",
          "cannot translate call EXTRACT.*",
          true);
    }
  }

  @Test public void testDayOfMonth() {
    tester.setFor(
        SqlStdOperatorTable.DAYOFMONTH,
        VM_FENNEL,
        VM_JAVA);
    tester.checkScalar(
        "dayofmonth(date '2008-1-23')",
        "23",
        "BIGINT NOT NULL");
    tester.checkNull("dayofmonth(cast(null as date))");
  }

  @Test public void testDayOfWeek() {
    tester.setFor(
        SqlStdOperatorTable.DAYOFWEEK,
        VM_FENNEL,
        VM_JAVA);
    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "dayofweek(date '2008-1-23')",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails("dayofweek(cast(null as date))",
          "cannot translate call EXTRACT.*",
          true);
    }
  }

  @Test public void testHour() {
    tester.setFor(
        SqlStdOperatorTable.HOUR,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "hour(timestamp '2008-1-23 12:34:56')",
        "12",
        "BIGINT NOT NULL");
    tester.checkNull("hour(cast(null as timestamp))");
  }

  @Test public void testMinute() {
    tester.setFor(
        SqlStdOperatorTable.MINUTE,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "minute(timestamp '2008-1-23 12:34:56')",
        "34",
        "BIGINT NOT NULL");
    tester.checkNull("minute(cast(null as timestamp))");
  }

  @Test public void testSecond() {
    tester.setFor(
        SqlStdOperatorTable.SECOND,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "second(timestamp '2008-1-23 12:34:56')",
        "56",
        "BIGINT NOT NULL");
    tester.checkNull("second(cast(null as timestamp))");
  }

  @Test public void testExtractIntervalYearMonth() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    if (TODO) {
      // Not supported, fails in type validation because the extract
      // unit is not YearMonth interval type.

      tester.checkScalar(
          "extract(epoch from interval '4-2' year to month)",
          // number of seconds elapsed since timestamp
          // '1970-01-01 00:00:00' + input interval
          "131328000",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(second from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(minute from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(hour from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(day from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");
    }

    // Postgres doesn't support DOW, DOY and WEEK on INTERVAL YEAR MONTH type.
    // SQL standard doesn't have extract units for DOW, DOY and WEEK.
    tester.checkFails("^extract(doy from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);
    tester.checkFails("^extract(dow from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);
    tester.checkFails("^extract(week from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);

    tester.checkScalar(
        "extract(month from interval '4-2' year to month)",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(quarter from interval '4-2' year to month)",
        "1",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from interval '4-2' year to month)",
        "4",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(decade from interval '426-3' year(3) to month)",
        "42",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from interval '426-3' year(3) to month)",
        "4",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millennium from interval '2005-3' year(4) to month)",
        "2",
        "BIGINT NOT NULL");
  }

  @Test public void testExtractIntervalDayTime() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    if (TODO) {
      // Not implemented in operator test
      tester.checkScalar(
          "extract(epoch from interval '2 3:4:5.678' day to second)",
          // number of seconds elapsed since timestamp
          // '1970-01-01 00:00:00' + input interval
          "183845.678",
          "BIGINT NOT NULL");
    }

    tester.checkScalar(
        "extract(second from interval '2 3:4:5.678' day to second)",
        "5",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from interval '2 3:4:5.678' day to second)",
        "4",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(hour from interval '2 3:4:5.678' day to second)",
        "3",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(day from interval '2 3:4:5.678' day to second)",
        "2",
        "BIGINT NOT NULL");

    // Postgres doesn't support DOW, DOY and WEEK on INTERVAL DAY TIME type.
    // SQL standard doesn't have extract units for DOW, DOY and WEEK.
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails("extract(doy from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
      tester.checkFails("extract(dow from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
      tester.checkFails("extract(week from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
      tester.checkFails("extract(isodow from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
    }

    tester.checkFails(
        "^extract(month from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "MONTH> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(quarter from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "QUARTER> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(year from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "YEAR> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(century from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "CENTURY> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);
  }

  @Test public void testExtractDate() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(epoch from date '2008-2-23')",
        "1203724800", // number of seconds elapsed since timestamp
                      // '1970-01-01 00:00:00' for given date
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(second from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(hour from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(day from date '2008-2-23')",
        "23",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from date '2008-2-23')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(quarter from date '2008-4-23')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isoyear from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(doy from date '2008-2-23')",
        "54",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(dow from date '2008-2-23')",
        "7",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(dow from date '2008-2-24')",
        "1",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isodow from date '2008-2-23')",
        "6",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isodow from date '2008-2-24')",
        "7",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(week from date '2008-2-23')",
        "8",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(week from timestamp '2008-2-23 01:23:45')",
        "8",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(week from cast(null as date))",
        null,
        "BIGINT");

    tester.checkScalar(
        "extract(decade from date '2008-2-23')",
        "200",
        "BIGINT NOT NULL");

    tester.checkScalar("extract(century from date '2008-2-23')",
        "21", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '2001-01-01')",
        "21", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '2000-12-31')",
        "20", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '1852-06-07')",
        "19", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '0001-02-01')",
        "1", "BIGINT NOT NULL");

    tester.checkScalar("extract(millennium from date '2000-2-23')",
        "2", "BIGINT NOT NULL");
    tester.checkScalar("extract(millennium from date '1969-2-23')",
        "2", "BIGINT NOT NULL");
    tester.checkScalar("extract(millennium from date '2000-12-31')",
        "2", "BIGINT NOT NULL");
    tester.checkScalar("extract(millennium from date '2001-01-01')",
        "3", "BIGINT NOT NULL");
  }

  @Test void testExtractTimestamp() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(epoch from timestamp '2008-2-23 12:34:56')",
        "1203770096", // number of seconds elapsed since timestamp
        // '1970-01-01 00:00:00' for given date
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(second from timestamp '2008-2-23 12:34:56')",
        "56",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(millisecond from timestamp '2008-2-23 12:34:56')",
        "56000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(microsecond from timestamp '2008-2-23 12:34:56')",
        "56000000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(nanosecond from timestamp '2008-2-23 12:34:56')",
        "56000000000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from timestamp '2008-2-23 12:34:56')",
        "34",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(hour from timestamp '2008-2-23 12:34:56')",
        "12",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(day from timestamp '2008-2-23 12:34:56')",
        "23",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from timestamp '2008-2-23 12:34:56')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(quarter from timestamp '2008-7-23 12:34:56')",
        "3",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from timestamp '2008-2-23 12:34:56')",
        "2008",
        "BIGINT NOT NULL");

    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "extract(doy from timestamp '2008-2-23 12:34:56')",
          "cannot translate call EXTRACT.*",
          true);

      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "extract(dow from timestamp '2008-2-23 12:34:56')",
          "cannot translate call EXTRACT.*",
          true);

      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "extract(week from timestamp '2008-2-23 12:34:56')",
          "cannot translate call EXTRACT.*",
          true);
    }

    tester.checkScalar(
        "extract(decade from timestamp '2008-2-23 12:34:56')",
        "200",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(century from timestamp '2008-2-23 12:34:56')",
        "21",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from timestamp '2001-01-01 12:34:56')",
        "21",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from timestamp '2000-12-31 12:34:56')",
        "20",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millennium from timestamp '2008-2-23 12:34:56')",
        "3",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millennium from timestamp '2000-2-23 12:34:56')",
        "2",
        "BIGINT NOT NULL");
  }

  @Test void testExtractFunc() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(day from interval '2 3:4:5.678' day to second)",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(day from interval '23456 3:4:5.678' day(5) to second)",
        "23456",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(hour from interval '2 3:4:5.678' day to second)",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(minute from interval '2 3:4:5.678' day to second)",
        "4",
        "BIGINT NOT NULL");

    // TODO: Seconds should include precision
    tester.checkScalar(
        "extract(second from interval '2 3:4:5.678' day to second)",
        "5",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millisecond from interval '2 3:4:5.678' day to second)",
        "5678",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(microsecond from interval '2 3:4:5.678' day to second)",
        "5678000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(nanosecond from interval '2 3:4:5.678' day to second)",
        "5678000000",
        "BIGINT NOT NULL");

    tester.checkNull(
        "extract(month from cast(null as interval year))");
  }

  @Test void testExtractFuncFromDateTime() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(year from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isoyear from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from date '2008-2-23')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from timestamp '2008-2-23 12:34:56')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from timestamp '2008-2-23 12:34:56')",
        "34",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from time '12:23:34')",
        "23",
        "BIGINT NOT NULL");

    tester.checkNull(
        "extract(month from cast(null as timestamp))");

    tester.checkNull(
        "extract(month from cast(null as date))");

    tester.checkNull(
        "extract(second from cast(null as time))");

    tester.checkNull(
        "extract(millisecond from cast(null as time))");

    tester.checkNull(
        "extract(microsecond from cast(null as time))");

    tester.checkNull(
        "extract(nanosecond from cast(null as time))");
  }

  @Test void testExtractWithDatesBeforeUnixEpoch() {

    tester.checkScalar(
            "extract(millisecond from TIMESTAMP '1969-12-31 21:13:17.357')",
            "17357",
            "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from TIMESTAMP '1970-01-01 00:00:00')",
        "1970",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(year from TIMESTAMP '1969-12-31 10:13:17')",
        "1969",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(quarter from TIMESTAMP '1969-12-31 08:13:17')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(quarter from TIMESTAMP '1969-5-31 21:13:17')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(month from TIMESTAMP '1969-12-31 00:13:17')",
        "12",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(day from TIMESTAMP '1969-12-31 12:13:17')",
        "31",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(week from TIMESTAMP '1969-2-23 01:23:45')",
        "8",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(doy from TIMESTAMP '1969-12-31 21:13:17.357')",
        "365",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(dow from TIMESTAMP '1969-12-31 01:13:17.357')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(decade from TIMESTAMP '1969-12-31 21:13:17.357')",
        "196",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(century from TIMESTAMP '1969-12-31 21:13:17.357')",
        "20",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(hour from TIMESTAMP '1969-12-31 21:13:17.357')",
        "21",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(minute from TIMESTAMP '1969-12-31 21:13:17.357')",
        "13",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(second from TIMESTAMP '1969-12-31 21:13:17.357')",
        "17",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(microsecond from TIMESTAMP '1969-12-31 21:13:17.357')",
        "17357000",
        "BIGINT NOT NULL");
  }

  @Test void testArrayValueConstructor() {
    tester.setFor(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);
    tester.checkScalar(
        "Array['foo', 'bar']",
        "[foo, bar]",
        "CHAR(3) NOT NULL ARRAY NOT NULL");

    // empty array is illegal per SQL spec. presumably because one can't
    // infer type
    tester.checkFails(
        "^Array[]^", "Require at least 1 argument", false);
  }

  @Test void testItemOp() {
    tester.setFor(SqlStdOperatorTable.ITEM);
    tester.checkScalar("ARRAY ['foo', 'bar'][1]", "foo", "CHAR(3)");
    tester.checkScalar("ARRAY ['foo', 'bar'][0]", null, "CHAR(3)");
    tester.checkScalar("ARRAY ['foo', 'bar'][2]", "bar", "CHAR(3)");
    tester.checkScalar("ARRAY ['foo', 'bar'][3]", null, "CHAR(3)");
    tester.checkNull(
        "ARRAY ['foo', 'bar'][1 + CAST(NULL AS INTEGER)]");
    tester.checkFails(
        "^ARRAY ['foo', 'bar']['baz']^",
        "Cannot apply 'ITEM' to arguments of type 'ITEM\\(<CHAR\\(3\\) ARRAY>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<VALUE>\\]\n"
            + "<VARIANT>\\[<ANY>\\]",
        false);

    // Array of INTEGER NOT NULL is interesting because we might be tempted
    // to represent the result as Java "int".
    tester.checkScalar("ARRAY [2, 4, 6][2]", "4", "INTEGER");
    tester.checkScalar("ARRAY [2, 4, 6][4]", null, "INTEGER");

    // Map item
    tester.checkScalarExact(
        "map['foo', 3, 'bar', 7]['bar']", "INTEGER", "7");
    tester.checkScalarExact(
        "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['bar']", "INTEGER",
        "7");
    tester.checkScalarExact(
        "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['baz']",
        "INTEGER",
        null);
    tester.checkColumnType(
        "select cast(null as any)['x'] from (values(1))",
        "ANY");
  }

  @Test void testMapValueConstructor() {
    tester.setFor(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, VM_JAVA);

    tester.checkFails(
        "^Map[]^", "Map requires at least 2 arguments", false);

    tester.checkFails(
        "^Map[1, 'x', 2]^",
        "Map requires an even number of arguments",
        false);

    tester.checkFails(
        "^map[1, 1, 2, 'x']^", "Parameters must be of the same type",
        false);
    tester.checkScalarExact(
        "map['washington', 1, 'obama', 44]",
        "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
        "{washington=1, obama=44}");

    final SqlTester tester2 =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    tester2.checkScalarExact(
        "map['washington', 1, 'obama', 44]",
        "(VARCHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
        "{washington=1, obama=44}");
  }

  @Test void testCeilFunc() {
    tester.setFor(SqlStdOperatorTable.CEIL, VM_FENNEL);
    tester.checkScalarApprox("ceil(10.1e0)", "DOUBLE NOT NULL", 11, 0);
    tester.checkScalarApprox("ceil(cast(-11.2e0 as real))", "REAL NOT NULL",
        -11, 0);
    tester.checkScalarExact("ceil(100)", "INTEGER NOT NULL", "100");
    tester.checkScalarExact(
        "ceil(1.3)", "DECIMAL(2, 0) NOT NULL", "2");
    tester.checkScalarExact(
        "ceil(-1.7)", "DECIMAL(2, 0) NOT NULL", "-1");
    tester.checkNull("ceiling(cast(null as decimal(2,0)))");
    tester.checkNull("ceiling(cast(null as double))");
  }

  @Test void testCeilFuncInterval() {
    if (!enable) {
      return;
    }
    tester.checkScalar(
        "ceil(interval '3:4:5' hour to second)",
        "+4:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "ceil(interval '-6.3' second)",
        "-6.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "ceil(interval '5-1' year to month)",
        "+6-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "ceil(interval '-5-1' year to month)",
        "-5-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull(
        "ceil(cast(null as interval year))");
  }

  @Test void testFloorFunc() {
    tester.setFor(SqlStdOperatorTable.FLOOR, VM_FENNEL);
    tester.checkScalarApprox("floor(2.5e0)", "DOUBLE NOT NULL", 2, 0);
    tester.checkScalarApprox("floor(cast(-1.2e0 as real))", "REAL NOT NULL", -2,
        0);
    tester.checkScalarExact("floor(100)", "INTEGER NOT NULL", "100");
    tester.checkScalarExact(
        "floor(1.7)", "DECIMAL(2, 0) NOT NULL", "1");
    tester.checkScalarExact(
        "floor(-1.7)", "DECIMAL(2, 0) NOT NULL", "-2");
    tester.checkNull("floor(cast(null as decimal(2,0)))");
    tester.checkNull("floor(cast(null as real))");
  }

  @Test public void testFloorFuncDateTime() {
    strictTester.checkFails("^floor('12:34:56')^",
        "Cannot apply 'FLOOR' to arguments of type 'FLOOR\\(<CHAR\\(8\\)>\\)'\\. Supported form\\(s\\): 'FLOOR\\(<NUMERIC>\\)'\n"
            + "'FLOOR\\(<DATETIME_INTERVAL>\\)'\n"
            + "'FLOOR\\(<DATE> TO <TIME_UNIT>\\)'\n"
            + "'FLOOR\\(<TIME> TO <TIME_UNIT>\\)'\n"
            + "'FLOOR\\(<TIMESTAMP> TO <TIME_UNIT>\\)'",
        false);
    tester.checkType("floor('12:34:56')", "DECIMAL(19, 0) NOT NULL");
    tester.checkFails("^floor(time '12:34:56')^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    tester.checkFails("^floor(123.45 to minute)^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    tester.checkFails("^floor('abcde' to minute)^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    tester.checkFails("^floor(timestamp '2015-02-19 12:34:56.78' to microsecond)^",
            "(?s)Encountered \"microsecond\" at .*", false);
    tester.checkFails("^floor(timestamp '2015-02-19 12:34:56.78' to nanosecond)^",
            "(?s)Encountered \"nanosecond\" at .*", false);
    tester.checkScalar(
        "floor(time '12:34:56' to minute)", "12:34:00", "TIME(0) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56.78' to second)",
        "2015-02-19 12:34:56", "TIMESTAMP(2) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56' to minute)",
        "2015-02-19 12:34:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56' to year)",
        "2015-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("floor(date '2015-02-19' to year)",
        "2015-01-01", "DATE NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56' to month)",
        "2015-02-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("floor(date '2015-02-19' to month)",
        "2015-02-01", "DATE NOT NULL");
    tester.checkNull("floor(cast(null as timestamp) to month)");
    tester.checkNull("floor(cast(null as date) to month)");
  }

  @Test public void testCeilFuncDateTime() {
    strictTester.checkFails("^ceil('12:34:56')^",
        "Cannot apply 'CEIL' to arguments of type 'CEIL\\(<CHAR\\(8\\)>\\)'\\. Supported form\\(s\\): 'CEIL\\(<NUMERIC>\\)'\n"
            + "'CEIL\\(<DATETIME_INTERVAL>\\)'\n"
            + "'CEIL\\(<DATE> TO <TIME_UNIT>\\)'\n"
            + "'CEIL\\(<TIME> TO <TIME_UNIT>\\)'\n"
            + "'CEIL\\(<TIMESTAMP> TO <TIME_UNIT>\\)'",
        false);
    tester.checkType("ceil('12:34:56')", "DECIMAL(19, 0) NOT NULL");
    tester.checkFails("^ceil(time '12:34:56')^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    tester.checkFails("^ceil(123.45 to minute)^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    tester.checkFails("^ceil('abcde' to minute)^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    tester.checkFails("^ceil(timestamp '2015-02-19 12:34:56.78' to microsecond)^",
            "(?s)Encountered \"microsecond\" at .*", false);
    tester.checkFails("^ceil(timestamp '2015-02-19 12:34:56.78' to nanosecond)^",
            "(?s)Encountered \"nanosecond\" at .*", false);
    tester.checkScalar("ceil(time '12:34:56' to minute)",
        "12:35:00", "TIME(0) NOT NULL");
    tester.checkScalar("ceil(time '12:59:56' to minute)",
        "13:00:00", "TIME(0) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56.78' to second)",
        "2015-02-19 12:34:57", "TIMESTAMP(2) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56.00' to second)",
        "2015-02-19 12:34:56", "TIMESTAMP(2) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to minute)",
        "2015-02-19 12:35:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to year)",
        "2016-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("ceil(date '2015-02-19' to year)",
        "2016-01-01", "DATE NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to month)",
        "2015-03-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("ceil(date '2015-02-19' to month)",
        "2015-03-01", "DATE NOT NULL");
    tester.checkNull("ceil(cast(null as timestamp) to month)");
    tester.checkNull("ceil(cast(null as date) to month)");

    // ceiling alias
    tester.checkScalar("ceiling(timestamp '2015-02-19 12:34:56' to month)",
        "2015-03-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("ceiling(date '2015-02-19' to month)",
        "2015-03-01", "DATE NOT NULL");
    tester.checkNull("ceiling(cast(null as timestamp) to month)");
  }

  @Test void testFloorFuncInterval() {
    if (!enable) {
      return;
    }
    tester.checkScalar(
        "floor(interval '3:4:5' hour to second)",
        "+3:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '-6.3' second)",
        "-7.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '5-1' year to month)",
        "+5-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '-5-1' year to month)",
        "-6-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '-6.3' second to second)",
        "-7.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '6-3' minute to second to minute)",
        "-7-0",
        "INTERVAL MINUTE TO SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '6-3' hour to minute to hour)",
        "7-0",
        "INTERVAL HOUR TO MINUTE NOT NULL");
    tester.checkScalar(
        "floor(interval '6 3' day to hour to day)",
        "7 00",
        "INTERVAL DAY TO HOUR NOT NULL");
    tester.checkScalar(
        "floor(interval '102-7' year to month to month)",
        "102-07",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '102-7' year to month to quarter)",
        "102-10",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '102-1' year to month to century)",
        "201",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '1004-1' year to month to millennium)",
        "2001-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull(
        "floor(cast(null as interval year))");
  }

  @Test public void testTimestampDateCompare() {
    tester.setFor(SqlStdOperatorTable.LESS_THAN);
    tester.checkScalar(
        "timestamp '2016-02-24 12:42:25' < date '2016-2-25'",
        "true",
        "BOOLEAN NOT NULL");
    tester.checkScalar(
        "date '2008-2-23' < timestamp '2016-02-24 12:42:25'",
        "true",
        "BOOLEAN NOT NULL");

    tester.setFor(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
    tester.checkScalar(
        "timestamp '2016-02-24 12:42:25' <= date '2016-02-24'",
        "false",
        "BOOLEAN NOT NULL");
    tester.checkScalar(
        "date '2016-02-24' <= timestamp '2016-02-24 12:42:25'",
        "true",
        "BOOLEAN NOT NULL");

    tester.setFor(SqlStdOperatorTable.EQUALS);
    tester.checkScalar(
        "timestamp '2016-02-24 12:42:25' = date '2016-02-24'",
        "false",
        "BOOLEAN NOT NULL");
    tester.checkScalar(
        "date '2016-02-24' = timestamp '2016-02-24 12:42:25'",
        "false",
        "BOOLEAN NOT NULL");
    tester.checkScalar(
        "date '2016-02-24' = timestamp '2016-02-24 00:00:00'",
        "true",
        "BOOLEAN NOT NULL");

    tester.setFor(SqlStdOperatorTable.GREATER_THAN);
    tester.checkScalar(
        "timestamp '2016-02-24 12:42:25' > date '2016-2-25'",
        "false",
        "BOOLEAN NOT NULL");
    tester.checkScalar(
        "date '2008-2-23' > timestamp '2016-02-24 12:42:25'",
        "false",
        "BOOLEAN NOT NULL");

    tester.setFor(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    tester.checkScalar(
        "timestamp '2016-02-24 12:42:25' >= date '2016-02-24'",
        "true",
        "BOOLEAN NOT NULL");
    tester.checkScalar(
        "date '2016-02-24' >= timestamp '2016-02-24 12:42:25'",
        "false",
        "BOOLEAN NOT NULL");
  }

  @Test public void testTimestampAdd() {
    tester.setFor(SqlStdOperatorTable.TIMESTAMP_ADD);
    tester.checkScalar(
        "timestampadd(MICROSECOND, 2000000, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:27",
        "TIMESTAMP(3) NOT NULL");
    tester.checkScalar(
        "timestampadd(SQL_TSI_SECOND, 2, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:27",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(NANOSECOND, 3000000000, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:28",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(SQL_TSI_FRAC_SECOND, 2000000000, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:27",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(MINUTE, 2, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:44:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(HOUR, -2000, timestamp '2016-02-24 12:42:25')",
        "2015-12-03 04:42:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkNull("timestampadd(HOUR, CAST(NULL AS INTEGER),"
        + " timestamp '2016-02-24 12:42:25')");
    tester.checkNull(
        "timestampadd(HOUR, -200, CAST(NULL AS TIMESTAMP))");
    tester.checkScalar(
        "timestampadd(MONTH, 3, timestamp '2016-02-24 12:42:25')",
        "2016-05-24 12:42:25", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(MONTH, 3, cast(null as timestamp))",
        null, "TIMESTAMP(0)");
    tester.checkScalar(
        "timestampadd(YEAR, 2, timestamp '2016-02-24 12:42:25')",
        "2018-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");

    // TIMESTAMPADD with DATE; returns a TIMESTAMP value for sub-day intervals.
    tester.checkScalar("timestampadd(MONTH, 1, date '2016-06-15')",
        "2016-07-15", "DATE NOT NULL");
    tester.checkScalar("timestampadd(DAY, 1, date '2016-06-15')",
        "2016-06-16", "DATE NOT NULL");
    tester.checkScalar("timestampadd(HOUR, -1, date '2016-06-15')",
        "2016-06-14 23:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, 1, date '2016-06-15')",
        "2016-06-15 00:01:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(SQL_TSI_SECOND, -1, date '2016-06-15')",
        "2016-06-14 23:59:59", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(SECOND, 1, date '2016-06-15')",
        "2016-06-15 00:00:01", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(SECOND, 1, cast(null as date))",
        null, "TIMESTAMP(0)");
    tester.checkScalar("timestampadd(DAY, 1, cast(null as date))",
        null, "DATE");

    // Round to the last day of previous month
    tester.checkScalar("timestampadd(MONTH, 1, date '2016-05-31')",
        "2016-06-30", "DATE NOT NULL");
    tester.checkScalar("timestampadd(MONTH, 5, date '2016-01-31')",
        "2016-06-30", "DATE NOT NULL");
    tester.checkScalar("timestampadd(MONTH, -1, date '2016-03-31')",
        "2016-02-29", "DATE NOT NULL");

    // TIMESTAMPADD with time; returns a time value.The interval is positive.
    tester.checkScalar("timestampadd(SECOND, 1, time '23:59:59')",
        "00:00:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, 1, time '00:00:00')",
        "00:01:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, 1, time '23:59:59')",
        "00:00:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(HOUR, 1, time '23:59:59')",
        "00:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(DAY, 15, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(WEEK, 3, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MONTH, 6, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(QUARTER, 1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(YEAR, 10, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    // TIMESTAMPADD with time; returns a time value .The interval is negative.
    tester.checkScalar("timestampadd(SECOND, -1, time '00:00:00')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, -1, time '00:00:00')",
        "23:59:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(HOUR, -1, time '00:00:00')",
        "23:00:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(DAY, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(WEEK, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MONTH, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(QUARTER, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(YEAR, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
  }

  @Test void testTimestampAddFractionalSeconds() {
    tester.setFor(SqlStdOperatorTable.TIMESTAMP_ADD);
    tester.checkType(
        "timestampadd(SQL_TSI_FRAC_SECOND, 2, timestamp '2016-02-24 12:42:25.000000')",
        // "2016-02-24 12:42:25.000002",
        "TIMESTAMP(3) NOT NULL");

    // The following test would correctly return "TIMESTAMP(6) NOT NULL" if max
    // precision were 6 or higher
    assumeTrue(tester.getValidator().getTypeFactory().getTypeSystem()
        .getMaxPrecision(SqlTypeName.TIMESTAMP) == 3);
    tester.checkType(
        "timestampadd(MICROSECOND, 2, timestamp '2016-02-24 12:42:25.000000')",
        // "2016-02-24 12:42:25.000002",
        "TIMESTAMP(3) NOT NULL");
  }

  @Test void testTimestampDiff() {
    tester.setFor(SqlStdOperatorTable.TIMESTAMP_DIFF);
    tester.checkScalar("timestampdiff(HOUR, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 15:42:25')",
        "3", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MICROSECOND, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:20')",
        "-5000000", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(SQL_TSI_FRAC_SECOND, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:20')",
        "-5000000000", "BIGINT NOT NULL");
    tester.checkScalar("timestampdiff(NANOSECOND, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:20')",
        "-5000000000", "BIGINT NOT NULL");
    tester.checkScalar("timestampdiff(YEAR, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "2", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(WEEK, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "104", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(WEEK, "
        + "timestamp '2014-02-19 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "105", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MONTH, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "24", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MONTH, "
        + "timestamp '2019-09-01 00:00:00', "
        + "timestamp '2020-03-01 00:00:00')",
        "6", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MONTH, "
        + "timestamp '2019-09-01 00:00:00', "
        + "timestamp '2016-08-01 00:00:00')",
        "-37", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(QUARTER, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "8", "INTEGER NOT NULL");
    tester.checkFails("timestampdiff(CENTURY, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2614-02-24 12:42:25')",
        "(?s)Encountered \"CENTURY\" at .*", false);
    tester.checkScalar("timestampdiff(QUARTER, "
        + "timestamp '2014-02-24 12:42:25', "
        + "cast(null as timestamp))",
        null, "INTEGER");
    tester.checkScalar("timestampdiff(QUARTER, "
        + "cast(null as timestamp), "
        + "timestamp '2014-02-24 12:42:25')",
        null, "INTEGER");

    // timestampdiff with date
    tester.checkScalar(
        "timestampdiff(MONTH, date '2016-03-15', date '2016-06-14')",
        "2",
        "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MONTH, date '2019-09-01', date '2020-03-01')",
        "6",
        "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MONTH, date '2019-09-01', date '2016-08-01')",
        "-37",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(DAY, date '2016-06-15', date '2016-06-14')",
        "-1",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(HOUR, date '2016-06-15', date '2016-06-14')",
        "-24",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(MINUTE, date '2016-06-15',  date '2016-06-15')",
        "0",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(SECOND, cast(null as date), date '2016-06-15')",
        null,
        "INTEGER");
    tester.checkScalar(
        "timestampdiff(DAY, date '2016-06-15', cast(null as date))",
        null,
        "INTEGER");
  }

  @Test void testDenseRankFunc() {
    tester.setFor(
        SqlStdOperatorTable.DENSE_RANK, VM_FENNEL, VM_JAVA);
  }

  @Test void testPercentRankFunc() {
    tester.setFor(
        SqlStdOperatorTable.PERCENT_RANK,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test void testRankFunc() {
    tester.setFor(SqlStdOperatorTable.RANK, VM_FENNEL, VM_JAVA);
  }

  @Test void testCumeDistFunc() {
    tester.setFor(
        SqlStdOperatorTable.CUME_DIST,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test void testRowNumberFunc() {
    tester.setFor(
        SqlStdOperatorTable.ROW_NUMBER,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test void testCountFunc() {
    tester.setFor(SqlStdOperatorTable.COUNT, VM_EXPAND);
    tester.checkType("count(*)", "BIGINT NOT NULL");
    tester.checkType("count('name')", "BIGINT NOT NULL");
    tester.checkType("count(1)", "BIGINT NOT NULL");
    tester.checkType("count(1.2)", "BIGINT NOT NULL");
    tester.checkType("COUNT(DISTINCT 'x')", "BIGINT NOT NULL");
    tester.checkFails(
        "^COUNT()^",
        "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
        false);
    tester.checkType("count(1, 2)", "BIGINT NOT NULL");
    tester.checkType("count(1, 2, 'x', 'y')", "BIGINT NOT NULL");
    final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
    tester.checkAgg(
        "COUNT(x)",
        values,
        3,
        (double) 0);
    tester.checkAgg(
        "COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        2,
        (double) 0);
    tester.checkAgg(
        "COUNT(DISTINCT x)",
        values,
        2,
        (double) 0);

    // string values -- note that empty string is not null
    final String[] stringValues = {
        "'a'", "CAST(NULL AS VARCHAR(1))", "''"
    };
    tester.checkAgg("COUNT(*)", stringValues, 3, (double) 0);
    tester.checkAgg("COUNT(x)", stringValues, 2, (double) 0);
    tester.checkAgg("COUNT(DISTINCT x)", stringValues, 2, (double) 0);
    tester.checkAgg("COUNT(DISTINCT 123)", stringValues, 1, (double) 0);
  }

  @Test void testApproxCountDistinctFunc() {
    tester.setFor(SqlStdOperatorTable.COUNT, VM_EXPAND);
    tester.checkFails("approx_count_distinct(^*^)", "Unknown identifier '\\*'",
        false);
    tester.checkType("approx_count_distinct('name')", "BIGINT NOT NULL");
    tester.checkType("approx_count_distinct(1)", "BIGINT NOT NULL");
    tester.checkType("approx_count_distinct(1.2)", "BIGINT NOT NULL");
    tester.checkType("APPROX_COUNT_DISTINCT(DISTINCT 'x')", "BIGINT NOT NULL");
    tester.checkFails("^APPROX_COUNT_DISTINCT()^",
        "Invalid number of arguments to function 'APPROX_COUNT_DISTINCT'. "
            + "Was expecting 1 arguments",
        false);
    tester.checkType("approx_count_distinct(1, 2)", "BIGINT NOT NULL");
    tester.checkType("approx_count_distinct(1, 2, 'x', 'y')",
        "BIGINT NOT NULL");
    final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
    // currently APPROX_COUNT_DISTINCT(x) returns the same as COUNT(DISTINCT x)
    tester.checkAgg(
        "APPROX_COUNT_DISTINCT(x)",
        values,
        2,
        (double) 0);
    tester.checkAgg(
        "APPROX_COUNT_DISTINCT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        1,
        (double) 0);
    // DISTINCT keyword is allowed but has no effect
    tester.checkAgg(
        "APPROX_COUNT_DISTINCT(DISTINCT x)",
        values,
        2,
        (double) 0);

    // string values -- note that empty string is not null
    final String[] stringValues = {
        "'a'", "CAST(NULL AS VARCHAR(1))", "''"
    };
    tester.checkAgg("APPROX_COUNT_DISTINCT(x)", stringValues, 2, (double) 0);
    tester.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT x)", stringValues, 2,
        (double) 0);
    tester.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT 123)", stringValues, 1,
        (double) 0);
  }

  @Test void testSumFunc() {
    tester.setFor(SqlStdOperatorTable.SUM, VM_EXPAND);
    tester.checkFails(
        "sum(^*^)", "Unknown identifier '\\*'", false);
    strictTester.checkFails(
        "^sum('name')^",
        "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("sum('name')", "DECIMAL(19, 19)");
    checkAggType(tester, "sum(1)", "INTEGER NOT NULL");
    checkAggType(tester, "sum(1.2)", "DECIMAL(2, 1) NOT NULL");
    checkAggType(tester, "sum(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    tester.checkFails(
        "^sum()^",
        "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^sum(1, 2)^",
        "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
        false);
    strictTester.checkFails(
        "^sum(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("sum(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    tester.checkAgg("sum(x)", values, 4, (double) 0);
    Object result1 = -3;
    if (!enable) {
      return;
    }
    tester.checkAgg("sum(CASE x WHEN 0 THEN NULL ELSE -1 END)", values, result1,
        (double) 0);
    Object result = -1;
    tester.checkAgg("sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        result, (double) 0);
    tester.checkAgg("sum(DISTINCT x)", values, 2, (double) 0);
  }

  /** Very similar to {@code tester.checkType}, but generates inside a SELECT
   * with a non-empty GROUP BY. Aggregate functions may be nullable if executed
   * in a SELECT with an empty GROUP BY.
   *
   * <p>Viz: {@code SELECT sum(1) FROM emp} has type "INTEGER",
   * {@code SELECT sum(1) FROM emp GROUP BY deptno} has type "INTEGER NOT NULL",
   */
  protected void checkAggType(SqlTester tester, String expr, String type) {
    tester.checkColumnType(AbstractSqlTester.buildQueryAgg(expr), type);
  }

  @Test void testAvgFunc() {
    tester.setFor(SqlStdOperatorTable.AVG, VM_EXPAND);
    tester.checkFails(
        "avg(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^avg(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'AVG' to arguments of type 'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'AVG\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("avg(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "AVG(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    checkAggType(tester, "avg(1)", "INTEGER NOT NULL");
    checkAggType(tester, "avg(1.2)", "DECIMAL(2, 1) NOT NULL");
    checkAggType(tester, "avg(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    tester.checkAgg("AVG(x)", values, 2d, 0d);
    tester.checkAgg("AVG(DISTINCT x)", values, 1.5d, 0d);
    Object result = -1;
    tester.checkAgg("avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        result, 0d);
  }

  @Test void testCovarPopFunc() {
    tester.setFor(SqlStdOperatorTable.COVAR_POP, VM_EXPAND);
    tester.checkFails("covar_pop(^*^)", "Unknown identifier '\\*'", false);
    strictTester.checkFails(
        "^covar_pop(cast(null as varchar(2)),cast(null as varchar(2)))^",
        "(?s)Cannot apply 'COVAR_POP' to arguments of type 'COVAR_POP\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'COVAR_POP\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("covar_pop(cast(null as varchar(2)),cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("covar_pop(CAST(NULL AS INTEGER),CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "covar_pop(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("covar_pop(x)", new String[]{}, null, 0d);
  }

  @Test void testCovarSampFunc() {
    tester.setFor(SqlStdOperatorTable.COVAR_SAMP, VM_EXPAND);
    tester.checkFails(
        "covar_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^covar_samp(cast(null as varchar(2)),cast(null as varchar(2)))^",
        "(?s)Cannot apply 'COVAR_SAMP' to arguments of type 'COVAR_SAMP\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'COVAR_SAMP\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("covar_samp(cast(null as varchar(2)),cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("covar_samp(CAST(NULL AS INTEGER),CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "covar_samp(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("covar_samp(x)", new String[]{}, null, 0d);
  }

  @Test void testRegrSxxFunc() {
    tester.setFor(SqlStdOperatorTable.REGR_SXX, VM_EXPAND);
    tester.checkFails(
        "regr_sxx(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^regr_sxx(cast(null as varchar(2)), cast(null as varchar(2)))^",
        "(?s)Cannot apply 'REGR_SXX' to arguments of type 'REGR_SXX\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'REGR_SXX\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("regr_sxx(cast(null as varchar(2)), cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("regr_sxx(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "regr_sxx(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("regr_sxx(x)", new String[]{}, null, 0d);
  }

  @Test void testRegrSyyFunc() {
    tester.setFor(SqlStdOperatorTable.REGR_SYY, VM_EXPAND);
    tester.checkFails(
        "regr_syy(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^regr_syy(cast(null as varchar(2)), cast(null as varchar(2)))^",
        "(?s)Cannot apply 'REGR_SYY' to arguments of type 'REGR_SYY\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'REGR_SYY\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("regr_syy(cast(null as varchar(2)), cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("regr_syy(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "regr_syy(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("regr_syy(x)", new String[]{}, null, 0d);
  }

  @Test void testStddevPopFunc() {
    tester.setFor(SqlStdOperatorTable.STDDEV_POP, VM_EXPAND);
    tester.checkFails("stddev_pop(^*^)", "Unknown identifier '\\*'", false);
    strictTester.checkFails("^stddev_pop(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'STDDEV_POP' to arguments of type 'STDDEV_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_POP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("stddev_pop(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("stddev_pop(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "stddev_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (enable) {
      // verified on Oracle 10g
      tester.checkAgg("stddev_pop(x)", values, 1.414213562373095d,
          0.000000000000001d);
      // Oracle does not allow distinct
      tester.checkAgg("stddev_pop(DISTINCT x)", values, 1.5d, 0d);
      tester.checkAgg("stddev_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
          values, 0, 0d);
    }
    // with one value
    tester.checkAgg("stddev_pop(x)", new String[]{"5"}, 0, 0d);
    // with zero values
    tester.checkAgg("stddev_pop(x)", new String[]{}, null, 0d);
  }

  @Test void testStddevSampFunc() {
    tester.setFor(SqlStdOperatorTable.STDDEV_SAMP, VM_EXPAND);
    tester.checkFails(
        "stddev_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^stddev_samp(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'STDDEV_SAMP' to arguments of type 'STDDEV_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_SAMP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("stddev_samp(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("stddev_samp(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "stddev_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (enable) {
      // verified on Oracle 10g
      tester.checkAgg("stddev_samp(x)", values, 1.732050807568877d,
          0.000000000000001d);
      // Oracle does not allow distinct
      tester.checkAgg("stddev_samp(DISTINCT x)", values, 2.121320343559642d,
          0.000000000000001d);
      tester.checkAgg(
          "stddev_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
          values,
          null,
          0d);
    }
    // with one value
    tester.checkAgg(
        "stddev_samp(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "stddev_samp(x)",
        new String[]{},
        null,
        0d);
  }

  @Test void testStddevFunc() {
    tester.setFor(SqlStdOperatorTable.STDDEV, VM_EXPAND);
    tester.checkFails(
        "stddev(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^stddev(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'STDDEV' to arguments of type 'STDDEV\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("stddev(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("stddev(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "stddev(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    // with one value
    tester.checkAgg(
        "stddev(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "stddev(x)",
        new String[]{},
        null,
        0d);
  }

  @Test void testVarPopFunc() {
    tester.setFor(SqlStdOperatorTable.VAR_POP, VM_EXPAND);
    tester.checkFails(
        "var_pop(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^var_pop(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'VAR_POP' to arguments of type 'VAR_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_POP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("var_pop(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("var_pop(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "var_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "var_pop(x)",
        values,
        2d, // verified on Oracle 10g
        0d);
    tester.checkAgg(
        "var_pop(DISTINCT x)", // Oracle does not allow distinct
        values,
        2.25d,
        0.0001d);
    tester.checkAgg(
        "var_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        0,
        0d);
    // with one value
    tester.checkAgg(
        "var_pop(x)",
        new String[]{"5"},
        0,
        0d);
    // with zero values
    tester.checkAgg(
        "var_pop(x)",
        new String[]{},
        null,
        0d);
  }

  @Test void testVarSampFunc() {
    tester.setFor(SqlStdOperatorTable.VAR_SAMP, VM_EXPAND);
    tester.checkFails(
        "var_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^var_samp(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'VAR_SAMP' to arguments of type 'VAR_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_SAMP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("var_samp(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("var_samp(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "var_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "var_samp(x)", values, 3d, // verified on Oracle 10g
        0d);
    tester.checkAgg(
        "var_samp(DISTINCT x)", // Oracle does not allow distinct
        values,
        4.5d,
        0.0001d);
    tester.checkAgg(
        "var_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        null,
        0d);
    // with one value
    tester.checkAgg(
        "var_samp(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "var_samp(x)",
        new String[]{},
        null,
        0d);
  }

  @Test void testVarFunc() {
    tester.setFor(SqlStdOperatorTable.VARIANCE, VM_EXPAND);
    tester.checkFails(
        "variance(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^variance(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'VARIANCE' to arguments of type 'VARIANCE\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VARIANCE\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("variance(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("variance(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "variance(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "variance(x)", values, 3d, // verified on Oracle 10g
        0d);
    tester.checkAgg(
        "variance(DISTINCT x)", // Oracle does not allow distinct
        values,
        4.5d,
        0.0001d);
    tester.checkAgg(
        "variance(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        null,
        0d);
    // with one value
    tester.checkAgg(
        "variance(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "variance(x)",
        new String[]{},
        null,
        0d);
  }

  @Test void testMinFunc() {
    tester.setFor(SqlStdOperatorTable.MIN, VM_EXPAND);
    tester.checkFails(
        "min(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("min(1)", "INTEGER");
    tester.checkType("min(1.2)", "DECIMAL(2, 1)");
    tester.checkType("min(DISTINCT 1.5)", "DECIMAL(2, 1)");
    tester.checkFails(
        "^min()^",
        "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^min(1, 2)^",
        "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "min(x)",
        values,
        "0",
        0d);
    tester.checkAgg(
        "min(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "min(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "min(DISTINCT x)",
        values,
        "0",
        0d);
  }

  @Test void testMaxFunc() {
    tester.setFor(SqlStdOperatorTable.MAX, VM_EXPAND);
    tester.checkFails(
        "max(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("max(1)", "INTEGER");
    tester.checkType("max(1.2)", "DECIMAL(2, 1)");
    tester.checkType("max(DISTINCT 1.5)", "DECIMAL(2, 1)");
    tester.checkFails(
        "^max()^",
        "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^max(1, 2)^",
        "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "max(x)",
        values,
        "2",
        0d);
    tester.checkAgg(
        "max(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "max(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "max(DISTINCT x)", values, "2", 0d);
  }

  @Test void testLastValueFunc() {
    tester.setFor(SqlStdOperatorTable.LAST_VALUE, VM_EXPAND);
    final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkWinAgg("last_value(x)", values, "ROWS 3 PRECEDING", "INTEGER",
        Arrays.asList("3", "0"), 0d);
    final String[] values2 = {"1.6", "1.2"};
    tester.checkWinAgg(
        "last_value(x)",
        values2,
        "ROWS 3 PRECEDING",
        "DECIMAL(2, 1) NOT NULL",
        Arrays.asList("1.6", "1.2"),
        0d);
    final String[] values3 = {"'foo'", "'bar'", "'name'"};
    tester.checkWinAgg(
        "last_value(x)",
        values3,
        "ROWS 3 PRECEDING",
        "CHAR(4) NOT NULL",
        Arrays.asList("foo ", "bar ", "name"),
        0d);
  }

  @Test void testFirstValueFunc() {
    tester.setFor(SqlStdOperatorTable.FIRST_VALUE, VM_EXPAND);
    final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkWinAgg("first_value(x)", values, "ROWS 3 PRECEDING", "INTEGER",
        Arrays.asList("0"), 0d);
    final String[] values2 = {"1.6", "1.2"};
    tester.checkWinAgg(
        "first_value(x)",
        values2,
        "ROWS 3 PRECEDING",
        "DECIMAL(2, 1) NOT NULL",
        Arrays.asList("1.6"),
        0d);
    final String[] values3 = {"'foo'", "'bar'", "'name'"};
    tester.checkWinAgg(
        "first_value(x)",
        values3,
        "ROWS 3 PRECEDING",
        "CHAR(4) NOT NULL",
        Arrays.asList("foo "),
        0d);
  }

  @Test void testEveryFunc() {
    tester.setFor(SqlStdOperatorTable.EVERY, VM_EXPAND);
    tester.checkFails(
        "every(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("every(1 = 1)", "BOOLEAN");
    tester.checkType("every(1.2 = 1.2)", "BOOLEAN");
    tester.checkType("every(1.5 = 1.4)", "BOOLEAN");
    tester.checkFails(
        "^every()^",
        "Invalid number of arguments to function 'EVERY'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^every(1, 2)^",
        "Invalid number of arguments to function 'EVERY'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "every(x = 2)",
        values,
        "false",
        0d);
  }

  @Test void testSomeAggFunc() {
    tester.setFor(SqlStdOperatorTable.SOME, VM_EXPAND);
    tester.checkFails(
        "some(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("some(1 = 1)", "BOOLEAN");
    tester.checkType("some(1.2 = 1.2)", "BOOLEAN");
    tester.checkType("some(1.5 = 1.4)", "BOOLEAN");
    tester.checkFails(
        "^some()^",
        "Invalid number of arguments to function 'SOME'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^some(1, 2)^",
        "Invalid number of arguments to function 'SOME'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "some(x = 2)",
        values,
        "true",
        0d);
  }


  @Test void testAnyValueFunc() {
    tester.setFor(SqlStdOperatorTable.ANY_VALUE, VM_EXPAND);
    tester.checkFails(
        "any_value(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("any_value(1)", "INTEGER");
    tester.checkType("any_value(1.2)", "DECIMAL(2, 1)");
    tester.checkType("any_value(DISTINCT 1.5)", "DECIMAL(2, 1)");
    tester.checkFails(
        "^any_value()^",
        "Invalid number of arguments to function 'ANY_VALUE'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^any_value(1, 2)^",
        "Invalid number of arguments to function 'ANY_VALUE'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "any_value(x)",
        values,
        "0",
        0d);
    tester.checkAgg(
        "any_value(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "any_value(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "any_value(DISTINCT x)",
        values,
        "0",
        0d);
  }
  @Test void testBitAndFunc() {
    tester.setFor(SqlStdOperatorTable.BIT_AND, VM_FENNEL, VM_JAVA);
    tester.checkFails("bit_and(^*^)", "Unknown identifier '\\*'", false);
    tester.checkType("bit_and(1)", "INTEGER");
    tester.checkType("bit_and(CAST(2 AS TINYINT))", "TINYINT");
    tester.checkType("bit_and(CAST(2 AS SMALLINT))", "SMALLINT");
    tester.checkType("bit_and(distinct CAST(2 AS BIGINT))", "BIGINT");
    tester.checkFails("^bit_and(1.2)^",
        "Cannot apply 'BIT_AND' to arguments of type 'BIT_AND\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): 'BIT_AND\\(<INTEGER>\\)'",
        false);
    tester.checkFails(
        "^bit_and()^",
        "Invalid number of arguments to function 'BIT_AND'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^bit_and(1, 2)^",
        "Invalid number of arguments to function 'BIT_AND'. Was expecting 1 arguments",
        false);
    final String[] values = {"3", "2", "2"};
    tester.checkAgg("bit_and(x)", values, 2, 0);
  }

  @Test void testBitOrFunc() {
    tester.setFor(SqlStdOperatorTable.BIT_OR, VM_FENNEL, VM_JAVA);
    tester.checkFails("bit_or(^*^)", "Unknown identifier '\\*'", false);
    tester.checkType("bit_or(1)", "INTEGER");
    tester.checkType("bit_or(CAST(2 AS TINYINT))", "TINYINT");
    tester.checkType("bit_or(CAST(2 AS SMALLINT))", "SMALLINT");
    tester.checkType("bit_or(distinct CAST(2 AS BIGINT))", "BIGINT");
    tester.checkFails("^bit_or(1.2)^",
        "Cannot apply 'BIT_OR' to arguments of type 'BIT_OR\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): 'BIT_OR\\(<INTEGER>\\)'",
        false);
    tester.checkFails(
        "^bit_or()^",
        "Invalid number of arguments to function 'BIT_OR'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^bit_or(1, 2)^",
        "Invalid number of arguments to function 'BIT_OR'. Was expecting 1 arguments",
        false);
    final String[] values = {"1", "2", "2"};
    tester.checkAgg("bit_or(x)", values, 3, 0);
  }

  @Test void testBitXorFunc() {
    tester.setFor(SqlStdOperatorTable.BIT_XOR, VM_FENNEL, VM_JAVA);
    tester.checkFails("bit_xor(^*^)", "Unknown identifier '\\*'", false);
    tester.checkType("bit_xor(1)", "INTEGER");
    tester.checkType("bit_xor(CAST(2 AS TINYINT))", "TINYINT");
    tester.checkType("bit_xor(CAST(2 AS SMALLINT))", "SMALLINT");
    tester.checkType("bit_xor(distinct CAST(2 AS BIGINT))", "BIGINT");
    tester.checkFails("^bit_xor(1.2)^",
        "Cannot apply 'BIT_XOR' to arguments of type 'BIT_XOR\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): 'BIT_XOR\\(<INTEGER>\\)'",
        false);
    tester.checkFails(
        "^bit_xor()^",
        "Invalid number of arguments to function 'BIT_XOR'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^bit_xor(1, 2)^",
        "Invalid number of arguments to function 'BIT_XOR'. Was expecting 1 arguments",
        false);
    final String[] values = {"1", "2", "1"};
    tester.checkAgg("bit_xor(x)", values, 2, 0);
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
