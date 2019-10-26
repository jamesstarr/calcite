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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;

import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link SqlPrettyWriter}.
 *
 * <p>You must provide the system property "source.dir".
 */
public class SqlPrettyWriterTest {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  public SqlPrettyWriterTest() {
  }

  //~ Methods ----------------------------------------------------------------

  // ~ Helper methods -------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(SqlPrettyWriterTest.class);
  }

  /**
   * Parses a SQL query. To use a different parser, override this method.
   */
  protected SqlNode parseQuery(String sql) {
    SqlNode node;
    try {
      node = SqlParser.create(sql).parseQuery();
    } catch (SqlParseException e) {
      String message = "Received error while parsing SQL '" + sql + "'"
          + "; error is:\n"
          + e.toString();
      throw new AssertionError(message);
    }
    return node;
  }

  /** Helper. */
  class Sql {
    private final String sql;
    private final boolean expr;
    private final String desc;
    private final String formatted;
    private final UnaryOperator<SqlWriterConfig> transform;

    Sql(String sql, boolean expr, String desc, String formatted,
        UnaryOperator<SqlWriterConfig> transform) {
      this.sql = Objects.requireNonNull(sql);
      this.expr = expr;
      this.desc = desc;
      this.formatted = Objects.requireNonNull(formatted);
      this.transform = Objects.requireNonNull(transform);
    }

    Sql withWriter(UnaryOperator<SqlWriterConfig> transform) {
      Objects.requireNonNull(transform);
      return new Sql(sql, expr, desc, formatted, w ->
              transform.apply(this.transform.apply(w)));
    }

    Sql expectingDesc(String desc) {
      return Objects.equals(this.desc, desc)
          ? this
          : new Sql(sql, expr, desc, formatted, transform);
    }

    Sql withExpr(boolean expr) {
      return this.expr == expr
          ? this
          : new Sql(sql, expr, desc, formatted, transform);
    }

    Sql expectingFormatted(String formatted) {
      return Objects.equals(this.formatted, formatted)
          ? this
          : new Sql(sql, expr, desc, formatted, transform);
    }

    Sql check() {
      final SqlWriterConfig config =
              transform.apply(SqlPrettyWriter.config()
                      .withDialect(AnsiSqlDialect.DEFAULT));
      final SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config);
      final SqlNode node;
      if (expr) {
        final SqlCall valuesCall = (SqlCall) parseQuery("VALUES (" + sql + ")");
        final SqlCall rowCall = valuesCall.operand(0);
        node = rowCall.operand(0);
      } else {
        node = parseQuery(sql);
      }

      // Describe settings
      if (desc != null) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        prettyWriter.describe(pw, true);
        pw.flush();
        final String desc = sw.toString();
        getDiffRepos().assertEquals("desc", this.desc, desc);
      }

      // Format
      final String formatted = prettyWriter.format(node);
      getDiffRepos().assertEquals("formatted", this.formatted, formatted);

      // Now parse the result, and make sure it is structurally equivalent
      // to the original.
      final String actual2 = formatted.replaceAll("`", "\"");
      final SqlNode node2;
      if (expr) {
        final SqlCall valuesCall =
            (SqlCall) parseQuery("VALUES (" + actual2 + ")");
        final SqlCall rowCall = valuesCall.operand(0);
        node2 = rowCall.operand(0);
      } else {
        node2 = parseQuery(actual2);
      }
      assertTrue(node.equalsDeep(node2, Litmus.THROW));

      return this;
    }
  }

  private Sql simple() {
    return sql("select x as a, b as b, c as c, d,"
        + " 'mixed-Case string',"
        + " unquotedCamelCaseId,"
        + " \"quoted id\" "
        + "from"
        + " (select *"
        + " from t"
        + " where x = y and a > 5"
        + " group by z, zz"
        + " window w as (partition by c),"
        + "  w1 as (partition by c,d order by a, b"
        + "   range between interval '2:2' hour to minute preceding"
        + "    and interval '1' day following)) "
        + "order by gg");
  }

  private Sql sql(String sql) {
    return new Sql(sql, false, null, "${formatted}", w -> w);
  }

  private Sql expr(String sql) {
    return sql(sql).withExpr(true).expectingDesc(null);
  }

  // ~ Tests ----------------------------------------------------------------

  @Test public void testDefault() {
    simple().check();
  }

  @Test public void testIndent8() {
    simple()
        .expectingDesc("${desc}")
        .withWriter(w -> w.withIndentation(8))
        .check();
  }

  @Test public void testClausesNotOnNewLine() {
    simple()
        .withWriter(w -> w.withClauseStartsLine(false))
        .check();
  }

  @Test public void testSelectListItemsOnSeparateLines() {
    simple()
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true))
        .check();
  }

  @Test public void testSelectListNoExtraIndentFlag() {
    simple()
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withClauseEndsLine(true))
        .check();
  }

  @Test public void testFold() {
    simple()
        .withWriter(w -> w.withLineFolding(SqlWriterConfig.LineFolding.FOLD)
            .withFoldLength(45))
        .check();
  }

  @Test public void testChop() {
    simple()
        .withWriter(w -> w.withLineFolding(SqlWriterConfig.LineFolding.CHOP)
            .withFoldLength(45))
        .check();
  }

  @Test public void testChopLeadingComma() {
    simple()
        .withWriter(w -> w.withLineFolding(SqlWriterConfig.LineFolding.CHOP)
            .withFoldLength(45)
            .withLeadingComma(true))
        .check();
  }

  @Test public void testLeadingComma() {
    simple()
        .withWriter(w -> w.withLeadingComma(true)
            .withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(true))
        .check();
  }

  @Test public void testClauseEndsLine() {
    simple()
        .withWriter(w -> w.withClauseEndsLine(true)
            .withLineFolding(SqlWriterConfig.LineFolding.WIDE)
            .withFoldLength(45))
        .check();
  }

  @Test public void testClauseEndsLineTall() {
    simple()
        .withWriter(w -> w.withClauseEndsLine(true)
            .withLineFolding(SqlWriterConfig.LineFolding.TALL)
            .withFoldLength(45))
        .check();
  @Test public void testCase2() {
    final String sql = "case 1"
            + " when 2 + 3 then 4"
            + " when case a when b then c else d end then 6"
            + " else 7 end";
    final String formatted = "CASE WHEN 1 = 2 + 3 THEN 4"
            + " WHEN 1 = CASE WHEN `A` = `B` THEN `C` ELSE `D` END THEN 6"
            + " ELSE 7 END";
    expr(sql)
            .expectingFormatted(formatted)
            .check();
  }

  @Test public void testBetween() {
    // todo: remove leading
    expr("x not between symmetric y and z")
            .expectingFormatted("`X` NOT BETWEEN SYMMETRIC `Y` AND `Z`")
            .check();

    // space
  }

  @Test public void testCast() {
    expr("cast(x + y as decimal(5, 10))")
            .expectingFormatted("CAST(`X` + `Y` AS DECIMAL(5, 10))")
            .check();
  }

  @Test public void testLiteralChain() {
    final String sql = "'x' /* comment */ 'y'\n"
            + "  'z' ";
    final String formatted = "'x'\n"
            + "'y'\n"
            + "'z'";
    expr(sql).expectingFormatted(formatted).check();
  }

  @Test public void testOverlaps() {
    final String sql = "(x,xx) overlaps (y,yy) or x is not null";
    final String formatted = "PERIOD (`X`, `XX`) OVERLAPS PERIOD (`Y`, `YY`)"
            + " OR `X` IS NOT NULL";
    expr(sql).expectingFormatted(formatted).check();
  }

  @Test public void testUnion() {
    final String sql = "select * from t "
        + "union select * from ("
        + "  select * from u "
        + "  union select * from v) "
        + "union select * from w "
        + "order by a, b";
    sql(sql)
        .check();
  }

  @Test public void testMultiset() {
    sql("values (multiset (select * from t))")
        .expectingDesc(null)
        .check();
  }

  @Test public void testJoinComma() {
    final String sql = "select *\n"
        + "from x, y as y1, z, (select * from a, a2 as a3),\n"
        + " (select * from b) as b2\n"
        + "where p = q\n"
        + "and exists (select 1 from v, w)";
    sql(sql).check();
  }

  @Test public void testInnerJoin() {
    sql("select * from x inner join y on x.k=y.k")
        .expectingDesc(null)
        .check();
  }

  @Test public void testJoinTall() {
    sql("select * from x inner join y on x.k=y.k left join z using (a)")
        .withWriter(c -> c.withLineFolding(SqlWriterConfig.LineFolding.TALL))
        .check();
  }

  @Test public void testJoinTallClauseEndsLine() {
    sql("select * from x inner join y on x.k=y.k left join z using (a)")
        .withWriter(c -> c.withLineFolding(SqlWriterConfig.LineFolding.TALL)
            .withClauseEndsLine(true))
        .check();
  }

  @Test public void testWhereListItemsOnSeparateLinesOr() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and i < j"
        + " or ((a or b) is true) and d not in (f,g)"
        + " or x <> z";
    sql(sql)
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withWhereListItemsOnSeparateLines(true))
        .check();
  }

  @Test public void testWhereListItemsOnSeparateLinesAnd() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and (i < j"
        + " or ((a or b) is true)) and (d not in (f,g)"
        + " or v <> ((w * x) + y) * z)";
    sql(sql)
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withWhereListItemsOnSeparateLines(true))
        .check();
  }

  /** As {@link #testWhereListItemsOnSeparateLinesAnd()}, but
   * with {@link SqlWriterConfig#clauseEndsLine ClauseEndsLine=true}. */
  @Test public void testWhereListItemsOnSeparateLinesAndNewline() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and (i < j"
        + " or ((a or b) is true)) and (d not in (f,g)"
        + " or v <> ((w * x) + y) * z)";
    sql(sql)
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withWhereListItemsOnSeparateLines(true)
            .withClauseEndsLine(true))
        .check();
  }

  @Test public void testUpdate() {
    final String sql = "update emp\n"
        + "set mgr = mgr + 1, deptno = 5\n"
        + "where deptno = 10 and name = 'Fred'";
    sql(sql)
        .check();
  }

  @Test public void testUpdateNoLine() {
    final String sql = "update emp\n"
        + "set mgr = mgr + 1, deptno = 5\n"
        + "where deptno = 10 and name = 'Fred'";
    sql(sql)
        .withWriter(w -> w.withUpdateSetListNewline(false))
        .check();
  }

  @Test public void testUpdateNoLine2() {
    final String sql = "update emp\n"
        + "set mgr = mgr + 1, deptno = 5\n"
        + "where deptno = 10 and name = 'Fred'";
    sql(sql)
        .withWriter(w -> w.withUpdateSetListNewline(false)
            .withClauseStartsLine(false))
        .check();
  }

  public static void main(String[] args) throws SqlParseException {
    final String sql = "select x as a, b as b, c as c, d,"
        + " 'mixed-Case string',"
        + " unquotedCamelCaseId,"
        + " \"quoted id\" "
        + "from"
        + " (select *"
        + " from t"
        + " where x = y and a > 5"
        + " group by z, zz"
        + " window w as (partition by c),"
        + "  w1 as (partition by c,d order by a, b"
        + "   range between interval '2:2' hour to minute preceding"
        + "    and interval '1' day following)) "
        + "order by gg desc nulls last, hh asc";
    final SqlNode node = SqlParser.create(sql).parseQuery();

    final SqlWriterConfig config = SqlPrettyWriter.config()
        .withLineFolding(SqlWriterConfig.LineFolding.STEP)
        .withSelectFolding(SqlWriterConfig.LineFolding.TALL)
        .withFromFolding(SqlWriterConfig.LineFolding.TALL)
        .withWhereFolding(SqlWriterConfig.LineFolding.TALL)
        .withHavingFolding(SqlWriterConfig.LineFolding.TALL)
        .withIndentation(4)
        .withClauseEndsLine(true);
    System.out.println(new SqlPrettyWriter(config).format(node));
  }
}
