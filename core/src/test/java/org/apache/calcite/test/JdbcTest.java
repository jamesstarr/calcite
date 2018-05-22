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
  @Test public void testRowComparison() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_SCOTT)
        .query("SELECT empno FROM JDBC_SCOTT.emp WHERE (ename, job) < ('Blake', 'Manager')")
        .returnsUnordered("EMPNO=7876", "EMPNO=7499", "EMPNO=7698");
  }

  @Test public void testUnicode() throws Exception {
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
        .throws_("Object 'zoo' not found within 'metaData'");
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
