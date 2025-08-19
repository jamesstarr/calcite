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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.Lattices;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

        .enableMaterializations(true)

    // Disable materialization rules from this test. For some reason, there is
    // a weird interaction between these rules and the lattice rewriting that
    // produces non-deterministic rewriting (even when only lattices are present).
    // For more context, see
    // <a href="https://issues.apache.org/jira/browse/CALCITE-2953">[CALCITE-2953]</a>.
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            Arrays.asList(rules).forEach(planner::removeRule))

    // disable for MySQL; times out running star-join query
    // disable for H2; it thinks our generated SQL has invalid syntax
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.MYSQL
            && CalciteAssert.DB != CalciteAssert.DatabaseInstance.H2)
        .explainContains(expectedExplain)
        .returnsUnordered("the_year=1997; quarter=Q1",
            "the_year=1997; quarter=Q2",
            "the_year=1997; quarter=Q3",
            "the_year=1997; quarter=Q4");
  }

  private static CalciteAssert.AssertThat foodmartLatticeModel(
      String statisticProvider) {
    return foodmartModel(" auto: false,\n"
        + "  algorithm: true,\n"
        + "  algorithmMaxMillis: -1,\n"
        + "  rowCountEstimate: 87000,\n"
        + "  defaultMeasures: [ {\n"
        + "      agg: 'sum',\n"
        + "      args: 'unit_sales'\n"
        + "    }, {\n"
        + "      agg: 'sum',\n"
        + "      args: 'store_sales'\n"
        + "    }, {\n"
        + "      agg: 'count'\n"
        + "  } ],\n"
        + "  statisticProvider: '"
        + statisticProvider
        + "',\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ ]\n"
        + "  } ]\n");
  }

  /** Tests a query that is created within {@link #testTileAlgorithm()}. */
  @Test void testJG() {
    final String sql = ""
        + "SELECT \"s\".\"unit_sales\", \"p\".\"recyclable_package\", \"t\".\"the_day\", \"t\".\"the_year\", \"t\".\"quarter\", \"pc\".\"product_family\", COUNT(*) AS \"m0\", SUM(\"s\".\"store_sales\") AS \"m1\", SUM(\"s\".\"unit_sales\") AS \"m2\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\" AS \"s\"\n"
        + "JOIN \"foodmart\".\"product\" AS \"p\" ON \"s\".\"product_id\" = \"p\".\"product_id\"\n"
        + "JOIN \"foodmart\".\"time_by_day\" AS \"t\" ON \"s\".\"time_id\" = \"t\".\"time_id\"\n"
        + "JOIN \"foodmart\".\"product_class\" AS \"pc\" ON \"p\".\"product_class_id\" = \"pc\".\"product_class_id\"\n"
        + "GROUP BY \"s\".\"unit_sales\", \"p\".\"recyclable_package\", \"t\".\"the_day\", \"t\".\"the_year\", \"t\".\"quarter\", \"pc\".\"product_family\"";
    final String explain = "JdbcToEnumerableConverter\n"
        + "  JdbcAggregate(group=[{3, 6, 8, 9, 10, 12}], m0=[COUNT()], m1=[$SUM0($2)], m2=[$SUM0($3)])\n"
        + "    JdbcJoin(condition=[=($4, $11)], joinType=[inner])\n"
        + "      JdbcJoin(condition=[=($1, $7)], joinType=[inner])\n"
        + "        JdbcJoin(condition=[=($0, $5)], joinType=[inner])\n"
        + "          JdbcProject(product_id=[$0], time_id=[$1], store_sales=[$5], unit_sales=[$7])\n"
        + "            JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
        + "          JdbcProject(product_class_id=[$0], product_id=[$1], recyclable_package=[$8])\n"
        + "            JdbcTableScan(table=[[foodmart, product]])\n"
        + "        JdbcProject(time_id=[$0], the_day=[$2], the_year=[$4], quarter=[$8])\n"
        + "          JdbcTableScan(table=[[foodmart, time_by_day]])\n"
        + "      JdbcProject(product_class_id=[$0], product_family=[$4])\n"
        + "        JdbcTableScan(table=[[foodmart, product_class]])";
    CalciteAssert.that().with(CalciteAssert.Config.JDBC_FOODMART)
        .query(sql)
        .explainContains(explain);
  }

  /** Tests a query that uses no columns from the fact table. */
  @Test void testGroupByEmpty() {
    foodmartModel()
        .query("select count(*) as c from \"foodmart\".\"sales_fact_1997\"")
        .enableMaterializations(true)
        .returnsUnordered("C=86837");
  }

  /** Calls {@link #testDistinctCount()} followed by
   * {@link #testGroupByEmpty()}. */
  @Test void testGroupByEmptyWithPrelude() {
    testDistinctCount();
    testGroupByEmpty();
  }

  /** Tests a query that uses no dimension columns and one measure column. */
  @Test void testGroupByEmpty2() {
    foodmartModel()
        .query("select sum(\"unit_sales\") as s\n"
            + "from \"foodmart\".\"sales_fact_1997\"")
        .enableMaterializations(true)
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.ORACLE)
        .returnsUnordered("S=266773.0000");
  }

  /** Tests that two queries of the same dimensionality that use different
   * measures can use the same materialization. */
  @Test public void testGroupByEmpty3() {
    final List<String> mats = new ArrayList<>();
    final CalciteAssert.AssertThat that = foodmartModel().pooled();
    that.query("select sum(\"unit_sales\") as s, count(*) as c\n"
            + "from \"foodmart\".\"sales_fact_1997\"")
        .withHook(Hook.CREATE_MATERIALIZATION, (Consumer<String>) mats::add)
        .enableMaterializations(true)
        .explainContains("EnumerableTableScan(table=[[adhoc, m{}]])")
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.ORACLE)
        .returnsUnordered("S=266773.0000; C=86837");
    assertThat(mats.toString(), mats.size(), equalTo(2));

    // A similar query can use the same materialization.
    that.query("select sum(\"unit_sales\") as s\n"
        + "from \"foodmart\".\"sales_fact_1997\"")
        .withHook(Hook.CREATE_MATERIALIZATION, (Consumer<String>) mats::add)
        .enableMaterializations(true)
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.ORACLE)
        .returnsUnordered("S=266773.0000");
    assertThat(mats.toString(), mats.size(), equalTo(2));
  }

  /** Rolling up SUM. */
  @Test void testSum() {
    foodmartModelWithOneTile()
        .query("select sum(\"unit_sales\") as c\n"
            + "from \"foodmart\".\"sales_fact_1997\"\n"
            + "group by \"product_id\"\n"
            + "order by 1 desc limit 1")
        .enableMaterializations(true)
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.ORACLE)
        .returnsUnordered("C=267.0000");
  }

  /** Tests a distinct-count query.
   *
   * <p>We can't just roll up count(distinct ...) as we do count(...), but we
   * can still use the aggregate table if we're smart. */
  @Test void testDistinctCount() {
    foodmartModelWithOneTile()
        .query("select count(distinct \"quarter\") as c\n"
            + "from \"foodmart\".\"sales_fact_1997\"\n"
            + "join \"foodmart\".\"time_by_day\" using (\"time_id\")\n"
            + "group by \"the_year\"")
        .enableMaterializations(true)
        .explainContains("EnumerableCalc(expr#0..1=[{inputs}], C=[$t1])\n"
            + "  EnumerableAggregate(group=[{0}], C=[COUNT($1)])\n"
            + "    EnumerableTableScan(table=[[adhoc, m{32, 36}]])")
        .returnsUnordered("C=4");
  }

  @Test void testDistinctCount2() {
    foodmartModelWithOneTile()
        .query("select count(distinct \"the_year\") as c\n"
            + "from \"foodmart\".\"sales_fact_1997\"\n"
            + "join \"foodmart\".\"time_by_day\" using (\"time_id\")\n"
            + "group by \"the_year\"")
        .enableMaterializations(true)
        .explainContains("EnumerableCalc(expr#0..1=[{inputs}], C=[$t1])\n"
            + "  EnumerableAggregate(group=[{0}], C=[COUNT($0)])\n"
            + "    EnumerableAggregate(group=[{0}])\n"
            + "      EnumerableTableScan(table=[[adhoc, m{32, 36}]])")
        .returnsUnordered("C=1");
  }

  /** Runs all queries against the Foodmart schema, using a lattice.
   *
   * <p>Disabled for normal runs, because it is slow. */
  @Disabled
<<<<<<< HEAD
<<<<<<< HEAD
  @Test void testAllFoodmartQueries() throws IOException {
=======
=======
  @Test public void testAllFoodmartQueries() throws IOException {
    // Test ids that had bugs in them until recently. Useful for a sanity check.
    final List<Integer> fixed = ImmutableList.of(13, 24, 28, 30, 61, 76, 79, 81,
        85, 98, 101, 107, 128, 129, 130, 131);
    // Test ids that still have bugs
    final List<Integer> bad = ImmutableList.of(382, 423);
    for (int i = 1; i < 1000; i++) {
      System.out.println("i=" + i);
      try {
        if (bad.contains(i)) {
          continue;
        }
        check(i);
      } catch (Throwable e) {
        throw new RuntimeException("error in " + i, e);
      }
    }
  }

  private void check(int n) throws IOException {
    final FoodMartQuerySet set = FoodMartQuerySet.instance();
    final FoodMartQuerySet.FoodmartQuery query = set.queries.get(n);
    if (query == null) {
      return;
    }
    foodmartModelWithOneTile()
        .withDefaultSchema("foodmart")
        .query(query.sql)
      .sameResultWithMaterializationsDisabled();
  }

  /** A tile with no measures should inherit default measure list from the
   * lattice. */
  @Test public void testTileWithNoMeasures() {
    foodmartModel(" auto: false,\n"
        + "  defaultMeasures: [ {\n"
        + "    agg: 'count'\n"
        + "  } ],\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .query("select count(t.\"the_year\", t.\"quarter\")\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n")
        .enableMaterializations(true)
        .explainContains("EnumerableAggregate(group=[{}], EXPR$0=[COUNT($0, $1)])\n"
            + "  EnumerableTableScan(table=[[adhoc, m{32, 36}")
        .returnsCount(1);
  }

  /** A lattice with no default measure list should get "count(*)" is its
   * default measure. */
  @Test public void testLatticeWithNoMeasures() {
    foodmartModel(" auto: false,\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .query("select count(*)\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"foodmart\".\"time_by_day\" as t using (\"time_id\")\n")
        .enableMaterializations(true)
        .explainContains("EnumerableAggregate(group=[{}], EXPR$0=[COUNT()])\n"
            + "  EnumerableTableScan(table=[[adhoc, m{32, 36}")
        .returnsCount(1);
  }

  @Test public void testDimensionIsInvalidColumn() {
    foodmartModel(" auto: false,\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'invalid_column'],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .connectThrows("Unknown lattice column 'invalid_column'");
  }

  @Test public void testMeasureArgIsInvalidColumn() {
    foodmartModel(" auto: false,\n"
        + "  defaultMeasures: [ {\n"
        + "   agg: 'sum',\n"
        + "   args: 'invalid_column'\n"
        + "  } ],\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .connectThrows("Unknown lattice column 'invalid_column'");
  }

  /** It is an error for "time_id" to be a measure arg, because is not a
   * unique alias. Both "s" and "t" have "time_id". */
  @Test public void testMeasureArgIsNotUniqueAlias() {
    foodmartModel(" auto: false,\n"
        + "  defaultMeasures: [ {\n"
        + "    agg: 'count',\n"
        + "    args: 'time_id'\n"
        + "  } ],\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .connectThrows("Lattice column alias 'time_id' is not unique");
  }

  @Test public void testMeasureAggIsInvalid() {
    foodmartModel(" auto: false,\n"
        + "  defaultMeasures: [ {\n"
        + "    agg: 'invalid_count',\n"
        + "    args: 'customer_id'\n"
        + "  } ],\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .connectThrows("Unknown lattice aggregate function invalid_count");
  }

  @Test void testTwoLattices() {
    final AtomicInteger counter = new AtomicInteger();
    // disable for MySQL; times out running star-join query
    // disable for H2; it thinks our generated SQL has invalid syntax
    final boolean enabled =
        CalciteAssert.DB != CalciteAssert.DatabaseInstance.MYSQL
            && CalciteAssert.DB != CalciteAssert.DatabaseInstance.H2;
    modelWithLattices(SALES_LATTICE, INVENTORY_LATTICE)
        .query("select s.\"unit_sales\", p.\"brand_name\"\n"
            + "from \"foodmart\".\"sales_fact_1997\" as s\n"
            + "join \"foodmart\".\"product\" as p using (\"product_id\")\n")
        .enableMaterializations(true)
        .enable(enabled)
        .substitutionMatches(
            CalciteAssert.checkRel(
                "LogicalProject(unit_sales=[$7], brand_name=[$10])\n"
                    + "  LogicalProject(product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], product_class_id=[$8], product_id0=[$9], brand_name=[$10], product_name=[$11], SKU=[$12], SRP=[$13], gross_weight=[$14], net_weight=[$15], recyclable_package=[$16], low_fat=[$17], units_per_case=[$18], cases_per_pallet=[$19], shelf_width=[$20], shelf_height=[$21], shelf_depth=[$22])\n"
                    + "    StarTableScan(table=[[adhoc, star]])\n",
                counter));
    if (enabled) {
      assertThat(counter.intValue(), is(1));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-787">[CALCITE-787]
   * Star table wrongly assigned to materialized view</a>. */
  @Test void testOneLatticeOneMV() {
    final AtomicInteger counter = new AtomicInteger();
    final Class<JdbcTest.EmpDeptTableFactory> clazz =
        JdbcTest.EmpDeptTableFactory.class;

    final String mv = "       materializations: [\n"
        + "         {\n"
        + "           table: \"m0\",\n"
        + "           view: \"m0v\",\n"
        + "           sql: \"select * from \\\"foodmart\\\".\\\"sales_fact_1997\\\" "
        + "where \\\"product_id\\\" = 10\" "
        + "         }\n"
        + "       ]\n";

    final String model = ""
        + "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + JdbcTest.FOODMART_SCHEMA
        + ",\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + clazz.getName()
        + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       lattices: " + "[" + INVENTORY_LATTICE
        + "       ]\n"
        + "     },\n"
        + "     {\n"
        + "       name: 'mat',\n"
        + mv
        + "     }\n"
        + "   ]\n"
        + "}";

    CalciteAssert.model(model)
        .withDefaultSchema("foodmart")
        .query("select * from \"foodmart\".\"sales_fact_1997\" where \"product_id\" = 10")
        .enableMaterializations(true)
        .substitutionMatches(
            CalciteAssert.checkRel(
                "LogicalTableScan(table=[[mat, m0]])\n",
                counter));
    assertThat(counter.intValue(), equalTo(1));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-760">[CALCITE-760]
   * Aggregate recommender blows up if row count estimate is too high</a>. */
  @Disabled
  @Test public void testLatticeWithBadRowCountEstimate() {
    final String lattice =
        INVENTORY_LATTICE.replace("rowCountEstimate: 4070,",
            "rowCountEstimate: 4074070,");
    assertFalse(lattice.equals(INVENTORY_LATTICE));
    modelWithLattices(lattice)
        .query("values 1\n")
        .returns("EXPR$0=1\n");
  }

  @Test public void testSuggester() {
    final Class<JdbcTest.EmpDeptTableFactory> clazz =
        JdbcTest.EmpDeptTableFactory.class;
    final String model = ""
        + "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + JdbcTest.FOODMART_SCHEMA
        + ",\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + clazz.getName() + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       \"autoLattice\": true"
        + "     }\n"
        + "   ]\n"
        + "}";
    final String sql = "select count(*)\n"
        + "from \"sales_fact_1997\"\n"
        + "join \"time_by_day\" using (\"time_id\")\n";
    final String explain = "PLAN=JdbcToEnumerableConverter\n"
        + "  JdbcAggregate(group=[{}], EXPR$0=[COUNT()])\n"
        + "    JdbcJoin(condition=[=($1, $0)], joinType=[inner])\n"
        + "      JdbcProject(time_id=[$0])\n"
        + "        JdbcTableScan(table=[[foodmart, time_by_day]])\n"
        + "      JdbcProject(time_id=[$1])\n"
        + "        JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n";
    CalciteAssert.model(model)
        .withDefaultSchema("foodmart")
        .query(sql)
        .returns("EXPR$0=86837\n")
        .explainContains(explain);
  }

  private static CalciteAssert.AssertThat foodmartModel(String... extras) {
    final String sql = "select 1\n"
        + "from \"foodmart\".\"sales_fact_1997\" as \"s\"\n"
        + "join \"foodmart\".\"product\" as \"p\" using (\"product_id\")\n"
        + "join \"foodmart\".\"time_by_day\" as \"t\" using (\"time_id\")\n"
        + "join \"foodmart\".\"product_class\" as \"pc\"\n"
        + "  on \"p\".\"product_class_id\" = \"pc\".\"product_class_id\"";
    return modelWithLattice("star", sql, extras);
  }

  private CalciteAssert.AssertThat foodmartModelWithOneTile() {
    return foodmartModel(" auto: false,\n"
        + "  defaultMeasures: [ {\n"
        + "    agg: 'count'\n"
        + "  } ],\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'the_year', ['t', 'quarter'] ],\n"
        + "    measures: [ {\n"
        + "      agg: 'sum',\n"
        + "      args: 'unit_sales'\n"
        + "    }, {\n"
        + "      agg: 'sum',\n"
        + "      args: 'store_sales'\n"
        + "    }, {\n"
        + "      agg: 'count'\n"
        + "    } ]\n"
        + "  } ]\n");
  }

  // Just for debugging.
  private static void runJdbc() throws SQLException {
    final Connection connection = DriverManager.getConnection(
        "jdbc:calcite:model=core/src/test/resources/mysql-foodmart-lattice-model.json");
    final ResultSet resultSet = connection.createStatement()
        .executeQuery("select * from \"adhoc\".\"m{32, 36}\"");
    System.out.println(CalciteAssert.toString(resultSet));
    connection.close();
  }

  /** Unit test for {@link Lattice#getRowCount(double, List)}. */
  @Test void testColumnCount() {
    assertThat(Lattice.getRowCount(10, 2, 3), within(5.03D, 0.01D));
    assertThat(Lattice.getRowCount(10, 9, 8), within(9.4D, 0.01D));
    assertThat(Lattice.getRowCount(100, 9, 8), within(54.2D, 0.1D));
    assertThat(Lattice.getRowCount(1000, 9, 8), within(72D, 0.01D));
    assertThat(Lattice.getRowCount(1000, 1, 1), is(1D));
    assertThat(Lattice.getRowCount(1, 3, 5), within(1D, 0.01D));
    assertThat(Lattice.getRowCount(1, 3, 5, 13, 4831), within(1D, 0.01D));
  }
}
