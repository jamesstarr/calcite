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
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyFilterRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyJoinRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectFilterRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectJoinRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

=======
  @Test public void testAllFoodmartQueries() throws IOException {
=======
  @Test void testAllFoodmartQueries() throws IOException {
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
  @Test void testTileWithNoMeasures() {
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
  @Test void testLatticeWithNoMeasures() {
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

  @Test void testDimensionIsInvalidColumn() {
    foodmartModel(" auto: false,\n"
        + "  tiles: [ {\n"
        + "    dimensions: [ 'invalid_column'],\n"
        + "    measures: [ ]\n"
        + "  } ]\n")
        .connectThrows("Unknown lattice column 'invalid_column'");
  }

  @Test void testMeasureArgIsInvalidColumn() {
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
  @Test void testMeasureArgIsNotUniqueAlias() {
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

  @Test void testMeasureAggIsInvalid() {
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
  @Test void testLatticeWithBadRowCountEstimate() {
    final String lattice =
        INVENTORY_LATTICE.replace("rowCountEstimate: 4070,",
            "rowCountEstimate: 4074070,");
    assertFalse(lattice.equals(INVENTORY_LATTICE));
    modelWithLattices(lattice)
        .query("values 1\n")
        .returns("EXPR$0=1\n");
  }

  @Test void testSuggester() {
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
        + "    JdbcJoin(condition=[=($0, $1)], joinType=[inner])\n"
        + "      JdbcProject(time_id=[$1])\n"
        + "        JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
        + "      JdbcProject(time_id=[$0])\n"
        + "        JdbcTableScan(table=[[foodmart, time_by_day]])\n";
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
