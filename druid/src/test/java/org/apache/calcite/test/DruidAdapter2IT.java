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

        .queryContains(new DruidChecker(expectedFilter));
  }

  /**
   * Test to ensure that aggregates with filter clauses that Druid cannot handle are not pushed in
   * as filtered aggregates.
   */
  @Test public void testFilterClauseNotPushable() {
    // Currently the adapter does not support the LIKE operator
    String sql = "select sum(\"store_sales\") "
        + "filter (where \"the_year\" like '199_') from \"foodmart\"";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[LIKE"
            + "($83, '199_')], projects=[[$90, IS TRUE(LIKE($83, '199_'))]], groups=[{}], "
            + "aggs=[[SUM($0)]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"filter\":{\"type"
                + "\":\"expression\",\"expression\":\"like(\\\"the_year\\\","));
  }

  @Test public void testFilterClauseWithMetricRef() {
    String sql = "select sum(\"store_sales\") filter (where \"store_cost\" > 10) from \"foodmart\"";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[>"
            + "($91, 10)], projects=[[$90, IS TRUE(>($91, 10))]], groups=[{}], aggs=[[SUM($0)"
            + "]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"", "\"filter\":{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"10\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=25.060000000000002");
  }

  @Test public void testFilterClauseWithMetricRefAndAggregates() {
    String sql = "select sum(\"store_sales\"), \"product_id\" "
        + "from \"foodmart\" where \"product_id\" > 1553 and \"store_cost\" > 5 group by \"product_id\"";
    String expectedSubExplain =
        "PLAN=EnumerableInterpreter\n"
            + "  BindableProject(EXPR$0=[$1], product_id=[$0])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(>"
            + "(CAST($1):INTEGER, 1553), >($91, 5))], groups=[{1}], aggs=[[SUM($90)]])";

    sql(sql)
        .explainContains(expectedSubExplain)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=10.16; product_id=1554\n"
            + "EXPR$0=45.05; product_id=1556\n"
            + "EXPR$0=88.5; product_id=1555");
  }

  @Test public void testFilterClauseWithMetricAndTimeAndAggregates() {
    String sql = "select sum(\"store_sales\"), \"product_id\""
        + "from \"foodmart\" where \"product_id\" > 1555 and \"store_cost\" > 5 and extract(year "
        + "from \"timestamp\") = 1997 "
        + "group by floor(\"timestamp\" to DAY),\"product_id\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .returnsUnordered("EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=10.6; product_id=1556\n"
            + "EXPR$0=13.25; product_id=1556");
  }

  /**
   * Test to ensure that an aggregate with a nested filter clause has it's filter factored out
   */
  @Test public void testNestedFilterClauseFactored() {
    // Logically equivalent to
    // select sum("store_sales") from "foodmart" where "store_state" in ('CA', 'OR')
    String sql =
        "select sum(\"store_sales\") "
            + "filter (where \"store_state\" = 'CA' or \"store_state\" = 'OR') from \"foodmart\"";

    String expectedFilterJson =
        "filter':{'type':'or','fields':[{'type':'selector','dimension':"
            + "'store_state','value':'CA'},{'type':'selector',"
            + "'dimension':'store_state','value':'OR'}]}";

    String expectedAggregateJson =
        "'aggregations':[{'type':'doubleSum',"
            + "'name':'EXPR$0','fieldName':'store_sales'}]";

    sql(sql)
        .queryContains(new DruidChecker(expectedFilterJson))
        .queryContains(new DruidChecker(expectedAggregateJson))
        .returnsUnordered("EXPR$0=301444.9099999999");
  }

  /**
   * Test to ensure that aggregates with nested filters have their filters factored out
   * into the outer filter for data pruning while still holding a reference to the filter clause
   */
  @Test public void testNestedFilterClauseInAggregates() {
    String sql =
        "select "
            + "sum(\"store_sales\") filter "
            + "(where \"store_state\" = 'CA' and \"the_month\" = 'October'), "
            + "sum(\"store_cost\") filter "
            + "(where \"store_state\" = 'CA' and \"the_day\" = 'Monday') "
            + "from \"foodmart\"";

    // (store_state = CA AND the_month = October) OR (store_state = CA AND the_day = Monday)
    String expectedFilterJson = "filter':{'type':'or','fields':[{'type':'and','fields':[{'type':"
        + "'selector','dimension':'store_state','value':'CA'},{'type':'selector','dimension':"
        + "'the_month','value':'October'}]},{'type':'and','fields':[{'type':'selector',"
        + "'dimension':'store_state','value':'CA'},{'type':'selector','dimension':'the_day',"
        + "'value':'Monday'}]}]}";

    String expectedAggregatesJson = "'aggregations':[{'type':'filtered','filter':{'type':'and',"
        + "'fields':[{'type':'selector','dimension':'store_state','value':'CA'},{'type':"
        + "'selector','dimension':'the_month','value':'October'}]},'aggregator':{'type':"
        + "'doubleSum','name':'EXPR$0','fieldName':'store_sales'}},{'type':'filtered',"
        + "'filter':{'type':'and','fields':[{'type':'selector','dimension':'store_state',"
        + "'value':'CA'},{'type':'selector','dimension':'the_day','value':'Monday'}]},"
        + "'aggregator':{'type':'doubleSum','name':'EXPR$1','fieldName':'store_cost'}}]";

    sql(sql)
        .queryContains(new DruidChecker(expectedFilterJson))
        .queryContains(new DruidChecker(expectedAggregatesJson))
        .returnsUnordered("EXPR$0=13077.789999999992; EXPR$1=9830.7799");
  }

  @Test public void testCountWithNonNull() {
    final String sql = "select count(\"timestamp\") from \"foodmart\"\n";
    final String druidQuery = "{'queryType':'timeseries','dataSource':'foodmart'";
    sql(sql)
        .returnsUnordered("EXPR$0=86829")
        .queryContains(new DruidChecker(druidQuery));
  }

  /**
   * Test to make sure the "not" filter has only 1 field, rather than an array of fields.
   */
  @Test public void testNotFilterForm() {
    String sql = "select count(distinct \"the_month\") from "
        + "\"foodmart\" where \"the_month\" <> \'October\'";
    String druidFilter = "'filter':{'type':'not',"
        + "'field':{'type':'selector','dimension':'the_month','value':'October'}}";
    // Check that the filter actually worked, and that druid was responsible for the filter
    sql(sql, FOODMART)
        .queryContains(new DruidChecker(druidFilter))
        .returnsOrdered("EXPR$0=11");
  }

  /**
   * Test to ensure that count(distinct ...) gets pushed to Druid when approximate results are
   * acceptable
   * */
  @Test public void testDistinctCountWhenApproxResultsAccepted() {
    String sql = "select count(distinct \"store_state\") from \"foodmart\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], projects=[[$63]], groups=[{}], aggs=[[COUNT(DISTINCT $0)]])";
    String expectedAggregate = "{'type':'cardinality','name':"
        + "'EXPR$0','fieldNames':['store_state']}";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, expectedAggregate);
  }

  /**
   * Test to ensure that count(distinct ...) doesn't get pushed to Druid when approximate results
   * are not acceptable
   */
  @Test public void testDistinctCountWhenApproxResultsNotAccepted() {
    String sql = "select count(distinct \"store_state\") from \"foodmart\"";
    String expectedSubExplain = "  BindableAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "groups=[{63}], aggs=[[]])";

    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  @Test
  public void testDistinctCountOnMetric() {
    final String sql = "select count(distinct \"store_sales\") from \"foodmart\" "
        + "where \"store_state\" = 'WA'";
    final String expectedSubExplainNoApprox = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{}], EXPR$0=[COUNT($0)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[=($63, 'WA')"
        + "], groups=[{90}], aggs=[[]])";
    final String expectedSubPlanWithApprox = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[=($63, 'WA')], groups=[{}], "
        + "aggs=[[COUNT(DISTINCT $90)]])";

    testCountWithApproxDistinct(true, sql, expectedSubPlanWithApprox, "'queryType':'timeseries'");
    testCountWithApproxDistinct(false, sql, expectedSubExplainNoApprox, "'queryType':'groupBy'");
  }

  /**
   * Test to ensure that a count on a metric does not get pushed into Druid
   */
  @Test public void testCountOnMetric() {
    String sql = "select \"brand_name\", count(\"store_sales\") from \"foodmart\" "
        + "group by \"brand_name\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], groups=[{2}], aggs=[[COUNT($90)]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
    testCountWithApproxDistinct(false, sql, expectedSubExplain, "\"queryType\":\"groupBy\"");
  }

  /**
   * Test to ensure that count(*) is pushed into Druid
   */
  @Test public void testCountStar() {
    String sql = "select count(*) from \"foodmart\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[0]], groups=[{}], aggs=[[COUNT()]])";

    sql(sql).explainContains(expectedSubExplain);
  }


  @Test public void testCountOnMetricRenamed() {
    String sql = "select \"B\", count(\"A\") from "
        + "(select \"unit_sales\" as \"A\", \"store_state\" as \"B\" from \"foodmart\") "
        + "group by \"B\"";
    String expectedSubExplain = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], groups=[{63}], aggs=[[COUNT($89)]])";

    testCountWithApproxDistinct(true, sql, expectedSubExplain);
    testCountWithApproxDistinct(false, sql, expectedSubExplain);
  }

  @Test
  public void testDistinctCountOnMetricRenamed() {
    final String sql = "select \"B\", count(distinct \"A\") from "
        + "(select \"unit_sales\" as \"A\", \"store_state\" as \"B\" from \"foodmart\") "
        + "group by \"B\"";
    final String expectedSubExplainNoApprox = "PLAN=EnumerableInterpreter\n"
        + "  BindableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $89]], groups=[{0, 1}], aggs=[[]])";
    final String expectedPlanWithApprox = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], groups=[{63}], aggs=[[COUNT(DISTINCT $89)]])";

    testCountWithApproxDistinct(true, sql, expectedPlanWithApprox, "'queryType':'groupBy'");
    testCountWithApproxDistinct(false, sql, expectedSubExplainNoApprox, "'queryType':'groupBy'");
  }

  private void testCountWithApproxDistinct(boolean approx, String sql, String expectedExplain) {
    testCountWithApproxDistinct(approx, sql, expectedExplain, "");
  }

  private void testCountWithApproxDistinct(boolean approx, String sql, String expectedExplain,
                                           String expectedDruidQuery) {
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .with(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.camelName(), approx)
        .query(sql)
        .runs()
        .explainContains(expectedExplain)
        .queryContains(new DruidChecker(expectedDruidQuery));
  }

  /**
   * Test to make sure that if a complex metric is also a dimension, then
   * {@link org.apache.calcite.adapter.druid.DruidTable} should allow it to be used like any other
   * column.
   * */
  @Test public void testComplexMetricAlsoDimension() {
    foodmartApprox("select \"customer_id\" from \"foodmart\"")
        .runs();

    foodmartApprox("select count(distinct \"the_month\"), \"customer_id\" "
        + "from \"foodmart\" group by \"customer_id\"")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart',"
                + "'granularity':'all','dimensions':[{'type':'default','dimension':"
                + "'customer_id','outputName':'customer_id','outputType':'STRING'}],"
                + "'limitSpec':{'type':'default'},'aggregations':[{"
                + "'type':'cardinality','name':'EXPR$0','fieldNames':['the_month']}],"
                + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z']}"));
  }

  /**
   * Test to make sure that the mapping from a Table name to a Table returned from
   * {@link org.apache.calcite.adapter.druid.DruidSchema} is always the same Java object.
   * */
  @Test public void testTableMapReused() {
    AbstractSchema schema = new DruidSchema("http://localhost:8082", "http://localhost:8081", true);
    assertSame(schema.getTable("wikiticker"), schema.getTable("wikiticker"));
  }

  @Test public void testPushEqualsCastDimension() {
    final String sqlQuery = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) = 1016.0";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[=(CAST($1):DOUBLE, 1016.0)], groups=[{}], aggs=[[SUM($91)]])";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'bound','dimension':'product_id','lower':'1016.0',"
            + "'lowerStrict':false,'upper':'1016.0','upperStrict':false,'ordering':'numeric'},"
            + "'aggregations':[{'type':'doubleSum','name':'A','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':true}}";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("A=85.31639999999999");

    final String sqlQuery2 = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) <= 1016.0 "
        + "and cast(\"product_id\" as double) >= 1016.0";
    sql(sqlQuery2, FOODMART)
        .returnsUnordered("A=85.31639999999999");
  }

  @Test public void testPushNotEqualsCastDimension() {
    final String sqlQuery = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) <> 1016.0";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[<>(CAST($1):DOUBLE, 1016.0)], groups=[{}], aggs=[[SUM($91)]])";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'not','field':{'type':'bound','dimension':'product_id','"
            + "lower':'1016.0','lowerStrict':false,'upper':'1016.0','upperStrict':false,'ordering':'numeric'}},"
            + "'aggregations':[{'type':'doubleSum','name':'A','fieldName':'store_cost'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':true}}";
    sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .returnsUnordered("A=225541.91720000014")
        .queryContains(new DruidChecker(druidQuery));

    final String sqlQuery2 = "select sum(\"store_cost\") as a "
        + "from \"foodmart\" "
        + "where cast(\"product_id\" as double) < 1016.0 "
        + "or cast(\"product_id\" as double) > 1016.0";
    sql(sqlQuery2, FOODMART)
        .returnsUnordered("A=225541.91720000014");
  }

  @Test public void testIsNull() {
    final String sql = "select count(*) as c "
        + "from \"foodmart\" "
        + "where \"product_id\" is null";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'selector','dimension':'product_id','value':null},"
            + "'aggregations':[{'type':'count','name':'C'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sql, FOODMART)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("C=0")
        .returnsCount(1);
  }

  @Test public void testIsNotNull() {
    final String sql = "select count(*) as c "
        + "from \"foodmart\" "
        + "where \"product_id\" is not null";
    final String druidQuery =
        "{'queryType':'timeseries','dataSource':'foodmart','descending':false,'granularity':'all',"
            + "'filter':{'type':'not','field':{'type':'selector','dimension':'product_id','value':null}},"
            + "'aggregations':[{'type':'count','name':'C'}],"
            + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],"
            + "'context':{'skipEmptyBuckets':false}}";
    sql(sql, FOODMART)
        .queryContains(new DruidChecker(druidQuery))
        .returnsUnordered("C=86829");
  }

  @Test public void testFilterWithFloorOnTime() {
    // Test filter on floor on time column is pushed to druid
    final String sql =
        "Select floor(\"timestamp\" to MONTH) as t from \"foodmart\" where "
            + "floor(\"timestamp\" to MONTH) between '1997-01-01 00:00:00'"
            + "and '1997-03-01 00:00:00' order by t limit 2";

    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart','intervals':"
        + "['1997-01-01T00:00:00.000Z/1997-04-01T00:00:00.000Z'],'virtualColumns':"
        + "[{'type':'expression','name':'vc','expression':'timestamp_floor(\\'__time\\'";
    sql(sql, FOODMART)
        .returnsOrdered("T=1997-01-01 00:00:00", "T=1997-01-01 00:00:00")
        .queryContains(
            new DruidChecker(druidQuery));
  }

  @Test
  public void testSelectFloorOnTimeWithFilterOnFloorOnTime() {
    final String sql = "Select floor(\"timestamp\" to MONTH) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= '1997-05-01 00:00:00' order by t"
        + " limit 1";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[>=(FLOOR($0, FLAG(MONTH)), 1997-05-01 00:00:00)], "
        + "projects=[[FLOOR($0, FLAG(MONTH))]])";

    sql(sql, FOODMART).returnsOrdered("T=1997-05-01 00:00:00").explainContains(plan);
  }

  @Test public void testTimeWithFilterOnFloorOnTimeAndCastToTimestamp() {
    final String sql = "Select floor(\"timestamp\" to MONTH) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= cast('1997-05-01 00:00:00' as TIMESTAMP) order by t"
        + " limit 1";
    final String druidQuery = "{'queryType':'scan','dataSource':'foodmart','intervals':"
        + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'filter':{'type':'bound',"
        + "'dimension':'__time','lower':'1997-05-01T00:00:00.000Z',"
        + "'lowerStrict':false,'ordering':'lexicographic','";
    sql(sql, FOODMART)
        .returnsOrdered("T=1997-05-01 00:00:00")
        .queryContains(new DruidChecker(druidQuery));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2122">[CALCITE-2122]
   * DateRangeRules issues</a>. */
  @Test public void testCombinationOfValidAndNotValidAndInterval() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" "
        + "WHERE  \"timestamp\" < CAST('1998-01-02' as TIMESTAMP) AND "
        + "EXTRACT(MONTH FROM \"timestamp\") = 01 AND EXTRACT(YEAR FROM \"timestamp\") = 1996 ";
    sql(sql, FOODMART)
        .runs()
        .queryContains(new DruidChecker("{\"queryType\":\"timeseries\""));
  }

  @Test public void testFloorToDateRangeWithTimeZone() {
    final String sql = "Select floor(\"timestamp\" to MONTH) as t from "
        + "\"foodmart\" where floor(\"timestamp\" to MONTH) >= '1997-05-01 00:00:00' "
        + "and floor(\"timestamp\" to MONTH) < '1997-05-02 00:00:00' order by t"
        + " limit 1";
    final String druidQuery = "{\"queryType\":\"scan\",\"dataSource\":\"foodmart\",\"intervals\":"
        + "[\"1997-05-01T00:00:00.000Z/1997-06-01T00:00:00.000Z\"],\"virtualColumns\":[{\"type\":"
        + "\"expression\",\"name\":\"vc\",\"expression\":\"timestamp_floor(\\\"__time\\\"";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql)
        .runs()
        .queryContains(new DruidChecker(druidQuery))
        .returnsOrdered("T=1997-05-01 00:00:00");
  }

  @Test
  public void testExpressionsFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where ABS(-EXP(LN(SQRT"
        + "(\"store_sales\")))) = 1";
    sql(sql, FOODMART)
        .queryContains(new DruidChecker("pow(\\\"store_sales\\\""))
        .returnsUnordered("EXPR$0=32");
  }

  @Test
  public void testExpressionsFilter2() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where CAST(SQRT(ABS(-\"store_sales\"))"
        + " /2 as INTEGER) = 1";
    sql(sql, FOODMART)
        .queryContains(new DruidChecker("(CAST((pow(abs((- \\\"store_sales\\\")),0.5) / 2),"))
        .returnsUnordered("EXPR$0=62449");
  }

  @Test
  public void testExpressionsLikeFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where \"product_id\" LIKE '1%'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"like"))
        .returnsUnordered("EXPR$0=36839");
  }

  @Test
  public void testExpressionsSTRLENFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where CHAR_LENGTH(\"product_id\") = 2";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"expression\":\"(strlen(\\\"product_id\\\") == 2"))
        .returnsUnordered("EXPR$0=4876");
  }

  @Test
  public void testExpressionsUpperLowerFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where upper(lower(\"city\")) = "
        + "'SPOKANE'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(upper"
                + "(lower(\\\"city\\\")) ==", "SPOKANE"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testExpressionsLowerUpperFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where lower(upper(\"city\")) = "
        + "'spokane'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(lower"
                + "(upper(\\\"city\\\")) ==", "spokane"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testExpressionsLowerFilterNotMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where lower(\"city\") = 'Spokane'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(lower"
                + "(\\\"city\\\") ==", "Spokane"))
        .returnsUnordered("EXPR$0=0");
  }

  @Test
  public void testExpressionsLowerFilterMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where lower(\"city\") = 'spokane'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(lower"
                + "(\\\"city\\\") ==", "spokane"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testExpressionsUpperFilterNotMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where upper(\"city\") = 'Spokane'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(upper"
                + "(\\\"city\\\") ==", "Spokane"))
        .returnsUnordered("EXPR$0=0");
  }

  @Test
  public void testExpressionsUpperFilterMatching() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where upper(\"city\") = 'SPOKANE'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\"(upper"
                + "(\\\"city\\\") ==", "SPOKANE"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testExpressionsConcatFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || '_extra') = "
        + "'Spokane_extra'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("{\"type\":\"expression\",\"expression\":\"(concat"
                + "(\\\"city\\\",", "Spokane_extra"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testExpressionsNotNull() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || 'extra') IS NOT NULL";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("{\"type\":\"expression\",\"expression\":\"(concat"
                + "(\\\"city\\\",", "!= null"))
        .returnsUnordered("EXPR$0=86829");
  }

  @Test
  public void testComplexExpressionsIsNull() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where ( cast(null as INTEGER) + cast"
        + "(\"city\" as INTEGER)) IS NULL";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "projects=[[0]], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker(
                "{\"queryType\":\"timeseries\",\"dataSource\":\"foodmart\",\"descending\":false,"
                    + "\"granularity\":\"all\",\"aggregations\":[{\"type\":\"count\","
                    + "\"name\":\"EXPR$0\"}],\"intervals\":[\"1900-01-09T00:00:00.000Z/"
                    + "2992-01-10T00:00:00.000Z\"],\"context\":{\"skipEmptyBuckets\":false}}"))
        .returnsUnordered("EXPR$0=86829");
  }

  @Test
  public void testExpressionsConcatFilterMultipleColumns() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || \"state_province\")"
        + " = 'SpokaneWA'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==", "SpokaneWA"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testAndCombinationOfExpAndSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || \"state_province\")"
        + " = 'SpokaneWA' "
        + "AND \"state_province\" = 'WA'";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==",
                "SpokaneWA",
                "{\"type\":\"selector\",\"dimension\":\"state_province\",\"value\":\"WA\"}]}"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testOrCombinationOfExpAndSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where (\"city\" || \"state_province\")"
        + " = 'SpokaneWA' "
        + "OR (\"state_province\" = 'CA' AND \"city\" IS NOT NULL)";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==",
                "SpokaneWA", "{\"type\":\"and\",\"fields\":[{\"type\":\"selector\","
                    + "\"dimension\":\"state_province\",\"value\":\"CA\"},{\"type\":\"not\","
                    + "\"field\":{\"type\":\"selector\",\"dimension\":\"city\",\"value\":null}}]}"))
        .returnsUnordered("EXPR$0=31835");
  }

  @Test
  public void testColumnAEqColumnB() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where \"city\" = \"state_province\"";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                + "(\\\"city\\\" == \\\"state_province\\\")\"}"))
        .returnsUnordered("EXPR$0=0");
  }

  @Test
  public void testColumnANotEqColumnB() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where \"city\" <> \"state_province\"";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                + "(\\\"city\\\" != \\\"state_province\\\")\"}"))
        .returnsUnordered("EXPR$0=86829");
  }

  @Test
  public void testAndCombinationOfComplexExpAndSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where ((\"city\" || "
        + "\"state_province\") = 'SpokaneWA' OR (\"city\" || '_extra') = 'Spokane_extra') "
        + "AND \"state_province\" = 'WA'";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], filter=[AND(OR(="
            + "(||($29, $30), 'SpokaneWA'), =(||($29, '_extra'), 'Spokane_extra')), =($30, 'WA'))"
            + "], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker("(concat(\\\"city\\\",\\\"state_province\\\") ==",
                "SpokaneWA", "{\"type\":\"selector\",\"dimension\":\"state_province\","
                    + "\"value\":\"WA\"}]}"))
        .returnsUnordered("EXPR$0=7394");
  }

  @Test
  public void testExpressionsFilterWithCast() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" where CAST(( SQRT(\"store_sales\") - 1 "
        + ") / 3 + 1 AS INTEGER) > 1";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("(CAST((((pow(\\\"store_sales\\\",0.5) - 1) / 3) + 1)", "LONG"))
        .returnsUnordered("EXPR$0=476");
  }

  @Test
  public void testExpressionsFilterWithCastTimeToDateToChar() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\" where CAST(CAST(\"timestamp\" as "
        + "DATE) as VARCHAR) = '1997-01-01'";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], "
            + "filter=[=(CAST(CAST($0):DATE NOT NULL):VARCHAR NOT NULL, '1997-01-01')], "
            + "groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker("{\"type\":\"expression\","
                + "\"expression\":\"(timestamp_format(timestamp_floor(\\\"__time\\\""))
        .returnsUnordered("EXPR$0=117");
  }

  @Test
  public void testExpressionsFilterWithExtract() {
    final String sql = "SELECT COUNT(*) FROM \"foodmart\"  where CAST((EXTRACT(MONTH FROM "
        + "\"timestamp\") - 1 ) / 3 + 1 AS INTEGER) = 1";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker(",\"filter\":{\"type\":\"expression\",\"expression\":\"((("
                + "(timestamp_extract(\\\"__time\\\"", "MONTH", ") - 1) / 3) + 1) == 1"))
        .returnsUnordered("EXPR$0=21587");
  }

  @Test
  public void testExtractYearFilterExpression() {
    final String sql = "SELECT count(*) from \"foodmart\" WHERE"
        + " EXTRACT(YEAR from \"timestamp\") + 1 > 1997";
    final String filterPart1 = "'filter':{'type':'expression','expression':"
        + "'((timestamp_extract(\\'__time\\'";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql)
        .runs()
        .returnsOrdered("EXPR$0=86829")
        .queryContains(new DruidChecker(filterPart1));
  }

  @Test
  public void testExtractMonthFilterExpression() {
    final String sql = "SELECT count(*) from \"foodmart\" WHERE"
        + " EXTRACT(MONTH from \"timestamp\") + 1 = 02";
    final String filterPart1 = "'filter':{'type':'expression','expression':"
        + "'((timestamp_extract(\\'__time\\'";
    CalciteAssert.that()
        .enable(enabled())
        .withModel(FOODMART)
        .query(sql)
        .runs()
        .returnsOrdered("EXPR$0=7033")
        .queryContains(new DruidChecker(filterPart1, "MONTH", "== 2"));
  }

  @Test
  public void testTimeFloorExpressions() {

    final String sql =
        "SELECT FLOOR(\"timestamp\" to DAY) as d from \"foodmart\" WHERE "
            + "CAST(FLOOR(CAST(\"timestamp\" AS DATE) to MONTH) AS DATE) = "
            + " CAST('1997-01-01' as DATE) GROUP BY  floor(\"timestamp\" to DAY) order by d limit 3";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[=(FLOOR(CAST($0):DATE NOT NULL, FLAG(MONTH)), "
        + "1997-01-01)], projects=[[FLOOR($0, FLAG(DAY))]], groups=[{0}], aggs=[[]], sort0=[0], "
        + "dir0=[ASC], fetch=[3])";
    sql(sql, FOODMART)
        .explainContains(plan)
        .returnsOrdered("D=1997-01-01 00:00:00", "D=1997-01-02 00:00:00", "D=1997-01-03 00:00:00");
  }

  @Test
  public void testDruidTimeFloorAndTimeParseExpressions() {
    final String sql = "SELECT \"timestamp\", count(*) "
        + "from \"foodmart\" WHERE "
        + "CAST(('1997' || '-01' || '-01') AS DATE) = CAST(\"timestamp\" AS DATE) "
        + "GROUP BY \"timestamp\"";
    sql(sql, FOODMART)
        .returnsOrdered("timestamp=1997-01-01 00:00:00; EXPR$1=117")
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                    + "(timestamp_floor(timestamp_parse(concat(concat(",
                "== timestamp_floor(\\\"__time\\\""));
  }

  @Test
  public void testDruidTimeFloorAndTimeParseExpressions2() {
    final String sql = "SELECT \"timestamp\", count(*) "
        + "from \"foodmart\" WHERE "
        + "CAST(('1997' || '-01' || '-01') AS TIMESTAMP) = CAST(\"timestamp\" AS TIMESTAMP) "
        + "GROUP BY \"timestamp\"";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\",\"expression\":\""
                + "(timestamp_parse(concat(concat("))
        .returnsOrdered("timestamp=1997-01-01 00:00:00; EXPR$1=117");
  }

  @Test
  public void testFilterFloorOnMetricColumn() {
    final String sql = "SELECT count(*) from \"foodmart\" WHERE floor(\"store_sales\") = 23";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]],"
        + " filter=[=(FLOOR($90), 23)], groups=[{}], aggs=[[COUNT()]]";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=2")
        .explainContains(plan)
        .queryContains(new DruidChecker("\"queryType\":\"timeseries\""));
  }


  @Test
  public void testExpressionFilterSimpleColumnAEqColumnB() {
    final String sql = "SELECT count(*) from \"foodmart\" where \"product_id\" = \"city\"";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[=($1, $29)], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(
            new DruidChecker("\"filter\":{\"type\":\"expression\","
                + "\"expression\":\"(\\\"product_id\\\" == \\\"city\\\")\"}"))
        .returnsOrdered("EXPR$0=0");
  }

  @Test
  public void testCastPlusMathOps() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE
        + "WHERE (CAST(\"product_id\" AS INTEGER) + 1 * \"store_sales\")/(\"store_cost\" - 5) "
        + "<= floor(\"store_sales\") * 25 + 2";
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker(
                "\"filter\":{\"type\":\"expression\",\"expression\":\"(((CAST(\\\"product_id\\\", ",
                "LONG",
                ") + (1 * \\\"store_sales\\\")) / (\\\"store_cost\\\" - 5))",
                " <= ((floor(\\\"store_sales\\\") * 25) + 2))\"}"))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[<=(/(+(CAST($1):INTEGER, *(1, $90)), -($91, 5)), +(*(FLOOR($90), 25), 2))], "
            + "groups=[{}], aggs=[[COUNT()]])")
        .returnsOrdered("EXPR$0=82129");
  }

  @Test
  //Boolean cast are not pushed for now.
  public void testBooleanFilterExpressions() {
    final String sql = "SELECT count(*) from " + FOODMART_TABLE
        + " WHERE (CAST((\"product_id\" <> '1') AS BOOLEAN)) IS TRUE";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[<>($1, '1')], groups=[{}], aggs=[[COUNT()]])")
        .queryContains(new DruidChecker("\"queryType\":\"timeseries\""))
        .returnsOrdered("EXPR$0=86803");
  }


  @Test
  public void testCombinationOfValidAndNotValidFilterts() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE
        + "WHERE ((CAST(\"product_id\" AS INTEGER) + 1 * \"store_sales\")/(\"store_cost\" - 5) "
        + "<= floor(\"store_sales\") * 25 + 2) AND \"timestamp\" < CAST('1997-01-02' as TIMESTAMP)"
        + "AND CAST(\"store_sales\" > 0 AS BOOLEAN) IS TRUE "
        + "AND \"product_id\" like '1%' AND \"store_cost\" > 1 "
        + "AND EXTRACT(MONTH FROM \"timestamp\") = 01 AND EXTRACT(DAY FROM \"timestamp\") = 01 "
        + "AND EXTRACT(MONTH FROM \"timestamp\") / 4 + 1 = 1";
    final String queryType = "{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
        + "'dimensions':[{'type':'default','dimension':'store_sales','outputName':'store_sales','outputType':'DOUBLE'}],";
    final String filterExp1 = "{'type':'expression','expression':'(((CAST(\\'product_id\\'";
    final String filterExpPart2 =  " (1 * \\'store_sales\\')) / (\\'store_cost\\' - 5)) "
        + "<= ((floor(\\'store_sales\\') * 25) + 2))'}";
    final String likeExpressionFilter = "{'type':'expression','expression':'like(\\'product_id\\'";
    final String likeExpressionFilter2 = "1%";
    final String simpleBound = "{'type':'bound','dimension':'store_cost','lower':'1',"
        + "'lowerStrict':true,'ordering':'numeric'}";
    final String timeSimpleFilter =
        "{'type':'bound','dimension':'__time','upper':'1997-01-02T00:00:00.000Z',"
            + "'upperStrict':true,'ordering':'lexicographic','extractionFn':{'type':'timeFormat','format':'yyyy-MM-dd";
    final String simpleExtractFilterMonth = "{'type':'bound','dimension':'__time','lower':'1',"
        + "'lowerStrict':false,'upper':'1','upperStrict':false,'ordering':'numeric',"
        + "'extractionFn':{'type':'timeFormat','format':'M','timeZone':'UTC','locale':'en-US'}}";
    final String simpleExtractFilterDay = "{'type':'bound','dimension':'__time','lower':'1',"
        + "'lowerStrict':false,'upper':'1','upperStrict':false,'ordering':'numeric',"
        + "'extractionFn':{'type':'timeFormat','format':'d','timeZone':'UTC','locale':'en-US'}}";
    final String quarterAsExpressionFilter = "{'type':'expression','expression':"
        + "'(((timestamp_extract(\\'__time\\'";
    final String quarterAsExpressionFilter2 = "MONTH";
    final String quarterAsExpressionFilterTimeZone = "UTC";
    final String quarterAsExpressionFilter3 = "/ 4) + 1) == 1)'}]}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], filter=[AND(<=(/(+(CAST($1):INTEGER, *(1, $90)), "
        + "-($91, 5)), +(*(FLOOR($90), 25), 2)), >($90, 0), LIKE($1, '1%'), >($91, 1), "
        + "<($0, 1997-01-02 00:00:00), =(EXTRACT(FLAG(MONTH), $0), 1), "
        + "=(EXTRACT(FLAG(DAY), $0), 1), =(+(/(EXTRACT(FLAG(MONTH), $0), 4), 1), 1))], "
        + "groups=[{}], aggs=[[COUNT()]])";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=36")
        .explainContains(plan)
        .queryContains(
            new DruidChecker(
                queryType, filterExp1, filterExpPart2, likeExpressionFilter, likeExpressionFilter2,
                simpleBound, timeSimpleFilter, simpleExtractFilterMonth, simpleExtractFilterDay,
                quarterAsExpressionFilter, quarterAsExpressionFilterTimeZone,
                quarterAsExpressionFilter2, quarterAsExpressionFilter3));
  }


  @Test
  public void testCeilFilterExpression() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + " WHERE ceil(\"store_sales\") > 1"
        + " AND ceil(\"timestamp\" TO DAY) < CAST('1997-01-05' AS TIMESTAMP)"
        + " AND ceil(\"timestamp\" TO MONTH) < CAST('1997-03-01' AS TIMESTAMP)"
        + " AND ceil(\"timestamp\" TO HOUR) > CAST('1997-01-01' AS TIMESTAMP) "
        + " AND ceil(\"timestamp\" TO MINUTE) > CAST('1997-01-01' AS TIMESTAMP) "
        + " AND ceil(\"timestamp\" TO SECOND) > CAST('1997-01-01' AS TIMESTAMP) ";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1997-01-01T00:00:00.001Z/"
        + "1997-01-04T00:00:00.001Z]], filter=[>(CEIL($90), 1)], groups=[{}], aggs=[[COUNT()]])";
    sql(sql, FOODMART)
        .explainContains(plan)
        .returnsOrdered("EXPR$0=408");
  }

  @Test
  public void testSubStringExpressionFilter() {
    final String sql =
        "SELECT COUNT(*) AS C, SUBSTRING(\"product_id\" from 1 for 4) FROM " + FOODMART_TABLE
            + " WHERE SUBSTRING(\"product_id\" from 1 for 4) like '12%' "
            + " AND CHARACTER_LENGTH(\"product_id\") = 4"
            + " AND SUBSTRING(\"product_id\" from 3 for 1) = '2'"
            + " AND CAST(SUBSTRING(\"product_id\" from 2 for 1) AS INTEGER) = 2"
            + " AND CAST(SUBSTRING(\"product_id\" from 4 for 1) AS INTEGER) = 7"
            + " AND CAST(SUBSTRING(\"product_id\" from 4) AS INTEGER) = 7"
            + " Group by SUBSTRING(\"product_id\" from 1 for 4)";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  BindableProject(C=[$1], EXPR$1=[$0])\n"
        + "    DruidQuery(table=[[foodmart, foodmart]], "
        + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
        + "filter=[AND(LIKE(SUBSTRING($1, 1, 4), '12%'), =(CHAR_LENGTH($1), 4), "
        + "=(SUBSTRING($1, 3, 1), '2'), =(CAST(SUBSTRING($1, 2, 1)):INTEGER, 2), "
        + "=(CAST(SUBSTRING($1, 4, 1)):INTEGER, 7), =(CAST(SUBSTRING($1, 4)):INTEGER, 7))], "
        + "projects=[[SUBSTRING($1, 1, 4)]], groups=[{0}], aggs=[[COUNT()]])";
    sql(sql, FOODMART)
        .returnsOrdered("C=60; EXPR$1=1227")
        .explainContains(plan)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "substring(\\\"product_id\\\"",
                "\"(strlen(\\\"product_id\\\")",
                ",\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\","
                    + "\"expression\":\"substring(\\\"product_id\\\", 0, 4)\","
                    + "\"outputType\":\"STRING\"}]"));
  }

  @Test
  public void testSubStringWithNonConstantIndexes() {
    final String sql = "SELECT COUNT(*) FROM "
        + FOODMART_TABLE
        + " WHERE SUBSTRING(\"product_id\" from CAST(\"store_cost\" as INT)/1000 + 2  "
        + "for CAST(\"product_id\" as INT)) like '1%'";

    sql(sql, FOODMART).returnsOrdered("EXPR$0=10893")
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"", "like(substring(\\\"product_id\\\""))
        .explainContains(
            "PLAN=EnumerableInterpreter\n  DruidQuery(table=[[foodmart, foodmart]], "
                + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
                + "filter=[LIKE(SUBSTRING($1, +(/(CAST($91):INTEGER, 1000), 2), CAST($1):INTEGER), '1%')], "
                + "groups=[{}], aggs=[[COUNT()]])\n\n");
  }

  @Test
  public void testSubStringWithNonConstantIndex() {
    final String sql = "SELECT COUNT(*) FROM "
        + FOODMART_TABLE
        + " WHERE SUBSTRING(\"product_id\" from CAST(\"store_cost\" as INT)/1000 + 1) like '1%'";

    sql(sql, FOODMART).returnsOrdered("EXPR$0=36839")
        .queryContains(new DruidChecker("like(substring(\\\"product_id\\\""))
        .explainContains(
            "PLAN=EnumerableInterpreter\n  DruidQuery(table=[[foodmart, foodmart]], "
                + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
                + "filter=[LIKE(SUBSTRING($1, +(/(CAST($91):INTEGER, 1000), 1)), '1%')],"
                + " groups=[{}], aggs=[[COUNT()]])\n\n");
  }


  /**
   * Test case for https://issues.apache.org/jira/browse/CALCITE-2098.
   * Need to make sure that when there we have a valid filter with no conjunction we still push
   * all the valid filters.
   */
  @Test
  public void testFilterClauseWithNoConjunction() {
    String sql = "select sum(\"store_sales\")"
        + "from \"foodmart\" where \"product_id\" > 1555 or \"store_cost\" > 5 or extract(year "
        + "from \"timestamp\") = 1997 "
        + "group by floor(\"timestamp\" to DAY),\"product_id\"";
    sql(sql)
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", "{\"type\":\"bound\","
                + "\"dimension\":\"store_cost\",\"lower\":\"5\",\"lowerStrict\":true,"
                + "\"ordering\":\"numeric\"}"))
        .runs();
  }

  /**
   * Test case for https://issues.apache.org/jira/browse/CALCITE-2123
   */
  @Test
  public void testBetweenFilterWithCastOverNumeric() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + " WHERE \"product_id\" = 16.0";
    // After CALCITE-2302 the Druid query changed a bit and the type of the
    // filter became an expression (instead of a bound filter) but it still
    // seems correct.
    sql(sql, FOODMART).runs().queryContains(
        new DruidChecker(
            false,
            "\"filter\":{"
                + "\"type\":\"expression\","
                + "\"expression\":\"(CAST(\\\"product_id\\\", \'DOUBLE\') == 16.0)\""
                + "}"));
  }

  @Test
  public void testTrigonometryMathFunctions() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + "WHERE "
        + "SIN(\"store_cost\") > SIN(20) AND COS(\"store_sales\") > COS(20) "
        + "AND FLOOR(TAN(\"store_cost\")) = 2 "
        + "AND ABS(TAN(\"store_cost\") - SIN(\"store_cost\") / COS(\"store_cost\")) < 10e-7";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=2")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[AND(>(SIN($91), SIN(20)), >(COS($90), COS(20)), =(FLOOR(TAN($91)), 2), "
            + "<(ABS(-(TAN($91), /(SIN($91), COS($91)))), 1.0E-6))], "
            + "groups=[{}], aggs=[[COUNT()]])");
  }

  @Test
  public void testCastLiteralToTimestamp() {
    final String sql = "SELECT COUNT(*) FROM "
        + FOODMART_TABLE + " WHERE \"timestamp\" < CAST('1997-01-02' as TIMESTAMP)"
        + " AND EXTRACT(MONTH FROM \"timestamp\") / 4 + 1 = 1 ";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=117")
        .queryContains(
            new DruidChecker("{'queryType':'timeseries','dataSource':'foodmart',"
                    + "'descending':false,'granularity':'all','filter':{'type':'and','fields':"
                    + "[{'type':'bound','dimension':'__time','upper':'1997-01-02T00:00:00.000Z',"
                    + "'upperStrict':true,'ordering':'lexicographic',"
                    + "'extractionFn':{'type':'timeFormat','format':'yyyy-MM-dd",
                "{'type':'expression','expression':'(((timestamp_extract(\\'__time\\',",
                "/ 4) + 1) == 1)'}]},",
                "'aggregations':[{'type':'count','name':'EXPR$0'}],"
                    + "'intervals':['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':false}}"));
  }

  @Test
  public void testNotTrueSimpleFilter() {
    final String sql = "SELECT COUNT(*) FROM " + FOODMART_TABLE + "WHERE "
        + "(\"product_id\" = 1020 ) IS NOT TRUE AND (\"product_id\" = 1020 ) IS FALSE";
    final String result = "EXPR$0=86773";
    sql(sql, FOODMART)
        .returnsOrdered(result)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "filter=[AND(IS NOT TRUE(=(CAST($1):INTEGER, 1020)), <>(CAST($1):INTEGER, 1020))], "
            + "groups=[{}], "
            + "aggs=[[COUNT()]])");
    final String sql2 = "SELECT COUNT(*) FROM " + FOODMART_TABLE + "WHERE "
        + "\"product_id\" <> 1020";
    sql(sql2, FOODMART).returnsOrdered(result);
  }

  // ADDING COMPLEX PROJECT PUSHDOWN

  @Test
  public void testPushOfSimpleMathOps() {
    final String sql =
        "SELECT COS(\"store_sales\") + 1, SIN(\"store_cost\"), EXTRACT(DAY from \"timestamp\") + 1  as D FROM "
            + FOODMART_TABLE + "WHERE \"store_sales\" < 20 order by D limit 3";
    sql(sql, FOODMART)
        .runs()
        .returnsOrdered("EXPR$0=1.060758881219386; EXPR$1=0.5172204046388567; D=2\n"
            + "EXPR$0=0.8316025520509229; EXPR$1=0.6544084288365644; D=2\n"
            + "EXPR$0=0.24267723077545622; EXPR$1=0.9286289016881148; D=2")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$2], dir0=[ASC], fetch=[3])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[<($90, 20)], projects=[[+(COS($90), 1), SIN($91),"
            + " +(EXTRACT(FLAG(DAY), $0), 1)]])");
  }

  @Test
  public void testPushOfSimpleColumnAPlusColumnB() {
    final String sql =
        "SELECT COS(\"store_sales\" + \"store_cost\") + 1, EXTRACT(DAY from \"timestamp\") + 1  as D FROM "
            + FOODMART_TABLE + "WHERE \"store_sales\" < 20 order by D limit 3";
    sql(sql, FOODMART)
        .runs()
        .returnsOrdered("EXPR$0=0.5357357987441458; D=2\n"
            + "EXPR$0=0.22760480207557643; D=2\n"
            + "EXPR$0=0.11259322182897047; D=2")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$1], dir0=[ASC], fetch=[3])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[<($90, 20)], projects=[[+(COS(+($90, $91)), 1), "
            + "+(EXTRACT(FLAG(DAY), $0), 1)]])");
  }

  @Test
  public void testSelectExtractMonth() {
    final String sql = "SELECT  EXTRACT(YEAR FROM \"timestamp\") FROM " + FOODMART_TABLE;
    sql(sql, FOODMART)
        .limit(1)
        .returnsOrdered("EXPR$0=1997")
        .explainContains("DruidQuery(table=[[foodmart, foodmart]], intervals="
            + "[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "projects=[[EXTRACT(FLAG(YEAR), $0)]])")
        .queryContains(
            new DruidChecker("\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\","
                + "\"expression\":\"timestamp_extract(\\\"__time\\\""));
  }

  @Test
  public void testAggOnArithmeticProject() {
    final String sql = "SELECT SUM(\"store_sales\" + 1) FROM " + FOODMART_TABLE;
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=652067.1299999984")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], "
            + "intervals=[[1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z]], "
            + "projects=[[+($90, 1)]], groups=[{}], aggs=[[SUM($0)]])")
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"",
                "\"doubleSum\",\"name\":\"EXPR$0\",\"expression\":\"(\\\"store_sales\\\" + 1)\""));
  }

  @Test
  public void testAggOnArithmeticProject2() {
    final String sql = "SELECT SUM(-\"store_sales\" * 2) as S FROM " + FOODMART_TABLE
        + "Group by \"timestamp\" order by s LIMIT 2";
    sql(sql, FOODMART)
        .returnsOrdered("S=-15918.020000000002\nS=-14115.959999999988")
        .explainContains("BindableProject(S=[$1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$0, *(-($90), 2)]], groups=[{0}], "
            + "aggs=[[SUM($1)]], sort0=[1], dir0=[ASC], fetch=[2])")
        .queryContains(
            new DruidChecker("'queryType':'groupBy'", "'granularity':'all'",
                "{'dimension':'S','direction':'ascending','dimensionOrder':'numeric'}",
                "{'type':'doubleSum','name':'S','expression':'((- \\'store_sales\\') * 2)'}]"));
  }

  @Test
  public void testAggOnArithmeticProject3() {
    final String sql = "SELECT SUM(-\"store_sales\" * 2)-Max(\"store_cost\" * \"store_cost\") AS S,"
        + "Min(\"store_sales\" + \"store_cost\") as S2 FROM " + FOODMART_TABLE
        + "Group by \"timestamp\" order by s LIMIT 2";
    sql(sql, FOODMART)
        .returnsOrdered("S=-16003.314460250002; S2=1.4768000000000001",
            "S=-14181.569999999989; S2=0.8093999999999999")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$0, *(-($90), 2), *($91, $91), +($90, $91)]],"
            + " groups=[{0}], aggs=[[SUM($1), MAX($2), MIN($3)]], post_projects=[[-($1, $2), $3]],"
            + " sort0=[0], dir0=[ASC], fetch=[2])")
        .queryContains(
            new DruidChecker(",\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"$f1\","
                + "\"expression\":\"((- \\\"store_sales\\\") * 2)\"},{\"type\":\"doubleMax\",\"name\""
                + ":\"$f2\",\"expression\":\"(\\\"store_cost\\\" * \\\"store_cost\\\")\"},"
                + "{\"type\":\"doubleMin\",\"name\":\"S2\",\"expression\":\"(\\\"store_sales\\\" "
                + "+ \\\"store_cost\\\")\"}],\"postAggregations\":[{\"type\":\"expression\","
                + "\"name\":\"S\",\"expression\":\"(\\\"$f1\\\" - \\\"$f2\\\")\"}]"));
  }

  @Test
  public void testGroupByVirtualColumn() {
    final String sql =
        "SELECT \"product_id\" || '_' ||\"city\", SUM(\"store_sales\" + "
            + "CAST(\"cost\" AS DOUBLE)) as S FROM " + FOODMART_TABLE
            + "GROUP BY \"product_id\" || '_' || \"city\" LIMIT 2";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=1000_Albany; S=12385.21", "EXPR$0=1000_Altadena; S=8.07")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[||(||($1, '_'), $29), "
            + "+($90, CAST($53):DOUBLE)]], groups=[{0}], aggs=[[SUM($1)]], fetch=[2])")
        .queryContains(
            new DruidChecker("'queryType':'groupBy'",
                "{'type':'doubleSum','name':'S','expression':'(\\'store_sales\\' + CAST(\\'cost\\'",
                "'expression':'concat(concat(\\'product_id\\'",
                "{'type':'default','dimension':'vc','outputName':'vc','outputType':'STRING'}],"
                    + "'virtualColumns':[{'type':'expression','name':'vc"));
  }

  @Test
  public void testCountOverVirtualColumn() {
    final String sql = "SELECT COUNT(\"product_id\" || '_' || \"city\") FROM "
        + FOODMART_TABLE + "WHERE \"state_province\" = 'CA'";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=24441")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[=($30, 'CA')], projects=[[||(||($1, '_'), $29)]],"
            + " groups=[{}], aggs=[[COUNT($0)]])")
        .queryContains(
            new DruidChecker("\"queryType\":\"timeseries\"",
                "\"aggregator\":{\"type\":\"count\",\"name\":\"EXPR$0\",\"expression\":"
                    + "\"concat(concat(\\\"product_id\\\"",
                "\"aggregations\":[{\"type\":\"filtered\",\"filter\":{\"type\":\"not\",\"field\":"
                    + "{\"type\":\"expression\",\"expression\":\"concat(concat(\\\"product_id\\\""));
  }

  @Test
  public void testAggOverStringToLong() {
    final String sql = "SELECT SUM(cast(\"product_id\" AS INTEGER)) FROM " + FOODMART_TABLE;
    sql(sql, FOODMART)
        .queryContains(
            new DruidChecker("{'queryType':'timeseries','dataSource':'foodmart',"
                + "'descending':false,'granularity':'all','aggregations':[{'type':'longSum',"
                + "'name':'EXPR$0','expression':'CAST(\\'product_id\\'", "LONG"))
        .returnsOrdered("EXPR$0=68222919");
  }

  @Test
  public void testAggOnTimeExtractColumn2() {
    final String sql = "SELECT MAX(EXTRACT(MONTH FROM \"timestamp\")) FROM \"foodmart\"";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=12")
        .queryContains(
            new DruidChecker("{'queryType':'timeseries','dataSource':'foodmart',"
                + "'descending':false,'granularity':'all','aggregations':[{"
                + "'type':'longMax','name':'EXPR$0','expression':'timestamp_extract(\\'__time\\'"));
  }

  @Test
  public void testStackedAggregateFilters() {
    final String sql = "SELECT COUNT(\"product_id\") filter (WHERE \"state_province\" = 'CA' "
        + "OR \"store_sales\" > 100 AND \"product_id\" <> '100'), count(*) FROM " + FOODMART_TABLE;
    final String query = "{'queryType':'timeseries','dataSource':'foodmart','descending':false,"
        + "'granularity':'all','aggregations':[{'type':'filtered','filter':{'type':'or','fields':"
        + "[{'type':'selector','dimension':'state_province','value':'CA'},{'type':'and','fields':"
        + "[{'type':'bound','dimension':'store_sales','lower':'100','lowerStrict':true,"
        + "'ordering':'numeric'},{'type':'not','field':{'type':'selector','dimension':'product_id',"
        + "'value':'100'}}]}]},'aggregator':{'type':'filtered','filter':{'type':'not',"
        + "'field':{'type':'selector','dimension':'product_id','value':null}},'aggregator':"
        + "{'type':'count','name':'EXPR$0','fieldName':'product_id'}}},"
        + "{'type':'count','name':'EXPR$1'}],'intervals':['1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z'],'context':{'skipEmptyBuckets':true}}";

    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=24441; EXPR$1=86829")
        .queryContains(new DruidChecker(query));
  }

  @Test
  public void testCastOverPostAggregates() {
    final String sql =
        "SELECT CAST(COUNT(*) + SUM(\"store_sales\") as INTEGER) FROM " + FOODMART_TABLE;
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=652067")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$90]], groups=[{}], "
            + "aggs=[[COUNT(), SUM($0)]], post_projects=[[CAST(+($0, $1)):INTEGER]])");
  }

  @Test
  public void testSubStringOverPostAggregates() {
    final String sql =
        "SELECT \"product_id\", SUBSTRING(\"product_id\" from 1 for 2) FROM " + FOODMART_TABLE
            + " GROUP BY \"product_id\"";
    sql(sql, FOODMART).limit(3).returnsOrdered(
        "product_id=1; EXPR$1=1\nproduct_id=10; EXPR$1=10\nproduct_id=100; EXPR$1=10")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1]], groups=[{0}], aggs=[[]], "
            + "post_projects=[[$0, SUBSTRING($0, 1, 2)]])");
  }

  @Test
  public void testTableQueryExtractYearQuarter() {
    final String sql = "SELECT * FROM (SELECT CAST((MONTH(\"timestamp\") - 1) / 3 + 1 AS BIGINT)"
        + "AS qr_timestamp_ok,  SUM(\"store_sales\") AS sum_store_sales, YEAR(\"timestamp\") AS yr_timestamp_ok"
        + " FROM \"foodmart\" GROUP BY CAST((MONTH(\"timestamp\") - 1) / 3 + 1 AS BIGINT),"
        + " YEAR(\"timestamp\")) LIMIT_ZERO LIMIT 1";

    final String extract_year = "{\"type\":\"extraction\",\"dimension\":\"__time\",\"outputName\":"
        + "\"extract_year\",\"extractionFn\":{\"type\":\"timeFormat\",\"format\":\"yyyy\","
        + "\"timeZone\":\"UTC\",\"locale\":\"en-US\"}}";

    final String extract_expression = "\"expression\":\"(((timestamp_extract(\\\"__time\\\",";
    sql(sql, FOODMART)
        .returnsOrdered(
            "QR_TIMESTAMP_OK=1; SUM_STORE_SALES=139628.34999999971; YR_TIMESTAMP_OK=1997")
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\"", extract_year, extract_expression))
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableProject(QR_TIMESTAMP_OK=[$0], SUM_STORE_SALES=[$2], YR_TIMESTAMP_OK=[$1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[+(/(-(EXTRACT(FLAG(MONTH), $0), 1), 3), 1), "
            + "EXTRACT(FLAG(YEAR), $0), $90]], groups=[{0, 1}], aggs=[[SUM($2)]], fetch=[1])");
  }

  @Test
  public void testTableauQueryExtractMonthDayYear() {
    final String sql = "SELECT * FROM (SELECT (((YEAR(\"foodmart\".\"timestamp\") * 10000) + "
        + "(MONTH(\"foodmart\".\"timestamp\") * 100)) + "
        + "EXTRACT(DAY FROM \"foodmart\".\"timestamp\")) AS md_t_timestamp_ok,\n"
        + "  SUM(\"foodmart\".\"store_sales\") AS sum_t_other_ok\n"
        + "FROM \"foodmart\"\n"
        + "GROUP BY (((YEAR(\"foodmart\".\"timestamp\") * 10000) + (MONTH(\"foodmart\".\"timestamp\")"
        + " * 100)) + EXTRACT(DAY FROM\"foodmart\".\"timestamp\"))) LIMIT 1";
    sql(sql, FOODMART)
        .returnsOrdered("MD_T_TIMESTAMP_OK=19970101; SUM_T_OTHER_OK=706.34")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[+(+(*(EXTRACT(FLAG(YEAR), $0), 10000), "
            + "*(EXTRACT(FLAG(MONTH), $0), 100)), EXTRACT(FLAG(DAY), $0)), $90]], groups=[{0}], "
            + "aggs=[[SUM($1)]], fetch=[1])")
        .queryContains(new DruidChecker("\"queryType\":\"groupBy\""));
  }

  @Test
  public void testTableauQuerySubStringHourMinutes() {
    final String sql = "SELECT * FROM (SELECT CAST(SUBSTRING(CAST(\"foodmart\".\"timestamp\" "
        + " AS VARCHAR) from 12 for 2) AS INT) AS hr_t_timestamp_ok,\n"
        + "  MINUTE(\"foodmart\".\"timestamp\") AS mi_t_timestamp_ok,\n"
        + "  SUM(\"foodmart\".\"store_sales\") AS sum_t_other_ok, EXTRACT(HOUR FROM \"timestamp\") "
        + " AS hr_t_timestamp_ok2 FROM  \"foodmart\" GROUP BY "
        + " CAST(SUBSTRING(CAST(\"foodmart\".\"timestamp\" AS VARCHAR) from 12 for 2 ) AS INT),"
        + "  MINUTE(\"foodmart\".\"timestamp\"), EXTRACT(HOUR FROM \"timestamp\")) LIMIT 1";
    sql(sql, FOODMART)
        .returnsOrdered("HR_T_TIMESTAMP_OK=0; MI_T_TIMESTAMP_OK=0; "
            + "SUM_T_OTHER_OK=565238.1299999986; HR_T_TIMESTAMP_OK2=0")
        .explainContains("BindableProject(HR_T_TIMESTAMP_OK=[$0], MI_T_TIMESTAMP_OK=[$1], "
            + "SUM_T_OTHER_OK=[$3], HR_T_TIMESTAMP_OK2=[$2])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[CAST(SUBSTRING(CAST($0):VARCHAR"
            + " CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" "
            + "NOT NULL, 12, 2)):INTEGER NOT NULL, EXTRACT(FLAG(MINUTE), $0), "
            + "EXTRACT(FLAG(HOUR), $0), $90]], groups=[{0, 1, 2}], aggs=[[SUM($3)]], fetch=[1])")
        .queryContains(new DruidChecker("\"queryType\":\"groupBy\""));
  }

  @Test
  public void testTableauQueryMinutesSecondsExtract() {
    final String sql =  "SELECT * FROM (SELECT SECOND(\"timestamp\") AS sc_t_timestamp_ok,"
        + "MINUTE(\"timestamp\") AS mi_t_timestamp_ok,  SUM(\"store_sales\") AS sum_store_sales "
        + " FROM \"foodmart\" GROUP BY SECOND(\"timestamp\"), MINUTE(\"timestamp\"))"
        + " LIMIT_ZERO LIMIT 1";
    sql(sql, FOODMART)
        .returnsOrdered(
            "SC_T_TIMESTAMP_OK=0; MI_T_TIMESTAMP_OK=0; SUM_STORE_SALES=565238.1299999986")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[EXTRACT(FLAG(SECOND), $0), "
            + "EXTRACT(FLAG(MINUTE), $0), $90]], groups=[{0, 1}], aggs=[[SUM($2)]], fetch=[1])")
        .queryContains(new DruidChecker("\"queryType\":\"groupBy\""));
  }

  @Test
  public void testCastConcatOverPostAggregates() {
    final String sql =
        "SELECT CAST(COUNT(*) + SUM(\"store_sales\") as VARCHAR) || '_' || CAST(SUM(\"store_cost\") "
            + "AS VARCHAR) FROM " + FOODMART_TABLE;
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=652067.1299999986_225627.2336000002")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$90, $91]], groups=[{}], aggs=[[COUNT(), "
            + "SUM($0), SUM($1)]], post_projects=[[||(||(CAST(+($0, $1)):VARCHAR, '_'), "
            + "CAST($2):VARCHAR)]])");
  }

  @Test
  public void testHavingSpecs() {
    final String sql = "SELECT \"product_id\" AS P, SUM(\"store_sales\") AS S FROM \"foodmart\" "
        + " GROUP BY  \"product_id\" HAVING  SUM(\"store_sales\") > 220  ORDER BY P LIMIT 2";
    sql(sql, FOODMART)
        .returnsOrdered("P=1; S=236.55", "P=10; S=230.04")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $90]], groups=[{0}], aggs=[[SUM($1)]], "
            + "filter=[>($1, 220)], sort0=[0], dir0=[ASC], fetch=[2])")
        .queryContains(
            new DruidChecker("'having':{'type':'filter','filter':{'type':'bound',"
                + "'dimension':'S','lower':'220','lowerStrict':true,'ordering':'numeric'}}"));
  }

  @Test
  public void testTransposableHavingFilter() {
    final String sql = "SELECT \"product_id\" AS P, SUM(\"store_sales\") AS S FROM \"foodmart\" "
        + " GROUP BY  \"product_id\" HAVING  SUM(\"store_sales\") > 220 AND \"product_id\" > '10'"
        + "  ORDER BY P LIMIT 2";
    sql(sql, FOODMART)
        .returnsOrdered("P=100; S=343.19999999999993", "P=1000; S=532.62")
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], filter=[>($1, '10')], projects=[[$1, $90]], groups=[{0}],"
            + " aggs=[[SUM($1)]], filter=[>($1, 220)], sort0=[0], dir0=[ASC], fetch=[2])\n")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all'"));
  }

  @Test
  public void testProjectSameColumnMultipleTimes() {
    final String sql =
        "SELECT \"product_id\" as prod_id1, \"product_id\" as prod_id2, "
            + "\"store_sales\" as S1, \"store_sales\" as S2 FROM " + FOODMART_TABLE
            + " order by prod_id1 LIMIT 1";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $1, $90, $90]])")
        .queryContains(
            new DruidChecker("{'queryType':'scan','dataSource':'foodmart','intervals':"
                + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'virtualColumns':["
                + "{'type':'expression','name':'vc','expression':'\\'product_id\\'','outputType':"
                + "'STRING'},{'type':'expression','name':'vc0','expression':'\\'store_sales\\'',"
                + "'outputType':'DOUBLE'}],'columns':['product_id','vc','store_sales','vc0'],"
                + "'resultFormat':'compactedList'}"))
        .returnsOrdered("PROD_ID1=1; PROD_ID2=1; S1=11.4; S2=11.4");
  }

  @Test
  public void testProjectSameMetricsColumnMultipleTimes() {
    final String sql =
        "SELECT \"product_id\" as prod_id1, \"product_id\" as prod_id2, "
            + "\"store_sales\" as S1, \"store_sales\" as S2 FROM " + FOODMART_TABLE
            + " order by prod_id1 LIMIT 1";
    sql(sql, FOODMART)
        .explainContains("PLAN=EnumerableInterpreter\n"
            + "  BindableSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $1, $90, $90]])")
        .queryContains(
            new DruidChecker("{\"queryType\":\"scan\",\"dataSource\":\"foodmart\",\"intervals\":"
                + "[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"],\"virtualColumns\":"
                + "[{\"type\":\"expression\",\"name\":\"vc\",\"expression\":\"\\\"product_id\\\"\","
                + "\"outputType\":\"STRING\"},{\"type\":\"expression\",\"name\":\"vc0\","
                + "\"expression\":\"\\\"store_sales\\\"\",\"outputType\":\"DOUBLE\"}],\"columns\":"
                + "[\"product_id\",\"vc\",\"store_sales\",\"vc0\"],\"resultFormat\":\"compactedList\"}"))
        .returnsOrdered("PROD_ID1=1; PROD_ID2=1; S1=11.4; S2=11.4");
  }

  @Test
  public void testAggSameColumnMultipleTimes() {
    final String sql =
        "SELECT \"product_id\" as prod_id1, \"product_id\" as prod_id2, "
            + "SUM(\"store_sales\") as S1, SUM(\"store_sales\") as S2 FROM " + FOODMART_TABLE
            + " GROUP BY \"product_id\" ORDER BY prod_id2 LIMIT 1";
    sql(sql, FOODMART)
        .explainContains("BindableProject(PROD_ID1=[$0], PROD_ID2=[$0], S1=[$1], S2=[$1])\n"
            + "    DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
            + "2992-01-10T00:00:00.000Z]], projects=[[$1, $90]], groups=[{0}], aggs=[[SUM($1)]], "
            + "sort0=[0], dir0=[ASC], fetch=[1])")
        .queryContains(
            new DruidChecker("\"queryType\":\"groupBy\""))
        .returnsOrdered("PROD_ID1=1; PROD_ID2=1; S1=236.55; S2=236.55");
  }

  @Test
  public void testGroupBy1() {
    final String sql = "SELECT SUM(\"store_sales\") FROM \"foodmart\" "
        + "GROUP BY 1 HAVING (COUNT(1) > 0)";
    sql(sql, FOODMART)
        .returnsOrdered("EXPR$0=565238.1299999986")
        .queryContains(
            new DruidChecker("{'queryType':'groupBy','dataSource':'foodmart','granularity':'all',"
                + "'dimensions':[{'type':'default','dimension':'vc','outputName':'vc','outputType':'LONG'}],"
                + "'virtualColumns':[{'type':'expression','name':'vc','expression':'1','outputType':'LONG'}],"
                + "'limitSpec':{'type':'default'},'aggregations':[{'type':'doubleSum','name':'EXPR$0',"
                + "'fieldName':'store_sales'},{'type':'count','name':'$f2'}],'intervals':"
                + "['1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z'],'having':"
                + "{'type':'filter','filter':{'type':'bound','dimension':'$f2','lower':'0',"
                + "'lowerStrict':true,'ordering':'numeric'}}}"));
  }

  @Test
  public void testFloorQuarter() {
    String sql = "SELECT floor(\"timestamp\" TO quarter), SUM(\"store_sales\") FROM "
        + FOODMART_TABLE
        + " GROUP BY floor(\"timestamp\" TO quarter)";

    sql(sql, FOODMART).queryContains(
        new DruidChecker(
            "{\"queryType\":\"timeseries\",\"dataSource\":\"foodmart\",\"descending\":false,"
                + "\"granularity\":{\"type\":\"period\",\"period\":\"P3M\",\"timeZone\":\"UTC\"},"
                + "\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"EXPR$1\",\"fieldName\":\"store_sales\"}],"
                + "\"intervals\":[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"],\"context\":{\"skipEmptyBuckets\":true}}"));
  }

  @Test
  public void testFloorQuarterPlusDim() {
    String sql =
        "SELECT floor(\"timestamp\" TO quarter),\"product_id\",  SUM(\"store_sales\") FROM "
            + FOODMART_TABLE
            + " GROUP BY floor(\"timestamp\" TO quarter), \"product_id\"";

    sql(sql, FOODMART).queryContains(
        new DruidChecker(
            "{\"queryType\":\"groupBy\",\"dataSource\":\"foodmart\",\"granularity\":\"all\",\"dimensions\":"
                + "[{\"type\":\"extraction\",\"dimension\":\"__time\",\"outputName\":\"floor_quarter\",\"extractionFn\":{\"type\":\"timeFormat\"",
            "\"granularity\":{\"type\":\"period\",\"period\":\"P3M\",\"timeZone\":\"UTC\"},\"timeZone\":\"UTC\",\"locale\":\"und\"}},"
                + "{\"type\":\"default\",\"dimension\":\"product_id\",\"outputName\":\"product_id\",\"outputType\":\"STRING\"}],"
                + "\"limitSpec\":{\"type\":\"default\"},\"aggregations\":[{\"type\":\"doubleSum\",\"name\":\"EXPR$2\",\"fieldName\":\"store_sales\"}],"
                + "\"intervals\":[\"1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z\"]}"));
  }


  @Test
  public void testExtractQuarterPlusDim() {
    String sql =
        "SELECT EXTRACT(quarter from \"timestamp\"),\"product_id\",  SUM(\"store_sales\") FROM "
            + FOODMART_TABLE
            + " WHERE \"product_id\" = 1"
            + " GROUP BY EXTRACT(quarter from \"timestamp\"), \"product_id\"";

    sql(sql, FOODMART).returnsOrdered("EXPR$0=1; product_id=1; EXPR$2=37.050000000000004\n"
        + "EXPR$0=2; product_id=1; EXPR$2=62.7\n"
        + "EXPR$0=3; product_id=1; EXPR$2=88.35\n"
        + "EXPR$0=4; product_id=1; EXPR$2=48.45")
        .queryContains(
            new DruidChecker(
                "{\"queryType\":\"groupBy\",\"dataSource\":\"foodmart\",\"granularity\":\"all\",\"dimensions\":"
                    + "[{\"type\":\"default\",\"dimension\":\"vc\",\"outputName\":\"vc\",\"outputType\":\"LONG\"},"
                    + "{\"type\":\"default\",\"dimension\":\"product_id\",\"outputName\":\"product_id\",\"outputType\":\"STRING\"}],"
                    + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\",\"expression\":\"timestamp_extract(\\\"__time\\\",",
                "QUARTER"));
  }

  @Test
  public void testExtractQuarter() {
    String sql = "SELECT EXTRACT(quarter from \"timestamp\"),  SUM(\"store_sales\") FROM "
        + FOODMART_TABLE
        + " GROUP BY EXTRACT(quarter from \"timestamp\")";

    sql(sql, FOODMART).returnsOrdered("EXPR$0=1; EXPR$1=139628.34999999971\n"
        + "EXPR$0=2; EXPR$1=132666.26999999944\n"
        + "EXPR$0=3; EXPR$1=140271.88999999964\n"
        + "EXPR$0=4; EXPR$1=152671.61999999985")
        .queryContains(
            new DruidChecker(
                "{\"queryType\":\"groupBy\",\"dataSource\":\"foodmart\",\"granularity\":\"all\","
                    + "\"dimensions\":[{\"type\":\"default\",\"dimension\":\"vc\",\"outputName\":\"vc\",\"outputType\":\"LONG\"}],"
                    + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"vc\",\"expression\":\"timestamp_extract(\\\"__time\\\",",
                "QUARTER"));
  }
<<<<<<< HEAD
=======

  @Test
  public void testCastTimestamp1() {
    final String sql = "Select cast(\"timestamp\" as varchar) as t"
        + " from \"foodmart\" order by t limit 1";

    sql(sql, FOODMART)
        .returnsOrdered("T=1997-01-01 00:00:00")
        .queryContains(
            new DruidChecker("UTC"));
  }

  @Test
  public void testCastTimestamp2() {
    final String sql = "Select cast(cast(\"timestamp\" as timestamp) as varchar) as t"
        + " from \"foodmart\" order by t limit 1";

    sql(sql, FOODMART)
        .returnsOrdered("T=1997-01-01 00:00:00")
        .queryContains(
            new DruidChecker("UTC"));
  }
}

// End DruidAdapter2IT.java
>>>>>>> 6f8185b09 (Update stale tests in DruidAdapter)
