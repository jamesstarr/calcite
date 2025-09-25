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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.JdbcTest.Department;
import org.apache.calcite.test.JdbcTest.Dependent;
import org.apache.calcite.test.JdbcTest.Employee;
import org.apache.calcite.test.JdbcTest.Event;
import org.apache.calcite.test.JdbcTest.Location;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the materialized view rewrite mechanism. Each test has a
 * query and one or more materializations (what Oracle calls materialized views)
 * and checks that the materialization is used.
 */
public class MaterializationTest {
  private static final Consumer<ResultSet> CONTAINS_M0 =
      CalciteAssert.checkResultContains(
          "EnumerableTableScan(table=[[hr, m0]])");

>>>>>>> 6a01f59d3 (Refactor MaterializationTest to use a fluent API (#187))

  /** Fluent class that contains information necessary to run a test. */
  private interface Sql {
    default void ok() {
      that().sameResultWithMaterializationsDisabled();
    }

    default CalciteAssert.AssertQuery that() {
      return checkThatMaterialize_(getMaterialize(), getQuery(), getViewName(),
          isExisting(), getModel(), getChecker(), getRuleSet(),
          isOnlyBySubstitution());
    }

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isExisting();
    Sql withExisting(boolean existing);

    default void noMat() {
      checkNoMaterialize_(getMaterialize(), getQuery(),
          getModel(), isOnlyBySubstitution());
    }

    default Sql withResultContains(String... expected) {
      return withChecker(CalciteAssert.checkResultContains(expected));
    }

    @ImmutableBeans.Property
    String getMaterialize();
    Sql withMaterialize(String materialize);

    @ImmutableBeans.Property
    String getQuery();
    Sql withQuery(String query);

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isOnlyBySubstitution();
    Sql withOnlyBySubstitution(boolean onlyBySubstitution);

    @ImmutableBeans.Property
    String getModel();
    Sql withModel(String model);

    @ImmutableBeans.Property
    Consumer<ResultSet> getChecker();
    Sql withChecker(Consumer<ResultSet> explainChecker);

    @ImmutableBeans.Property
    RuleSet getRuleSet();
    Sql withRuleSet(RuleSet ruleSet);

    @ImmutableBeans.Property
    @ImmutableBeans.StringDefault("m0")
    String getViewName();
    Sql withViewName(String viewName);
  }
<<<<<<< HEAD
=======
=======
>>>>>>> 6a01f59d3 (Refactor MaterializationTest to use a fluent API (#187))
}
