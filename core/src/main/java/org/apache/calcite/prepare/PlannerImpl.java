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
package org.apache.calcite.prepare;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
>>>>>>> 0b7d6d6d5 (Revert "[CALCITE-1787] Add ThetaSketch and HyperUnique support to Calcite via rolled up columns (Zain Humayun)")
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;

  @SuppressWarnings("deprecation")
  public final RelNode convert(SqlNode sql) throws RelConversionException {
    return rel(sql).rel;
  }

  public RelRoot rel(SqlNode sql) throws RelConversionException {
    ensure(State.STATE_4_VALIDATED);
    assert validatedSqlNode != null;
    final RexBuilder rexBuilder = createRexBuilder();
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
        .withConfig(sqlToRelConverterConfig)
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .build();
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(new ViewExpanderImpl(), validator,
            createCatalogReader(), cluster, convertletTable, config);
    root =
        sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
    final RelBuilder relBuilder =
        config.getRelBuilderFactory().create(cluster, null);
    root = root.withRel(
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder, false));
    state = State.STATE_5_CONVERTED;
    return root;
  }

  /** Implements {@link org.apache.calcite.plan.RelOptTable.ViewExpander}
   * interface for {@link org.apache.calcite.tools.Planner}. */
  public class ViewExpanderImpl implements ViewExpander {
    @Override public RelRoot expandView(RelDataType rowType, String queryString,
      List<String> schemaPath, List<String> viewPath) {
      SqlParser parser = SqlParser.create(queryString, parserConfig);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("parse failed", e);
      }

      final SqlConformance conformance = conformance();
      final CalciteCatalogReader catalogReader =
          createCatalogReader().withSchemaPath(schemaPath);
      final SqlValidator validator =
          new CalciteSqlValidator(operatorTable, catalogReader, typeFactory,
              conformance);
      validator.setIdentifierExpansion(true);
      final SqlNode validatedSqlNode = validator.validate(sqlNode);

      final RexBuilder rexBuilder = createRexBuilder();
      final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
      final SqlToRelConverter.Config config = SqlToRelConverter
          .configBuilder()
          .withConfig(sqlToRelConverterConfig)
          .withTrimUnusedFields(false)
          .withConvertTableAccess(false)
          .build();
      final SqlToRelConverter sqlToRelConverter =
          new SqlToRelConverter(new ViewExpanderImpl(), validator,
              catalogReader, cluster, convertletTable, config);

      root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false);
      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
      final RelBuilder relBuilder =
          config.getRelBuilderFactory().create(cluster, null);
      root = root.withRel(
          RelDecorrelator.decorrelateQuery(root.rel, relBuilder, false));

      return PlannerImpl.this.root;
    }
  }

  // CalciteCatalogReader is stateless; no need to store one
  private CalciteCatalogReader createCatalogReader() {
    SchemaPlus rootSchema = rootSchema(defaultSchema);
    return new CalciteCatalogReader(
        CalciteSchema.from(rootSchema),
        parserConfig.caseSensitive(),
        CalciteSchema.from(defaultSchema).path(null),
        typeFactory);
  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    for (;;) {
      if (schema.getParentSchema() == null) {
        return schema;
      }
      schema = schema.getParentSchema();
    }
  }

  // RexBuilder is stateless; no need to store one
  private RexBuilder createRexBuilder() {
    return new RexBuilder(typeFactory);
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @SuppressWarnings("deprecation")
  public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits,
      RelNode rel) {
    ensure(State.STATE_5_CONVERTED);
    //Remove this when CachingRelMetadataProvider is removed.
    rel.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(
            rel.getCluster().getMetadataProvider(),
            rel.getCluster().getPlanner()));
    Program program = programs.get(ruleSetIndex);
    return program.run(planner, rel, requiredOutputTraits, ImmutableList.of(),
        ImmutableList.of());
  }

  /** Stage of a statement in the query-preparation lifecycle. */
  private enum State {
    STATE_0_CLOSED {
      @Override void from(PlannerImpl planner) {
        planner.close();
      }
    },
    STATE_1_RESET {
      @Override void from(PlannerImpl planner) {
        planner.ensure(STATE_0_CLOSED);
        planner.reset();
      }
    },
    STATE_2_READY {
      @Override void from(PlannerImpl planner) {
        STATE_1_RESET.from(planner);
        planner.ready();
      }
    },
    STATE_3_PARSED,
    STATE_4_VALIDATED,
    STATE_5_CONVERTED;

    /** Moves planner's state to this state. This must be a higher state. */
    void from(PlannerImpl planner) {
      throw new IllegalArgumentException("cannot move from " + planner.state
          + " to " + this);
    }
  }
}
