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
package org.apache.calcite.tools;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
      statisticProvider = MapSqlStatisticProvider.INSTANCE;
    }

    /** Creates a ConfigBuilder, initializing from an existing config. */
    private ConfigBuilder(FrameworkConfig config) {
      convertletTable = config.getConvertletTable();
      operatorTable = config.getOperatorTable();
      programs = config.getPrograms();
      context = config.getContext();
      traitDefs = config.getTraitDefs();
      parserConfig = config.getParserConfig();
      sqlValidatorConfig = config.getSqlValidatorConfig();
      sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
      defaultSchema = config.getDefaultSchema();
      executor = config.getExecutor();
      costFactory = config.getCostFactory();
      typeSystem = config.getTypeSystem();
      evolveLattice = config.isEvolveLattice();
      statisticProvider = config.getStatisticProvider();
    }

    public FrameworkConfig build() {
      return new StdFrameworkConfig(context, convertletTable, operatorTable,
          programs, traitDefs, parserConfig, sqlValidatorConfig, sqlToRelConverterConfig,
          defaultSchema, costFactory, typeSystem, executor, evolveLattice,
          statisticProvider);
    }

    public ConfigBuilder context(Context c) {
      this.context = Objects.requireNonNull(c);
      return this;
    }

    public ConfigBuilder executor(RexExecutor executor) {
      this.executor = Objects.requireNonNull(executor);
      return this;
    }

    public ConfigBuilder convertletTable(
        SqlRexConvertletTable convertletTable) {
      this.convertletTable = Objects.requireNonNull(convertletTable);
      return this;
    }

    public ConfigBuilder operatorTable(SqlOperatorTable operatorTable) {
      this.operatorTable = Objects.requireNonNull(operatorTable);
      return this;
    }

    public ConfigBuilder traitDefs(List<RelTraitDef> traitDefs) {
      if (traitDefs == null) {
        this.traitDefs = null;
      } else {
        this.traitDefs = ImmutableList.copyOf(traitDefs);
      }
      return this;
    }

    public ConfigBuilder traitDefs(RelTraitDef... traitDefs) {
      this.traitDefs = ImmutableList.copyOf(traitDefs);
      return this;
    }

    public ConfigBuilder parserConfig(SqlParser.Config parserConfig) {
      this.parserConfig = Objects.requireNonNull(parserConfig);
      return this;
    }

    public ConfigBuilder sqlValidatorConfig(SqlValidator.Config sqlValidatorConfig) {
      this.sqlValidatorConfig = Objects.requireNonNull(sqlValidatorConfig, "sqlValidatorConfig");
      return this;
    }

    public ConfigBuilder sqlToRelConverterConfig(
        SqlToRelConverter.Config sqlToRelConverterConfig) {
      this.sqlToRelConverterConfig =
          Objects.requireNonNull(sqlToRelConverterConfig);
      return this;
    }

    public ConfigBuilder defaultSchema(SchemaPlus defaultSchema) {
      this.defaultSchema = defaultSchema;
      return this;
    }

    public ConfigBuilder costFactory(RelOptCostFactory costFactory) {
      this.costFactory = costFactory;
      return this;
    }

    public ConfigBuilder ruleSets(RuleSet... ruleSets) {
      return programs(Programs.listOf(ruleSets));
    }

    public ConfigBuilder ruleSets(List<RuleSet> ruleSets) {
      return programs(Programs.listOf(Objects.requireNonNull(ruleSets)));
    }

    public ConfigBuilder programs(List<Program> programs) {
      this.programs = ImmutableList.copyOf(programs);
      return this;
    }

    public ConfigBuilder programs(Program... programs) {
      this.programs = ImmutableList.copyOf(programs);
      return this;
    }

    public ConfigBuilder typeSystem(RelDataTypeSystem typeSystem) {
      this.typeSystem = Objects.requireNonNull(typeSystem);
      return this;
    }

    public ConfigBuilder evolveLattice(boolean evolveLattice) {
      this.evolveLattice = evolveLattice;
      return this;
    }

    public ConfigBuilder statisticProvider(
        SqlStatisticProvider statisticProvider) {
      this.statisticProvider = Objects.requireNonNull(statisticProvider);
      return this;
    }
  }

  /**
   * An implementation of {@link FrameworkConfig} that uses standard Calcite
   * classes to provide basic planner functionality.
   */
  static class StdFrameworkConfig implements FrameworkConfig {
    private final Context context;
    private final SqlRexConvertletTable convertletTable;
    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final SqlParser.Config parserConfig;
    private final SqlValidator.Config sqlValidatorConfig;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SchemaPlus defaultSchema;
    private final RelOptCostFactory costFactory;
    private final RelDataTypeSystem typeSystem;
    private final RexExecutor executor;
    private final boolean evolveLattice;
    private final SqlStatisticProvider statisticProvider;
<<<<<<< HEAD
    private final RelOptTable.ViewExpander viewExpander;
=======

    StdFrameworkConfig(Context context,
        SqlRexConvertletTable convertletTable,
        SqlOperatorTable operatorTable,
        ImmutableList<Program> programs,
        ImmutableList<RelTraitDef> traitDefs,
        SqlParser.Config parserConfig,
        SqlValidator.Config sqlValidatorConfig,
        SqlToRelConverter.Config sqlToRelConverterConfig,
        SchemaPlus defaultSchema,
        RelOptCostFactory costFactory,
        RelDataTypeSystem typeSystem,
        RexExecutor executor,
        boolean evolveLattice,
        SqlStatisticProvider statisticProvider) {
      this.context = context;
      this.convertletTable = convertletTable;
      this.operatorTable = operatorTable;
      this.programs = programs;
      this.traitDefs = traitDefs;
      this.parserConfig = parserConfig;
      this.sqlValidatorConfig = sqlValidatorConfig;
      this.sqlToRelConverterConfig = sqlToRelConverterConfig;
      this.defaultSchema = defaultSchema;
      this.costFactory = costFactory;
      this.typeSystem = typeSystem;
      this.executor = executor;
      this.evolveLattice = evolveLattice;
      this.statisticProvider = statisticProvider;
>>>>>>> e891d5bff ([CALCITE-1870] Lattice suggester)
    }

    public SqlParser.Config getParserConfig() {
      return parserConfig;
    }

    public SqlValidator.Config getSqlValidatorConfig() {
      return sqlValidatorConfig;
    }

    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
      return sqlToRelConverterConfig;
    }

    public SchemaPlus getDefaultSchema() {
      return defaultSchema;
    }

    public RexExecutor getExecutor() {
      return executor;
    }

    public ImmutableList<Program> getPrograms() {
      return programs;
    }

    public RelOptCostFactory getCostFactory() {
      return costFactory;
    }

    public ImmutableList<RelTraitDef> getTraitDefs() {
      return traitDefs;
    }

    public SqlRexConvertletTable getConvertletTable() {
      return convertletTable;
    }

    public Context getContext() {
      return context;
    }

    public SqlOperatorTable getOperatorTable() {
      return operatorTable;
    }

    public RelDataTypeSystem getTypeSystem() {
      return typeSystem;
    }

    public boolean isEvolveLattice() {
      return evolveLattice;
    }

    public SqlStatisticProvider getStatisticProvider() {
      return statisticProvider;
    }
<<<<<<< HEAD

    public RelOptTable.ViewExpander getViewExpander() {
      return viewExpander;
    }
=======
  }
}
