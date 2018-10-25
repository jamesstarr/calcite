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
=======
    private final RelOptTable.ViewExpander viewExpander;

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
        SqlStatisticProvider statisticProvider,
        RelOptTable.ViewExpander viewExpander) {
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
      this.viewExpander = viewExpander;
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
>>>>>>> ca65868f2 ([CALCITE-2441] RelBuilder.scan should expand TranslatableTable and views)

    public RelOptTable.ViewExpander getViewExpander() {
      return viewExpander;
    }
<<<<<<< HEAD
=======
=======
  }
}
