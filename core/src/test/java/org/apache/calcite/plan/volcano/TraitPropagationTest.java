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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBitSet;

              convertedInput, false, rel.getGroupSet(),
              rel.getGroupSets(), rel.getAggCallList()));
    }
  }

  /** Rule for PhysProj */
  private static class PhysProjRule extends RelOptRule {
    static final PhysProjRule INSTANCE = new PhysProjRule(false);

    final boolean subsetHack;

    private PhysProjRule(boolean subsetHack) {
      super(
          RelOptRule.operand(LogicalProject.class,
              anyChild(RelNode.class)),
          "PhysProj");
      this.subsetHack = subsetHack;
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalProject rel = call.rel(0);
      RelNode rawInput = call.rel(1);
      RelNode input = convert(rawInput, PHYSICAL);

      if (subsetHack && input instanceof RelSubset) {
        RelSubset subset = (RelSubset) input;
        for (RelNode child : subset.getRels()) {
          // skip logical nodes
          if (child.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
              == Convention.NONE) {
            continue;
          } else {
            RelTraitSet outcome = child.getTraitSet().replace(PHYSICAL);
            call.transformTo(
                new PhysProj(rel.getCluster(), outcome, convert(child, outcome),
                    rel.getProjects(), rel.getRowType()));
          }
        }
      } else {
        call.transformTo(
            PhysProj.create(input, rel.getProjects(), rel.getRowType()));
      }

    }
  }

  /** Rule for PhysSort */
  private static class PhysSortRule extends ConverterRule {
    static final PhysSortRule INSTANCE = new PhysSortRule();

    PhysSortRule() {
      super(Sort.class, Convention.NONE, PHYSICAL, "PhysSortRule");
    }

    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelNode input = convert(sort.getInput(),
          rel.getCluster().traitSetOf(PHYSICAL));
      return new PhysSort(
          rel.getCluster(),
          input.getTraitSet().plus(sort.getCollation()),
          convert(input, input.getTraitSet().replace(PHYSICAL)),
          sort.getCollation(),
          null,
          null);
    }
  }

  /** Rule for PhysTable */
  private static class PhysTableRule extends RelOptRule {
    static final PhysTableRule INSTANCE = new PhysTableRule();

    private PhysTableRule() {
      super(anyChild(LogicalTableScan.class), "PhysScan");
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalTableScan rel = call.rel(0);
      call.transformTo(new PhysTable(rel.getCluster()));
    }
  }

  /* RELS */
  /** Market interface for Phys nodes */
  private interface Phys extends RelNode { }

  /** Physical Aggregate RelNode */
  private static class PhysAgg extends Aggregate implements Phys {
    PhysAgg(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      super(cluster, traits, child, groupSet, groupSets, aggCalls);
      Preconditions.checkArgument(!indicator,
          "indicator is not supported, use GROUPING function instead");
    }

    public Aggregate copy(RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      return new PhysAgg(getCluster(), traitSet, input, groupSet,
          groupSets, aggCalls);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /** Physical Project RelNode */
  private static class PhysProj extends Project implements Phys {
    PhysProj(RelOptCluster cluster, RelTraitSet traits, RelNode child,
        List<RexNode> exps, RelDataType rowType) {
      super(cluster, traits, ImmutableList.of(), child, exps, rowType);
    }

    public static PhysProj create(final RelNode input,
        final List<RexNode> projects, RelDataType rowType) {
      final RelOptCluster cluster = input.getCluster();
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      final RelTraitSet traitSet =
          cluster.traitSet().replace(PHYSICAL)
              .replaceIfs(
                  RelCollationTraitDef.INSTANCE,
                  () -> RelMdCollation.project(mq, input, projects));
      return new PhysProj(cluster, traitSet, input, projects, rowType);
    }

    public PhysProj copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> exps, RelDataType rowType) {
      return new PhysProj(getCluster(), traitSet, input, exps, rowType);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /** Physical Sort RelNode */
  private static class PhysSort extends Sort implements Phys {
    PhysSort(RelOptCluster cluster, RelTraitSet traits, RelNode child,
        RelCollation collation, RexNode offset,
        RexNode fetch) {
      super(cluster, traits, child, collation, offset, fetch);

    }

    public PhysSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, RexNode offset,
        RexNode fetch) {
      return new PhysSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /** Physical Table RelNode */
  private static class PhysTable extends AbstractRelNode implements Phys {
    PhysTable(RelOptCluster cluster) {
      super(cluster, cluster.traitSet().replace(PHYSICAL).replace(COLLATION));
      RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      final RelDataType stringType = typeFactory.createJavaType(String.class);
      final RelDataType integerType = typeFactory.createJavaType(Integer.class);
      this.rowType = typeFactory.builder().add("s", stringType)
          .add("i", integerType).build();
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /* UTILS */
  public static RelOptRuleOperand anyChild(Class<? extends RelNode> first) {
    return RelOptRule.operand(first, RelOptRule.any());
  }

  // Created so that we can control when the TraitDefs are defined (e.g.
  // before the cluster is created).
  private static RelNode run(PropAction action, RuleSet rules)
      throws Exception {

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .ruleSets(rules).build();

    final Properties info = new Properties();
    final Connection connection = DriverManager
        .getConnection("jdbc:calcite:", info);
    final CalciteServerStatement statement = connection
        .createStatement().unwrap(CalciteServerStatement.class);
    final CalcitePrepare.Context prepareContext =
          statement.createPrepareContext();
    final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
    CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(prepareContext.getRootSchema(),
              prepareContext.getDefaultSchemaPath(),
              typeFactory,
              prepareContext.config());
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptPlanner planner = new VolcanoPlanner(config.getCostFactory(),
        config.getContext());

    // set up rules before we generate cluster
    planner.clearRelTraitDefs();
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.clear();
    for (RelOptRule r : rules) {
      planner.addRule(r);
    }

    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return action.apply(cluster, catalogReader,
        prepareContext.getRootSchema().plus());
  }
}
