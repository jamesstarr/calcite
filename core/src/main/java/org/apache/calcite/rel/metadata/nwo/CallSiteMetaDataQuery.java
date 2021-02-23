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
package org.apache.calcite.rel.metadata.nwo;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * hello.
 */
public class CallSiteMetaDataQuery implements RelMetadataQuery {
  private final MetadataCallSite.NoArg<Multimap<Class<? extends RelNode>, RelNode>> nodeTypes;
  private final MetadataCallSite.NoArg<Double> maxRowCount;
  private final MetadataCallSite.NoArg<Double> minRowCount;
  private final MetadataCallSite.NoArg<RelOptCost> cumulativeCost;
  private final MetadataCallSite.NoArg<RelOptCost> nonCumulativeCost;
  private final MetadataCallSite.NoArg<Double> percentageOriginalRows;
  private final MetadataCallSite.NoArg<Set<RexTableInputRef.RelTableRef>> tableReferences;
  private final MetadataCallSite.NoArg<RelOptTable> tableOrigin;
  private final MetadataCallSite.NoArg<Set<ImmutableBitSet>> uniqueKeys;
  private final MetadataCallSite.NoArg<Double> averageRowSize;
  private final MetadataCallSite.NoArg<List<@Nullable Double>> averageColumnSizes;
  private final MetadataCallSite.NoArg<RelOptPredicateList> allPredicates;
  private final MetadataCallSite.NoArg<RelDistribution> distribution;
  private final MetadataCallSite.NoArg<Boolean> isPhaseTransition;
  private final MetadataCallSite.NoArg<Integer> splitCount;
  private final MetadataCallSite.NoArg<Double> memory;
  private final MetadataCallSite.NoArg<Double> cumulativeMemoryWithinPhase;
  private final MetadataCallSite.NoArg<Double> cumulativeMemoryWithinPhaseSplit;
  private final MetadataCallSite.NoArg<ImmutableList<RelCollation>> collations;
  private final MetadataCallSite.NoArg<Boolean> areRowsUnique;
  private final MetadataCallSite.NoArg<List<@Nullable Double>> averageColumnSizesNotNull;;
  private final MetadataCallSite.NoArg<RelOptPredicateList> pulledUpPredicates;;
  private final MetadataCallSite.NoArg<Double> rowCount;
  private final MetadataCallSite.IntArg<Set<RelColumnOrigin>> columnOrigins;
  private final MetadataCallSite.IntArg<RelColumnOrigin> columnOrigin;
  private final MetadataCallSite.RexNodeArg<Set<RexNode>> expressionLineage;
  private final MetadataCallSite.RexNodeArg<Double> selectivity;
  private final MetadataCallSite.BooleanArg<Set<ImmutableBitSet>> uniqueKeysIgnoreNulls;
  private final MetadataCallSite.ImmutableBitSetBooleanArg<Boolean> columnsUniqueIgnoreNulls;
  private final MetadataCallSite.ImmutableBitSetArg<Double> populationSize;
  private final MetadataCallSite.ImmutableBitSetRexNodeArg<Double> distinctRowCount;
  private final MetadataCallSite.SqlExplainLevelArg<Boolean> isVisibleInExplain;
  private final MetadataCallSite.VolcanoPlannerArg<RelOptCost> lowerBoundCost;

  public CallSiteMetaDataQuery(){
    this(new RegistryMetadataProvider());
  }

  public CallSiteMetaDataQuery(NWOMetadataProvider provider) {
    this.nodeTypes = provider.callSite(MetadataTypes.NodeTypes);
    this.maxRowCount = provider.callSite(MetadataTypes.MaxRowCount);
    this.minRowCount = provider.callSite(MetadataTypes.MinRowCount);
    this.cumulativeCost = provider.callSite(MetadataTypes.CumulativeCost);
    this.nonCumulativeCost = provider.callSite(MetadataTypes.NonCumulativeCost);
    this.percentageOriginalRows = provider.callSite(MetadataTypes.PercentageOriginalRows);
    this.tableReferences = provider.callSite(MetadataTypes.TableReferences);
    this.tableOrigin = provider.callSite(MetadataTypes.TableOrigin);
    this.uniqueKeys = provider.callSite(MetadataTypes.UniqueKeys);
    this.averageRowSize = provider.callSite(MetadataTypes.AverageRowSize);
    this.averageColumnSizes = provider.callSite(MetadataTypes.AverageColumnSizes);
    this.allPredicates = provider.callSite(MetadataTypes.AllPredicates);
    this.distribution = provider.callSite(MetadataTypes.Distribution);
    this.isPhaseTransition = provider.callSite(MetadataTypes.IsPhaseTransition);
    this.splitCount = provider.callSite(MetadataTypes.SplitCount);
    this.memory = provider.callSite(MetadataTypes.Memory);
    this.cumulativeMemoryWithinPhase = provider.callSite(MetadataTypes.CumulativeMemoryWithinPhase);
    this.cumulativeMemoryWithinPhaseSplit = provider.callSite(MetadataTypes.CumulativeMemoryWithinPhaseSplit);
    this.collations = provider.callSite(MetadataTypes.Collations);
    this.areRowsUnique = provider.callSite(MetadataTypes.AreRowsUnique);
    this.averageColumnSizesNotNull = provider.callSite(MetadataTypes.AverageColumnSizesNotNull);;
    this.pulledUpPredicates = provider.callSite(MetadataTypes.PulledUpPredicates);;
    this.rowCount = provider.callSite(MetadataTypes.RowCount);
    this.columnOrigins = provider.callSite(MetadataTypes.ColumnOrigins);
    this.columnOrigin = provider.callSite(MetadataTypes.ColumnOrigin);
    this.expressionLineage = provider.callSite(MetadataTypes.ExpressionLineage);
    this.selectivity = provider.callSite(MetadataTypes.Selectivity);
    this.uniqueKeysIgnoreNulls = provider.callSite(MetadataTypes.UniqueKeysIgnoreNulls);
    this.columnsUniqueIgnoreNulls = provider.callSite(MetadataTypes.ColumnsUniqueIgnoreNulls);
    this.populationSize = provider.callSite(MetadataTypes.PopulationSize);
    this.distinctRowCount = provider.callSite(MetadataTypes.DistinctRowCount);
    this.isVisibleInExplain = provider.callSite(MetadataTypes.IsVisibleInExplain);
    this.lowerBoundCost = provider.callSite(MetadataTypes.LowerBoundCost);
  }

  @Override public @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel) {
    return nodeTypes.extract(new MetadataArguments.NoArg(rel, nodeTypes.metadataType()));
  }

  @Override public Double getRowCount(RelNode rel) {
    return rowCount.extract(new MetadataArguments.NoArg(rel, rowCount.metadataType()));
  }

  @Override public @Nullable Double getMaxRowCount(RelNode rel) {
    return maxRowCount.extract(new MetadataArguments.NoArg(rel, maxRowCount.metadataType()));
  }

  @Override public @Nullable Double getMinRowCount(RelNode rel) {
    return minRowCount.extract(new MetadataArguments.NoArg(rel, minRowCount.metadataType()));
  }

  @Override public @Nullable RelOptCost getCumulativeCost(RelNode rel) {
    return cumulativeCost.extract(new MetadataArguments.NoArg(rel, cumulativeCost.metadataType()));
  }

  @Override public @Nullable RelOptCost getNonCumulativeCost(RelNode rel) {
    return nonCumulativeCost.extract(new MetadataArguments.NoArg(rel, nonCumulativeCost.metadataType()));
  }

  @Override public @Nullable Double getPercentageOriginalRows(RelNode rel) {
    return percentageOriginalRows.extract(new MetadataArguments.NoArg(rel, percentageOriginalRows.metadataType()));
  }

  @Override public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column) {
    return columnOrigins.extract(rel,column);
  }

  @Override public @Nullable RelColumnOrigin getColumnOrigin(RelNode rel, int column) {
    return columnOrigin.extract(rel,column);
  }

  @Override public @Nullable Set<RexNode> getExpressionLineage(RelNode rel, RexNode expression) {
    return expressionLineage.extract(rel,expression);
  }

  @Override public @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(RelNode rel) {
    return tableReferences.extract(new MetadataArguments.NoArg(rel, tableReferences.metadataType()));
  }

  @Override public @Nullable RelOptTable getTableOrigin(RelNode rel) {
    return tableOrigin.extract(new MetadataArguments.NoArg(rel, tableOrigin.metadataType()));
  }

  @Override public @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate) {
    return selectivity.extract(rel, predicate);
  }

  @Override public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    return uniqueKeys.extract(new MetadataArguments.NoArg(rel, uniqueKeys.metadataType()));
  }

  @Override public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel, boolean ignoreNulls) {
    return uniqueKeysIgnoreNulls.extract(rel, ignoreNulls);
  }

  @Override public @Nullable Boolean areRowsUnique(RelNode rel) {
    return areRowsUnique.extract(new MetadataArguments.NoArg(rel, areRowsUnique.metadataType()));
  }

  @Override public @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns) {
    return areColumnsUnique(rel, columns, false);
  }

  @Override public @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns,
      boolean ignoreNulls) {
    return columnsUniqueIgnoreNulls.extract(rel,columns,ignoreNulls);
  }

  @Override public @Nullable ImmutableList<RelCollation> collations(RelNode rel) {
    return collations.extract(new MetadataArguments.NoArg(rel, collations.metadataType()));
  }

  @Override public RelDistribution distribution(RelNode rel) {
    return distribution.extract(new MetadataArguments.NoArg(rel, distribution.metadataType()));
  }

  @Override public @Nullable Double getPopulationSize(RelNode rel, ImmutableBitSet groupKey) {
    return populationSize.extract(rel,groupKey);
  }

  @Override public @Nullable Double getAverageRowSize(RelNode rel) {
    return averageRowSize.extract(new MetadataArguments.NoArg(rel, averageRowSize.metadataType()));
  }

  @Override public @Nullable List<@Nullable Double> getAverageColumnSizes(RelNode rel) {
    return averageColumnSizes.extract(new MetadataArguments.NoArg(rel, averageColumnSizes.metadataType()));
  }

  @Override public List<@Nullable Double> getAverageColumnSizesNotNull(RelNode rel) {
    return averageColumnSizesNotNull.extract(new MetadataArguments.NoArg(rel, averageColumnSizesNotNull.metadataType()));
  }

  @Override public @Nullable Boolean isPhaseTransition(RelNode rel) {
    return isPhaseTransition.extract(new MetadataArguments.NoArg(rel, isPhaseTransition.metadataType()));
  }

  @Override public @Nullable Integer splitCount(RelNode rel) {
    return splitCount.extract(new MetadataArguments.NoArg(rel, splitCount.metadataType()));
  }

  @Override public @Nullable Double memory(RelNode rel) {
    return memory.extract(new MetadataArguments.NoArg(rel, memory.metadataType()));
  }

  @Override public @Nullable Double cumulativeMemoryWithinPhase(RelNode rel) {
    return cumulativeMemoryWithinPhase.extract(new MetadataArguments.NoArg(rel, cumulativeMemoryWithinPhase.metadataType()));
  }

  @Override public @Nullable Double cumulativeMemoryWithinPhaseSplit(RelNode rel) {
    return cumulativeMemoryWithinPhaseSplit.extract(new MetadataArguments.NoArg(rel, cumulativeMemoryWithinPhaseSplit.metadataType()));
  }

  @Override public @Nullable Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey,
      @Nullable RexNode predicate) {
    return distinctRowCount.extract(rel,groupKey,predicate);
  }

  @Override public RelOptPredicateList getPulledUpPredicates(RelNode rel) {
    RelOptPredicateList result = pulledUpPredicates.extract(
        new MetadataArguments.NoArg(rel, pulledUpPredicates.metadataType()));
    return result != null ? result : RelOptPredicateList.EMPTY;

  }

  @Override public @Nullable RelOptPredicateList getAllPredicates(RelNode rel) {
    return allPredicates.extract(new MetadataArguments.NoArg(rel, allPredicates.metadataType()));
  }

  @Override public Boolean isVisibleInExplain(RelNode rel, SqlExplainLevel explainLevel) {
    Boolean b = isVisibleInExplain.extract(rel,explainLevel);
    return b == null || b;
  }

  @Override public @Nullable RelDistribution getDistribution(RelNode rel) {
    return distribution.extract(new MetadataArguments.NoArg(rel, distribution.metadataType()));
  }

  @Override public @Nullable RelOptCost getLowerBoundCost(RelNode rel, VolcanoPlanner planner) {
    return lowerBoundCost.extract(rel, planner);
  }

  @Override public Table<RelNode, List, Object> map() {
    throw new RuntimeException();
  }
}
