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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
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
 * Sentence with a period.
 */
public interface RelMetadataQuery {
  Table<RelNode, List, Object> map();

  @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel);

  /* @Nullable: CALCITE-4263 */ Double getRowCount(RelNode rel);

  @Nullable Double getMaxRowCount(RelNode rel);

  @Nullable Double getMinRowCount(RelNode rel);

  @Nullable RelOptCost getCumulativeCost(RelNode rel);

  @Nullable RelOptCost getNonCumulativeCost(RelNode rel);

  @Nullable Double getPercentageOriginalRows(RelNode rel);

  @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column);

  @Nullable RelColumnOrigin getColumnOrigin(RelNode rel, int column);

  @Nullable Set<RexNode> getExpressionLineage(RelNode rel, RexNode expression);

  @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(RelNode rel);

  @Nullable RelOptTable getTableOrigin(RelNode rel);

  @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate);

  @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel);

  @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel,
      boolean ignoreNulls);

  @Nullable Boolean areRowsUnique(RelNode rel);

  @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns);

  @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns,
      boolean ignoreNulls);

  @Nullable ImmutableList<RelCollation> collations(RelNode rel);

  RelDistribution distribution(RelNode rel);

  @Nullable Double getPopulationSize(RelNode rel,
      ImmutableBitSet groupKey);

  @Nullable Double getAverageRowSize(RelNode rel);

  @Nullable List<@Nullable Double> getAverageColumnSizes(RelNode rel);

  List<@Nullable Double> getAverageColumnSizesNotNull(RelNode rel);

  @Nullable Boolean isPhaseTransition(RelNode rel);

  @Nullable Integer splitCount(RelNode rel);

  @Nullable Double memory(RelNode rel);

  @Nullable Double cumulativeMemoryWithinPhase(RelNode rel);

  @Nullable Double cumulativeMemoryWithinPhaseSplit(RelNode rel);

  @Nullable Double getDistinctRowCount(
      RelNode rel,
      ImmutableBitSet groupKey,
      @Nullable RexNode predicate);

  RelOptPredicateList getPulledUpPredicates(RelNode rel);

  @Nullable RelOptPredicateList getAllPredicates(RelNode rel);

  Boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel);

  @Nullable RelDistribution getDistribution(RelNode rel);

  @Nullable RelOptCost getLowerBoundCost(RelNode rel, VolcanoPlanner planner);
}
