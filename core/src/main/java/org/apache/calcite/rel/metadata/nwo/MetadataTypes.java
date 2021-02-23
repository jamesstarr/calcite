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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments.BooleanArg;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments.ImmutableBitSetArg;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments.IntArg;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments.NoArg;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments.RexNodeArg;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

import static org.apache.calcite.rel.metadata.nwo.MetadataArguments.*;

public interface MetadataTypes {
  MetadataType<@Nullable Double, NoArg> RowCount = new MetadataType<>();
  MetadataType<@Nullable Multimap<Class<? extends RelNode>, RelNode>, NoArg> NodeTypes = new MetadataType<>();
  MetadataType<Double, NoArg> MaxRowCount = new MetadataType<>();
  MetadataType<Double, NoArg> MinRowCount = new MetadataType<>();
  MetadataType<RelOptCost, NoArg> CumulativeCost = new MetadataType<>();
  MetadataType<RelOptCost, NoArg> NonCumulativeCost = new MetadataType<>();
  MetadataType<Double, NoArg> PercentageOriginalRows = new MetadataType<>();
  MetadataType<Set<RexTableInputRef.RelTableRef>, NoArg> TableReferences = new MetadataType<>();
  MetadataType<RelOptTable, NoArg> TableOrigin = new MetadataType<>();
  MetadataType<Set<ImmutableBitSet>, NoArg> UniqueKeys = new MetadataType<>();
  MetadataType<Double, NoArg> AverageRowSize = new MetadataType<>();
  MetadataType<List<@Nullable Double>, NoArg> AverageColumnSizes = new MetadataType<>();
  MetadataType<RelOptPredicateList, NoArg> AllPredicates = new MetadataType<>();
  MetadataType<RelDistribution, NoArg> Distribution = new MetadataType<>();
  MetadataType<Boolean, NoArg> IsPhaseTransition = new MetadataType<>();
  MetadataType<Integer, NoArg> SplitCount = new MetadataType<>();
  MetadataType<Double, NoArg> Memory = new MetadataType<>();
  MetadataType<Double, NoArg> CumulativeMemoryWithinPhase = new MetadataType<>();
  MetadataType<Double, NoArg> CumulativeMemoryWithinPhaseSplit = new MetadataType<>();
  MetadataType<ImmutableList<RelCollation>, NoArg> Collations = new MetadataType<>();
  MetadataType<Boolean, NoArg> AreRowsUnique = new MetadataType<>();
  MetadataType<List<@Nullable Double>, NoArg> AverageColumnSizesNotNull = new MetadataType<>();;
  MetadataType<RelOptPredicateList, NoArg> PulledUpPredicates = new MetadataType<>();;
  MetadataType<Set<RelColumnOrigin>, IntArg> ColumnOrigins = new MetadataType<>();
  MetadataType<RelColumnOrigin, IntArg> ColumnOrigin = new MetadataType<>();
  MetadataType<Set<RexNode>, RexNodeArg> ExpressionLineage = new MetadataType<>();
  MetadataType<@Nullable Double, RexNodeArg> Selectivity = new MetadataType<>();
  MetadataType<Set<ImmutableBitSet>, BooleanArg> UniqueKeysIgnoreNulls = new MetadataType<>();
  MetadataType<Boolean, ImmutableBitSetArg> AreColumnsUnique = new MetadataType<>();
  MetadataType<@Nullable Boolean, ImmutableBitSetBooleanArg> ColumnsUniqueIgnoreNulls = new MetadataType<>();
  MetadataType<Double, ImmutableBitSetArg> PopulationSize = new MetadataType<>();
  MetadataType<@Nullable Double, ImmutableBitSetRexNodeArg> DistinctRowCount = new MetadataType<>();
  MetadataType<Boolean, SqlExplainLevelArg> IsVisibleInExplain = new MetadataType<>();
  MetadataType<RelOptCost, VolcanoPlannerArg> LowerBoundCost = new MetadataType<>();
}
