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
package org.apache.calcite.rel.metadata.nwo.cleaned;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments;
import org.apache.calcite.rel.metadata.nwo.MetadataCallSite;
import org.apache.calcite.rel.metadata.nwo.MetadataType;
import org.apache.calcite.rel.metadata.nwo.MetadataTypes;

import org.checkerframework.checker.nullness.qual.Nullable;

public class GeneratedNonCumulativeCost {
  public static class getNonCumulativeCost implements MetadataCallSite.NoArg<@Nullable RelOptCost> {
    private final org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows _RelMdPercentageOriginalRows;
    public getNonCumulativeCost(
      org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows _RelMdPercentageOriginalRows) {
      this._RelMdPercentageOriginalRows = _RelMdPercentageOriginalRows;
    }

    @Override public MetadataType<@Nullable RelOptCost, MetadataArguments.NoArg> metadataType() {
      return MetadataTypes.NonCumulativeCost;
    }

    @Override public @Nullable RelOptCost extract(MetadataArguments.NoArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      return _RelMdPercentageOriginalRows.getNonCumulativeCost(relNode, mq);
    }
  }
}
