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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments;
import org.apache.calcite.rel.metadata.nwo.MetadataCallSite;
import org.apache.calcite.rel.metadata.nwo.MetadataType;
import org.apache.calcite.rel.metadata.nwo.MetadataTypes;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

public class GeneratedColumnUniqueness {
  public static class areColumnsUnique implements MetadataCallSite.ImmutableBitSetBooleanArg<@Nullable Boolean>{
    private final org.apache.calcite.rel.metadata.RelMdColumnUniqueness _RelMdColumnUniqueness;
    public areColumnsUnique(
      org.apache.calcite.rel.metadata.RelMdColumnUniqueness _RelMdColumnUniqueness) {
      this._RelMdColumnUniqueness = _RelMdColumnUniqueness;
    }

    @Override public MetadataType< Boolean, MetadataArguments.ImmutableBitSetBooleanArg> metadataType() {
      return MetadataTypes.ColumnsUniqueIgnoreNulls;
    }

    @Override public @Nullable Boolean extract(MetadataArguments.ImmutableBitSetBooleanArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      ImmutableBitSet immutableBitSet = arg.bitset;
      boolean ignoreNull = arg.booleanValue;
      if (relNode instanceof org.apache.calcite.rel.core.Join) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Join)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.plan.hep.HepRelVertex) {
        return mq.areColumnsUnique(((HepRelVertex) relNode).getCurrentRel(), immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Correlate) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Correlate)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Calc) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Calc)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Project) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Project)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Minus) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Minus)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableScan) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.TableScan)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Exchange) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Exchange)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Aggregate) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Aggregate)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Intersect) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Intersect)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.plan.volcano.RelSubset) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.plan.volcano.RelSubset)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Values) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Values)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Filter) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Filter)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.SetOp) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.SetOp)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableModify) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.TableModify)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.convert.Converter) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.convert.Converter)relNode, mq, immutableBitSet, ignoreNull);
      } else if (relNode instanceof org.apache.calcite.rel.core.Sort) {
        return _RelMdColumnUniqueness.areColumnsUnique((org.apache.calcite.rel.core.Sort)relNode, mq, immutableBitSet, ignoreNull);
      } else {
        return _RelMdColumnUniqueness.areColumnsUnique(relNode, mq, immutableBitSet, ignoreNull);
      }
    }
  }

}
