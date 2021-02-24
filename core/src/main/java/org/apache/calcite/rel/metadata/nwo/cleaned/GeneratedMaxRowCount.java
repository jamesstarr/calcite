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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments;
import org.apache.calcite.rel.metadata.nwo.MetadataCallSite;

import org.apache.calcite.rel.metadata.nwo.MetadataType;

import org.apache.calcite.rel.metadata.nwo.MetadataTypes;

import org.checkerframework.checker.nullness.qual.Nullable;

public class GeneratedMaxRowCount {
  public static class getMaxRowCount implements MetadataCallSite.NoArg<@Nullable Double>{
    private final org.apache.calcite.rel.metadata.RelMdMaxRowCount _RelMdMaxRowCount;
    public getMaxRowCount(
      org.apache.calcite.rel.metadata.RelMdMaxRowCount _RelMdMaxRowCount) {
      this._RelMdMaxRowCount = _RelMdMaxRowCount;
    }

    @Override public MetadataType<@Nullable Double, MetadataArguments.NoArg> metadataType() {
      return MetadataTypes.MaxRowCount;
    }

    @Override public @Nullable Double extract(MetadataArguments.NoArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      if(relNode instanceof org.apache.calcite.rel.core.TableModify) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.TableModify)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.enumerable.EnumerableLimit) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.adapter.enumerable.EnumerableLimit)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Minus) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Minus)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableScan) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.TableScan)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Project) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Project)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Union) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Union)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.plan.volcano.RelSubset) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.plan.volcano.RelSubset)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Filter) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Filter)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Values) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Values)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Intersect) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Intersect)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Aggregate) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Aggregate)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Join) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Join)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Sort) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Sort)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Exchange) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Exchange)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Calc) {
        return _RelMdMaxRowCount.getMaxRowCount((org.apache.calcite.rel.core.Calc)relNode, mq);
      } else {
        return _RelMdMaxRowCount.getMaxRowCount(relNode, mq);
      }
    }
  }
}
