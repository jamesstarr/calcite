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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments;
import org.apache.calcite.rel.metadata.nwo.MetadataCallSite;
import org.apache.calcite.rel.metadata.nwo.MetadataType;
import org.apache.calcite.rel.metadata.nwo.MetadataTypes;

public class GeneratedPredicates {
  public static class getPredicates implements MetadataCallSite.NoArg<RelOptPredicateList>{
    private final org.apache.calcite.rel.metadata.RelMdPredicates _RelMdPredicates;
    public getPredicates(
      org.apache.calcite.rel.metadata.RelMdPredicates _RelMdPredicates) {
      this._RelMdPredicates = _RelMdPredicates;
    }

    @Override public MetadataType<RelOptPredicateList, MetadataArguments.NoArg> metadataType() {
      return MetadataTypes.PulledUpPredicates;
    }

    @Override public org.apache.calcite.plan.RelOptPredicateList extract(MetadataArguments.NoArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      if (relNode instanceof org.apache.calcite.plan.hep.HepRelVertex) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.plan.hep.HepRelVertex)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Project) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Project)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Exchange) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Exchange)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableScan) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.TableScan)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Minus) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Minus)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Aggregate) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Aggregate)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Intersect) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Intersect)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.plan.volcano.RelSubset) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.plan.volcano.RelSubset)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Filter) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Filter)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableModify) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.TableModify)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Sort) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Sort)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Union) {
        return _RelMdPredicates.getPredicates((org.apache.calcite.rel.core.Union)relNode, mq);
      } else {
        return _RelMdPredicates.getPredicates(relNode,mq);
      }
    }
  }
}
