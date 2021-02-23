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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.nwo.MetadataArguments;
import org.apache.calcite.rel.metadata.nwo.MetadataCallSite;
import org.apache.calcite.rel.metadata.nwo.MetadataType;
import org.apache.calcite.rel.metadata.nwo.MetadataTypes;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

public class GeneratedCollation {
  public static class Collations implements MetadataCallSite.NoArg<ImmutableList<RelCollation>> {
    private final org.apache.calcite.rel.metadata.RelMdCollation _RelMdCollation;
    public Collations(
      org.apache.calcite.rel.metadata.RelMdCollation _RelMdCollation) {
      this._RelMdCollation = _RelMdCollation;
    }

    @Override public MetadataType<ImmutableList<RelCollation>, MetadataArguments.NoArg> metadataType() {
      return MetadataTypes.Collations;
    }

    @Override public @Nullable ImmutableList<RelCollation> extract(MetadataArguments.NoArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      if(relNode instanceof org.apache.calcite.rel.core.SortExchange) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.SortExchange)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter) {
        return _RelMdCollation.collations((org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.enumerable.EnumerableCorrelate) {
        return _RelMdCollation.collations((org.apache.calcite.adapter.enumerable.EnumerableCorrelate)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.plan.hep.HepRelVertex) {
        return _RelMdCollation.collations((org.apache.calcite.plan.hep.HepRelVertex)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.enumerable.EnumerableHashJoin) {
        return _RelMdCollation.collations((org.apache.calcite.adapter.enumerable.EnumerableHashJoin)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Calc) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Calc)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Project) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Project)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableModify) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.TableModify)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Match) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Match)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableScan) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.TableScan)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin) {
        return _RelMdCollation.collations((org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.enumerable.EnumerableMergeJoin) {
        return _RelMdCollation.collations((org.apache.calcite.adapter.enumerable.EnumerableMergeJoin)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.plan.volcano.RelSubset) {
        return _RelMdCollation.collations((org.apache.calcite.plan.volcano.RelSubset)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Values) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Values)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Filter) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Filter)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.adapter.enumerable.EnumerableMergeUnion) {
        return _RelMdCollation.collations((org.apache.calcite.adapter.enumerable.EnumerableMergeUnion)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Window) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Window)relNode, mq);
      } else if (relNode instanceof org.apache.calcite.rel.core.Sort) {
        return _RelMdCollation.collations((org.apache.calcite.rel.core.Sort)relNode, mq);
      } else {
        return _RelMdCollation.collations(relNode, mq);
      }
    }
  }

}
