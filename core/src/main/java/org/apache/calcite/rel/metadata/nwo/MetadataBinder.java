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

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedCollation;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedColumnUniqueness;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedDistinctRowCount;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedExplainVisibility;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedMaxRowCount;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedNonCumulativeCost;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedPredicates;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedRowCount;
import org.apache.calcite.rel.metadata.nwo.cleaned.GeneratedSelectivity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

public class MetadataBinder {

  public static void bind(RegistryMetadataProvider registryMetadataProvider){
    registryMetadataProvider.register(
        new GeneratedCollation.Collations(new RelMdCollation())
    );
    registryMetadataProvider.register(
        new GeneratedColumnUniqueness.areColumnsUnique(new RelMdColumnUniqueness())
    );
    registryMetadataProvider.register(
        new GeneratedDistinctRowCount.getDistinctRowCount(new RelMdDistinctRowCount())
    );
    registryMetadataProvider.register(
        new GeneratedExplainVisibility.isVisibleInExplain(new RelMdExplainVisibility())
    );
    registryMetadataProvider.register(
        new GeneratedNonCumulativeCost.getNonCumulativeCost(new RelMdPercentageOriginalRows())
    );
    registryMetadataProvider.register(
        new GeneratedMaxRowCount.getMaxRowCount(new RelMdMaxRowCount())
    );
    registryMetadataProvider.register(
        new GeneratedPredicates.getPredicates(new RelMdPredicates())
    );
    registryMetadataProvider.register(
        new GeneratedRowCount.getRowCount(new RelMdRowCount())
    );
    registryMetadataProvider.register(
        new GeneratedSelectivity.getSelectivity(new RelMdSelectivity())
    );
  }

  public static MetadataCallSite.NoArg<@Nullable Double> rowCount(){
    RelMdRowCount relMdRowCount = new RelMdRowCount();
    return new MetadataCallSite.NoArg<@Nullable Double>() {

      @Override public MetadataType<Double, MetadataArguments.NoArg> metadataType() {
        return MetadataTypes.RowCount;
      }

      @Override public @Nullable Double extract(MetadataArguments.NoArg arg) {
        RelNode relNode = arg.relNode;
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        if(relNode instanceof RelSubset) {
          return relMdRowCount.getRowCount((RelSubset) relNode, mq);
        } else if (relNode instanceof TableModify) {
          return relMdRowCount.getRowCount((TableModify)relNode, mq);
        } else if (relNode instanceof Exchange) {
          return relMdRowCount.getRowCount((Exchange)relNode, mq);
        } else if (relNode instanceof Union) {
          return relMdRowCount.getRowCount((Union) relNode, mq);
        } else if (relNode instanceof Intersect) {
          return relMdRowCount.getRowCount((Intersect)relNode, mq);
        } else if (relNode instanceof Minus) {
          return relMdRowCount.getRowCount((Minus)relNode, mq);
        } else if (relNode instanceof Filter) {
          return relMdRowCount.getRowCount((Filter)relNode, mq);
        } else if (relNode instanceof Calc) {
          return relMdRowCount.getRowCount((Calc)relNode, mq);
        } else if (relNode instanceof Project) {
          return relMdRowCount.getRowCount((Project)relNode, mq);
        } else if (relNode instanceof Sort) {
          return relMdRowCount.getRowCount((Sort)relNode, mq);
        } else if (relNode instanceof EnumerableLimit) {
          return relMdRowCount.getRowCount((EnumerableLimit)relNode, mq);
        } else if (relNode instanceof Join) {
          return relMdRowCount.getRowCount((Join)relNode, mq);
        } else if (relNode instanceof Aggregate) {
          return relMdRowCount.getRowCount((Aggregate)relNode, mq);
        } else if (relNode instanceof SingleRel) {
          return relMdRowCount.getRowCount((SingleRel)relNode, mq);
        } else if (relNode instanceof TableScan) {
          return relMdRowCount.getRowCount((TableScan)relNode, mq);
        } else if (relNode instanceof Values) {
          return relMdRowCount.getRowCount((Values)relNode, mq);
        }else {
          return relMdRowCount.getRowCount(relNode, mq);
        }
      }
    };
  }

  public static MetadataCallSite.NoArg<@Nullable Multimap<Class<? extends RelNode>, RelNode>> nodeType() {
    RelMdNodeTypes relMdNodeTypes = new RelMdNodeTypes();
    return new MetadataCallSite.NoArg<@Nullable Multimap<Class<? extends RelNode>, RelNode>>() {
      @Override public MetadataType<@Nullable Multimap<Class<? extends RelNode>, RelNode>,
          MetadataArguments.NoArg> metadataType() {
        return MetadataTypes.NodeTypes;
      }

      @Override public @Nullable Multimap<Class<? extends RelNode>, RelNode> extract(
          MetadataArguments.NoArg arg) {
        RelNode relNode = arg.relNode;
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        if (relNode instanceof HepRelVertex) {
          return relMdNodeTypes.getNodeTypes((HepRelVertex) relNode, mq);
        } else if (relNode instanceof RelSubset) {
          return relMdNodeTypes.getNodeTypes((RelSubset) relNode, mq);
        } else if (relNode instanceof Union) {
          return relMdNodeTypes.getNodeTypes((Union) relNode, mq);
        } else if (relNode instanceof Intersect) {
          return relMdNodeTypes.getNodeTypes((Intersect) relNode, mq);
        } else if (relNode instanceof Minus) {
          return relMdNodeTypes.getNodeTypes((Minus) relNode, mq);
        } else if (relNode instanceof Filter) {
          return relMdNodeTypes.getNodeTypes((Filter) relNode, mq);
        } else if (relNode instanceof Calc) {
          return relMdNodeTypes.getNodeTypes((Calc) relNode, mq);
        } else if (relNode instanceof Project) {
          return relMdNodeTypes.getNodeTypes((Project) relNode, mq);
        } else if (relNode instanceof Sort) {
          return relMdNodeTypes.getNodeTypes((Sort) relNode, mq);
        } else if (relNode instanceof Join) {
          return relMdNodeTypes.getNodeTypes((Join) relNode, mq);
        } else if (relNode instanceof Aggregate) {
          return relMdNodeTypes.getNodeTypes((Aggregate) relNode, mq);
        } else if (relNode instanceof TableScan) {
          return relMdNodeTypes.getNodeTypes((TableScan) relNode, mq);
        } else if (relNode instanceof Values) {
          return relMdNodeTypes.getNodeTypes((Values) relNode, mq);
        } else if (relNode instanceof TableModify) {
          return relMdNodeTypes.getNodeTypes((TableModify) relNode, mq);
        } else if (relNode instanceof Exchange) {
          return relMdNodeTypes.getNodeTypes((Exchange) relNode, mq);
        } else if (relNode instanceof Sample) {
          return relMdNodeTypes.getNodeTypes((Sample) relNode, mq);
        } else if (relNode instanceof Correlate) {
          return relMdNodeTypes.getNodeTypes((Correlate) relNode, mq);
        } else if (relNode instanceof Window) {
          return relMdNodeTypes.getNodeTypes((Window) relNode, mq);
        } else if (relNode instanceof Match) {
          return relMdNodeTypes.getNodeTypes((Match) relNode, mq);
        } else {
          return relMdNodeTypes.getNodeTypes(relNode, mq);
        }
      }
    };
  }

  public static MetadataCallSite.NoArg<ImmutableList<RelCollation>> collactions(){
    return null;
  }
}
