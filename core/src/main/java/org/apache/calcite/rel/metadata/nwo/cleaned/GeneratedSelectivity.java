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
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

public class GeneratedSelectivity {
  public static class getSelectivity implements MetadataCallSite.RexNodeArg<@Nullable Double>{
    private final org.apache.calcite.rel.metadata.RelMdSelectivity _RelMdSelectivity;
    public getSelectivity(
      org.apache.calcite.rel.metadata.RelMdSelectivity _RelMdSelectivity) {
      this._RelMdSelectivity = _RelMdSelectivity;
    }

    @Override public MetadataType<@Nullable Double, MetadataArguments.RexNodeArg> metadataType() {
      return MetadataTypes.Selectivity;
    }

    @Override public @Nullable Double extract(MetadataArguments.RexNodeArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      RexNode rexNode = arg.rexNode;

      if (relNode instanceof org.apache.calcite.rel.core.Filter) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.Filter)relNode, mq, rexNode);
      } else if (relNode instanceof org.apache.calcite.rel.core.Calc) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.Calc)relNode, mq, rexNode);
      } else if (relNode instanceof org.apache.calcite.rel.core.TableModify) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.TableModify)relNode, mq, rexNode);
      } else if (relNode instanceof org.apache.calcite.rel.core.Project) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.Project)relNode, mq, rexNode);
      } else if (relNode instanceof org.apache.calcite.rel.core.Aggregate) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.Aggregate)relNode, mq, rexNode);
      } else if (relNode instanceof org.apache.calcite.rel.core.Union) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.Union)relNode, mq, rexNode);
      } else if (relNode instanceof org.apache.calcite.rel.core.Sort) {
        return _RelMdSelectivity.getSelectivity((org.apache.calcite.rel.core.Sort)relNode, mq,rexNode);
      } else {
        return _RelMdSelectivity.getSelectivity(relNode,mq, rexNode);
      }
    }
  }

}
