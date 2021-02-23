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
import org.apache.calcite.sql.SqlExplainLevel;

public class GeneratedExplainVisibility {
  public static class isVisibleInExplain implements MetadataCallSite.SqlExplainLevelArg<Boolean> {
    private final org.apache.calcite.rel.metadata.RelMdExplainVisibility _RelMdExplainVisibility;

    public isVisibleInExplain(
        org.apache.calcite.rel.metadata.RelMdExplainVisibility _RelMdExplainVisibility) {
      this._RelMdExplainVisibility = _RelMdExplainVisibility;
    }

    @Override public MetadataType<Boolean, MetadataArguments.SqlExplainLevelArg> metadataType() {
      return MetadataTypes.IsVisibleInExplain;
    }

    @Override public java.lang.Boolean extract(MetadataArguments.SqlExplainLevelArg arg) {
      RelNode relNode = arg.relNode;
      RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
      SqlExplainLevel sqlExplainLevel = arg.sqlExplainLevel;

      return _RelMdExplainVisibility.isVisibleInExplain(relNode, mq, sqlExplainLevel);
    }
  }
}
