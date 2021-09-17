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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;

            .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                  public List<RelCollation> get() {
                    return RelMdCollation.project(mq, input, projects);
                  }
                });
    return new LogicalProject(cluster, traitSet, hints, input, projects, rowType);
  }

  @Deprecated // to be removed before 1.23
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, List<String> fieldNames) {
    return create(input, ImmutableList.of(), projects, fieldNames);
  }

  @Deprecated // to be removed before 1.23
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    return create(input, ImmutableList.of(), projects, rowType);
  }

  @Override public LogicalProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new LogicalProject(getCluster(), traitSet, hints, input, projects, rowType);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
<<<<<<< HEAD
=======

  @Override public RelNode attachHints(List<RelHint> hintList) {
    return new LogicalProject(getCluster(), traitSet, mergeHints(hintList),
        input, getProjects(), rowType);
  }
}
>>>>>>> abb01f235 ([CALCITE-482] Implement sql and planner hints)

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new LogicalProject(getCluster(), traitSet, hintList,
        input, getProjects(), rowType);
  }
}
