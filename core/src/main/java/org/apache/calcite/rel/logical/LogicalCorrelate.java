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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
    assert !CalcitePrepareImpl.DEBUG || isValid(Litmus.THROW, null);
  }

  @Deprecated // To be removed before 2.0
  public LogicalCorrelate(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
      RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns,
      SemiJoinType joinType) {
    this(cluster, traitSet, left, right, correlationId, requiredColumns,
        joinType.toJoinType());
  }

  /**
   * Creates a LogicalCorrelate by parsing serialized output.
   */
  public LogicalCorrelate(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        new CorrelationId((Integer) input.get("correlation")),
        input.getBitSet("requiredColumns"),
        input.getEnum("joinType", JoinRelType.class));
  }

  /** Creates a LogicalCorrelate. */
  public static LogicalCorrelate create(RelNode left, RelNode right,
      CorrelationId correlationId, ImmutableBitSet requiredColumns,
      JoinRelType joinType) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalCorrelate(cluster, traitSet, left, right, correlationId,
        requiredColumns, joinType);
  }

  @Deprecated // To be removed before 2.0
  public static LogicalCorrelate create(RelNode left, RelNode right,
      CorrelationId correlationId, ImmutableBitSet requiredColumns,
      SemiJoinType joinType) {
    return create(left, right, correlationId, requiredColumns, joinType.toJoinType());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalCorrelate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalCorrelate(getCluster(), traitSet, left, right,
        correlationId, requiredColumns, joinType);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}
