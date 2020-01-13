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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

    super(cluster, traits, left, right, condition, variablesSet, joinType);
=======
    super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    this.leftKeys = Objects.requireNonNull(joinInfo.leftKeys);
    this.rightKeys = Objects.requireNonNull(joinInfo.rightKeys);
    assert joinInfo.isEqui() : "Create EquiJoin with non-equi join condition.";
  }

  /** Creates an EquiJoin. */
  @Deprecated // to be removed before 2.0
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    this.leftKeys = Preconditions.checkNotNull(leftKeys);
    this.rightKeys = Preconditions.checkNotNull(rightKeys);
  }

  @Deprecated // to be removed before 2.0
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traits, left, right, condition, leftKeys, rightKeys,
        CorrelationId.setOf(variablesStopped), joinType);
  }

  public ImmutableIntList getLeftKeys() {
    return leftKeys;
  }

  public ImmutableIntList getRightKeys() {
    return rightKeys;
  }
}
