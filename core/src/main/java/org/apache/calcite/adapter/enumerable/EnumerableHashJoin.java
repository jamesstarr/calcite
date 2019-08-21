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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
=======
>>>>>>> fc94695f0 ([CALCITE-3089] Deprecate EquiJoin)
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
    Expression predicate = Expressions.constant(null);
    if (!joinInfo.nonEquiConditions.isEmpty()) {
      RexNode nonEquiCondition = RexUtil.composeConjunction(
          getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
      if (nonEquiCondition != null) {
        predicate = generatePredicate(implementor,
            leftResult.physType,
            rightResult.physType, nonEquiCondition);
      }
    }
    return implementor.result(
        physType,
        builder.append(
            Expressions.call(
                leftExpression,
                BuiltInMethod.HASH_JOIN.method,
                Expressions.list(
                    rightExpression,
                    leftResult.physType.generateAccessor(joinInfo.leftKeys),
                    rightResult.physType.generateAccessor(joinInfo.rightKeys),
                    EnumUtils.joinSelector(joinType,
                        physType,
                        ImmutableList.of(
                            leftResult.physType, rightResult.physType)))
                    .append(
                        Util.first(keyPhysType.comparer(),
                            Expressions.constant(null)))
                    .append(
                        Expressions.constant(joinType.generatesNullsOnLeft()))
                    .append(
                        Expressions.constant(
                            joinType.generatesNullsOnRight())).append(predicate)
            ))
            .toBlock());
<<<<<<< HEAD
=======
  }

  Expression generatePredicate(EnumerableRelImplementor implementor,
      PhysType leftPhysType, PhysType rightPhysType,
      RexNode condition) {
    BlockBuilder builder = new BlockBuilder();
    final ParameterExpression left_ =
        Expressions.parameter(leftPhysType.getJavaRowType(), "left");
    final ParameterExpression right_ =
        Expressions.parameter(rightPhysType.getJavaRowType(), "right");
    final RexProgramBuilder program =
        new RexProgramBuilder(
            implementor.getTypeFactory().builder()
                .addAll(left.getRowType().getFieldList())
                .addAll(right.getRowType().getFieldList())
                .build(),
            getCluster().getRexBuilder());
    program.addCondition(condition);
    builder.add(
        Expressions.return_(null,
            RexToLixTranslator.translateCondition(program.getProgram(),
                implementor.getTypeFactory(),
                builder,
                new RexToLixTranslator.InputGetterImpl(
                    ImmutableList.of(Pair.of(left_, leftPhysType),
                        Pair.of(right_, rightPhysType))),
                implementor.allCorrelateVariables,
                implementor.getConformance())));
    return Expressions.lambda(Predicate2.class, builder.toBlock(), left_,
        right_);
>>>>>>> dad1a9684 ([CALCITE-2973],[CALCITE-3284] Allow joins(hash,semi,anti) that have equi conditions to be executed using a hash join algorithm)
  }
}
<<<<<<< HEAD
=======

// End EnumerableHashJoin.java
