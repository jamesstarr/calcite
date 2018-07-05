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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
<<<<<<< HEAD
=======
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
>>>>>>> df3383b8b ([CALCITE-2259] Allow Java 8 syntax)

<<<<<<< HEAD
  @Deprecated // To be removed before 2.0
  protected Correlate(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, CorrelationId correlationId, ImmutableBitSet
      requiredColumns, SemiJoinType joinType) {
    super(cluster, traits, left, right);
    this.joinType = joinType.toJoinType();
    this.correlationId = correlationId;
    this.requiredColumns = requiredColumns;
  }

=======
>>>>>>> 7ae993b90 ([CALCITE-3133] Remove completely SemiJoinType)
  /**
   * Creates a Correlate by parsing serialized output.
   *
   * @param input Input representation
   */
  public Correlate(RelInput input) {
    this(
        input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        new CorrelationId((Integer) input.get("correlation")),
        input.getBitSet("requiredColumns"),
        input.getEnum("joinType", JoinRelType.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean isValid(Litmus litmus, Context context) {
    ImmutableBitSet leftColumns = ImmutableBitSet.range(left.getRowType().getFieldCount());
    return super.isValid(litmus, context)
        && litmus.check(leftColumns.contains(requiredColumns),
        "Required columns {} not subset of left columns {}", requiredColumns, leftColumns)
        && RelOptUtil.notContainsCorrelation(left, correlationId, litmus);
  }

  @Override public Correlate copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return copy(traitSet,
        inputs.get(0),
        inputs.get(1),
        correlationId,
        requiredColumns,
        joinType);
  }

  public abstract Correlate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType);

  public JoinRelType getJoinType() {
    return joinType;
  }

  @Override protected RelDataType deriveRowType() {
    switch (joinType) {
    case LEFT:
    case INNER:
      return SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
          right.getRowType(), joinType,
          getCluster().getTypeFactory(), null,
          ImmutableList.of());
    case ANTI:
    case SEMI:
      return left.getRowType();
    default:
      throw new IllegalStateException("Unknown join type " + joinType);
    }
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("correlation", correlationId)
        .item("joinType", joinType.lowerName)
        .item("requiredColumns", requiredColumns);
  }

  /**
   * Returns the correlating expressions.
   *
   * @return correlating expressions
   */
  public CorrelationId getCorrelationId() {
    return correlationId;
  }

  @Override public String getCorrelVariable() {
    return correlationId.getName();
  }

  /**
   * Returns the required columns in left relation required for the correlation
   * in the right.
   *
   * @return columns in left relation required for the correlation in the right
   */
  public ImmutableBitSet getRequiredColumns() {
    return requiredColumns;
  }

  @Override public Set<CorrelationId> getVariablesSet() {
    return ImmutableSet.of(correlationId);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    double leftRowCount = mq.getRowCount(left);
    switch (joinType) {
    case SEMI:
    case ANTI:
      return leftRowCount;
    default:
      return leftRowCount * mq.getRowCount(right);
    }
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);

    final double rightRowCount = right.estimateRowCount(mq);
    final double leftRowCount = left.estimateRowCount(mq);
    if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    Double restartCount = mq.getRowCount(getLeft());
    // RelMetadataQuery.getCumulativeCost(getRight()); does not work for
    // RelSubset, so we ask planner to cost-estimate right relation
    RelOptCost rightCost = planner.getCost(getRight(), mq);
    RelOptCost rescanCost =
        rightCost.multiplyBy(Math.max(1.0, restartCount - 1));

    return planner.getCostFactory().makeCost(
        rowCount /* generate results */ + leftRowCount /* scan left results */,
        0, 0).plus(rescanCost);
  }
}
