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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
>>>>>>> 79e6848c7 (DX-34864 Add CoreRules and move static references of these rules to CoreRules)
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
<<<<<<< HEAD
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
=======
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.core.Aggregate}
 * if it computes no aggregate functions
 * (that is, it is implementing {@code SELECT DISTINCT}),
 * or all the aggregate functions are splittable,
 * and the underlying relational expression is already distinct.
 *
 * @see CoreRules#AGGREGATE_REMOVE
 */
public class AggregateRemoveRule
    extends RelRule<AggregateRemoveRule.Config>
    implements SubstitutionRule {

  /** Creates an AggregateRemoveRule. */
  protected AggregateRemoveRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateRemoveRule(Class<? extends Aggregate> aggregateClass) {
    this(aggregateClass, RelFactories.LOGICAL_BUILDER);
  }

  @Deprecated // to be removed before 2.0
  public AggregateRemoveRule(Class<? extends Aggregate> aggregateClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass));
  }

  private static boolean isAggregateSupported(Aggregate aggregate) {
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE
        || aggregate.getGroupCount() == 0) {
      return false;
    }
    // If any aggregate functions do not support splitting, bail out.
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if (aggregateCall.filterArg >= 0
          || !aggregateCall.getAggregation()
              .maybeUnwrap(SqlSplittableAggFunction.class).isPresent()) {
        return false;
      }
    }
    return true;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .as(Config.class)
        .withOperandFor(LogicalAggregate.class);

    @Override default AggregateRemoveRule toRule() {
      return new AggregateRemoveRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b ->
          b.operand(aggregateClass)
                  .oneInput(b1 ->
                          b1.operand(RelNode.class).anyInputs()))
          .as(Config.class);
    }
  }
}

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .as(Config.class)
        .withOperandFor(LogicalAggregate.class);

    @Override default AggregateRemoveRule toRule() {
      return new AggregateRemoveRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b ->
          b.operand(aggregateClass)
              .predicate(AggregateRemoveRule::isAggregateSupported)
              .anyInputs())
          .as(Config.class);
    }
  }
}
