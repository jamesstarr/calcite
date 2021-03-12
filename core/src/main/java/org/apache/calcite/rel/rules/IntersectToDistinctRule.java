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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.rel.rules.ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule.projectToWindow;

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Intersect}
 * (<code>all</code> = <code>false</code>)
 * into a group of operators composed of
 * {@link org.apache.calcite.rel.core.Union},
 * {@link org.apache.calcite.rel.core.Aggregate}, etc.
 *
 * <p> Rewrite: (GB-Union All-GB)-GB-UDTF (on all attributes)
 *
 * <h2>Example</h2>
 *
 * <p>Query: <code>R1 Intersect All R2</code>
 *
 * <p><code>R3 = GB(R1 on all attributes, count(*) as c)<br>
 *   union all<br>
 *   GB(R2 on all attributes, count(*) as c)</code>
 *
 * <p><code>R4 = GB(R3 on all attributes, count(c) as cnt, min(c) as m)</code>
 *
 * <p>Note that we do not need <code>min(c)</code> in intersect distinct.
 *
 * <p><code>R5 = Filter(cnt == #branch)</code>
 *
 * <p>If it is intersect all then
 *
 * <p><code>R6 = UDTF (R5) which will explode the tuples based on min(c)<br>
 * R7 = Project(R6 on all attributes)</code>
 *
 * <p>Else
 *
 * <p><code>R6 = Proj(R5 on all attributes)</code>
 *
 * @see org.apache.calcite.rel.rules.UnionToDistinctRule
 * @see CoreRules#INTERSECT_TO_DISTINCT
 */
public class IntersectToDistinctRule
    extends RelRule<IntersectToDistinctRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToDistinctRule. */
  protected IntersectToDistinctRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public IntersectToDistinctRule(Class<? extends Intersect> intersectClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(intersectClass));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    final RelOptCluster cluster = intersect.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final RelDataType intType = cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);

    RelDataType intersectRowType = intersect.getRowType();
    List<RexNode> intersectFields = call.builder().push(intersect).fields();
    RelDataType newRowType = cluster.getTypeFactory().createStructType(
        ImmutableList.<Map.Entry<String, RelDataType>>builder()
            .addAll(intersectRowType.getFieldList())
            .add(new AbstractMap.SimpleEntry("helloWorld", intType))
            .build()
    );

    final RexNode over = rexBuilder.makeOver(
        intType,
        SqlStdOperatorTable.ROW_NUMBER,
        ImmutableList.of(),
        intersectFields,
        ImmutableList.of(),
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.CURRENT_ROW,
        true, true, false, false, false);


        // 1st level GB: create a GB (col0, col1, count() as c) for each branch
    for (RelNode input : intersect.getInputs()) {
      relBuilder.push(input);
      if (intersect.all) {
        relBuilder.project(
            ImmutableList.<RexNode>builder()
              .addAll(intersectFields)
              .add(relBuilder.alias(over, "helloworld"))
              .build());
        relBuilder.push(projectToWindow(call, (Project) relBuilder.build()));
      } else {
        relBuilder.aggregate(
            relBuilder.groupKey(relBuilder.fields()),
            relBuilder.countStar(null)); //HACK we have to have an aggergations?
      }
    }

    // create a union above all the branches
    final int branchCount = intersect.getInputs().size();
    relBuilder.union(true, branchCount);
    final RelNode union = relBuilder.peek();

    // 2nd level GB: create a GB (col0, col1, count(c)) for each branch
    // the index of c is union.getRowType().getFieldList().size() - 1
    final int unionFieldCount = union.getRowType().getFieldCount() - ((intersect.all) ? 0 : 1);

    final ImmutableBitSet groupSet =
        ImmutableBitSet.range(unionFieldCount);
    relBuilder.aggregate(relBuilder.groupKey(groupSet),
        relBuilder.countStar(null));

    // add a filter count(c) = #branches
    relBuilder.filter(
        relBuilder.equals(relBuilder.field(relBuilder.fields().size() - 1),
            rexBuilder.makeBigintLiteral(new BigDecimal(branchCount))));

    // Project all but the last field
    relBuilder.project(intersectFields);

    // the schema for intersect distinct is like this
    // R3 on all attributes + count(c) as cnt
    // finally add a project to project out the last column
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalIntersect.class);

    @Override default IntersectToDistinctRule toRule() {
      return new IntersectToDistinctRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Intersect> intersectClass) {
      return withOperandSupplier(b -> b.operand(intersectClass).anyInputs())
          .as(Config.class);
    }
  }
}
