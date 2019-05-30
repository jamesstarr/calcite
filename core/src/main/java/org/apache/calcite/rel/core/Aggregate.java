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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Relational operator that eliminates
 * duplicates and computes totals.
 *
 * <p>It corresponds to the {@code GROUP BY} operator in a SQL query
 * statement, together with the aggregate functions in the {@code SELECT}
 * clause.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}.
 * </ul>
 */
public abstract class Aggregate extends SingleRel implements Hintable {

  public static boolean isSimple(Aggregate aggregate) {
    return aggregate.getGroupType() == Group.SIMPLE;
  }
   * @param traits   Traits
   * @param child    Child
   * @param groupSet Bit set of grouping fields
   * @param groupSets List of all grouping sets; null for just {@code groupSet}
   * @param aggCalls Collection of calls to aggregate functions
   */
  protected Aggregate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traits, child);
    this.aggCalls = ImmutableList.copyOf(aggCalls);
    this.groupSet = Objects.requireNonNull(groupSet);
    if (groupSets == null) {
      this.groupSets = ImmutableList.of(groupSet);
    } else {
      this.groupSets = ImmutableList.copyOf(groupSets);
      assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
      for (ImmutableBitSet set : groupSets) {
        assert groupSet.contains(set);
      }
    }
    assert groupSet.length() <= input.getRowType().getFieldCount();
    for (AggregateCall aggCall : aggCalls) {
      assert typeMatchesInferred(aggCall, Litmus.THROW);
      Preconditions.checkArgument(aggCall.filterArg < 0
          || isPredicate(input, aggCall.filterArg),
          "filter must be BOOLEAN NOT NULL");
    }
  }

  /**
   * Creates an Aggregate.
   */
  @Deprecated // to be removed before 2.0
  protected Aggregate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    this(cluster, traits, child, groupSet, groupSets, aggCalls);
    Preconditions.checkArgument(!indicator,
        "indicator is not supported, use GROUPING function instead");
  }

  public static boolean isNotGrandTotal(Aggregate aggregate) {
    return aggregate.getGroupCount() > 0;
  }

  @Deprecated // to be removed before 2.0
  public static boolean noIndicator(Aggregate aggregate) {
    return true;
  }

  private boolean isPredicate(RelNode input, int index) {
    final RelDataType type =
        input.getRowType().getFieldList().get(index).getType();
    return type.getSqlTypeName() == SqlTypeName.BOOLEAN
        && !type.isNullable();
  }

  /**
   * Creates an Aggregate by parsing serialized output.
   */
  protected Aggregate(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBitSet("group"), input.getBitSetList("groups"),
        input.getAggregateCalls("aggs"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), false, groupSet, groupSets,
        aggCalls);
  }

  /** Creates a copy of this aggregate.
   *
   * @param traitSet Traits
   * @param input Input
    Preconditions.checkArgument(!indicator,
        "indicator is not supported, use GROUPING function instead");
    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      final String base;
      if (aggCall.e.name != null) {
        base = aggCall.e.name;
      } else {
        base = "$f" + (groupList.size() + aggCall.i);
      }
      String name = base;
      int i = 0;
      while (containedNames.contains(name)) {
        name = base + "_" + i++;
      }
      containedNames.add(name);
      builder.add(name, aggCall.e.type);
    }
    return builder.build();
  }

  private static boolean allContain(List<ImmutableBitSet> groupSets,
      int groupKey) {
    for (ImmutableBitSet groupSet : groupSets) {
      if (!groupSet.get(groupKey)) {
        return false;
      }
    }
    return true;
  }

  public boolean isValid(Litmus litmus, Context context) {
    return super.isValid(litmus, context)
        && litmus.check(Util.isDistinct(getRowType().getFieldNames()),
            "distinct field names: {}", getRowType());
  }

  /**
   * Returns whether the inferred type of an {@link AggregateCall} matches the
   * type it was given when it was created.
   *
   * @param aggCall Aggregate call
   * @param litmus What to do if an error is detected (types do not match)
   * @return Whether the inferred and declared types match
   */
  private boolean typeMatchesInferred(
      final AggregateCall aggCall,
      final Litmus litmus) {
    SqlAggFunction aggFunction = aggCall.getAggregation();
    AggCallBinding callBinding = aggCall.createBinding(this);
    RelDataType type = aggFunction.inferReturnType(callBinding);
    RelDataType expectedType = aggCall.type;
    return RelOptUtil.eq("aggCall type",
        expectedType,
        "inferred type",
        type,
        litmus);
  }

  /**
   * Returns whether any of the aggregates are DISTINCT.
   *
   * @return Whether any of the aggregates are DISTINCT
   */
  public boolean containsDistinctCall() {
    for (AggregateCall call : aggCalls) {
      if (call.isDistinct()) {
        return true;
      }
    }
    return false;
  }

  @Override public ImmutableList<RelHint> getHints() {
    return hints;
  }

  /**
   * Returns the type of roll-up.
   *
   * @return Type of roll-up
   */
  public Group getGroupType() {
    return Group.induce(groupSet, groupSets);
  }

  /** What kind of roll-up is it? */
  public enum Group {
    SIMPLE,
    ROLLUP,
    CUBE,
    OTHER;

    public static Group induce(ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets) {
      if (!ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets)) {
        throw new IllegalArgumentException("must be sorted: " + groupSets);
      }
      if (groupSets.size() == 1 && groupSets.get(0).equals(groupSet)) {
        return SIMPLE;
      }
      if (groupSets.size() == IntMath.pow(2, groupSet.cardinality())) {
        return CUBE;
      }
      if (isRollup(groupSet, groupSets)) {
        return ROLLUP;
      }
      return OTHER;
    }

    /** Returns whether a list of sets is a rollup.
     *
     * <p>For example, if {@code groupSet} is <code>{2, 4, 5}</code>, then
     * <code>[{2, 4, 5], {2, 5}, {5}, {}]</code> is a rollup. The first item is
     * equal to {@code groupSet}, and each subsequent item is a subset with one
     * fewer bit than the previous.
     *
     * @see #getRollup(List) */
    public static boolean isRollup(ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets) {
      if (groupSets.size() != groupSet.cardinality() + 1) {
        return false;
      }
      ImmutableBitSet g = null;
      for (ImmutableBitSet bitSet : groupSets) {
        if (g == null) {
          // First item must equal groupSet
          if (!bitSet.equals(groupSet)) {
            return false;
          }
        } else {
          // Each subsequent items must be a subset with one fewer bit than the
          // previous item
          if (!g.contains(bitSet)
              || g.except(bitSet).cardinality() != 1) {
            return false;
          }
        }
        g = bitSet;
      }
      assert g.isEmpty();
      return true;
    }

    /** Returns the ordered list of bits in a rollup.
     *
     * <p>For example, given a {@code groupSets} value
     * <code>[{2, 4, 5], {2, 5}, {5}, {}]</code>, returns the list
     * {@code [5, 2, 4]}, which are the succession of bits
     * added to each of the sets starting with the empty set.
     *
     * @see #isRollup(ImmutableBitSet, List) */
    public static List<Integer> getRollup(List<ImmutableBitSet> groupSets) {
      final Set<Integer> set = new LinkedHashSet<>();
      ImmutableBitSet g = null;
      for (ImmutableBitSet bitSet : groupSets) {
        if (g == null) {
          // First item must equal groupSet
        } else {
          // Each subsequent items must be a subset with one fewer bit than the
          // previous item
          set.addAll(g.except(bitSet).toList());
        }
        g = bitSet;
      }
      return ImmutableList.copyOf(set).reverse();
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Implementation of the {@link SqlOperatorBinding} interface for an
   * {@link AggregateCall aggregate call} applied to a set of operands in the
   * context of a {@link org.apache.calcite.rel.logical.LogicalAggregate}.
   */
  public static class AggCallBinding extends SqlOperatorBinding {
    private final List<RelDataType> operands;
    private final int groupCount;
    private final boolean filter;

    /**
     * Creates an AggCallBinding
     *
     * @param typeFactory  Type factory
     * @param aggFunction  Aggregate function
     * @param operands     Data types of operands
     * @param groupCount   Number of columns in the GROUP BY clause
     * @param filter       Whether the aggregate function has a FILTER clause
     */
    public AggCallBinding(RelDataTypeFactory typeFactory,
        SqlAggFunction aggFunction, List<RelDataType> operands, int groupCount,
        boolean filter) {
      super(typeFactory, aggFunction);
      this.operands = operands;
      this.groupCount = groupCount;
      this.filter = filter;
      assert operands != null
          : "operands of aggregate call should not be null";
      assert groupCount >= 0
          : "number of group by columns should be greater than zero in "
          + "aggregate call. Got " + groupCount;
    }

    @Override public int getGroupCount() {
      return groupCount;
    }

    @Override public boolean hasFilter() {
      return filter;
    }

    public int getOperandCount() {
      return operands.size();
    }

    public RelDataType getOperandType(int ordinal) {
      return operands.get(ordinal);
    }

    public CalciteException newError(
        Resources.ExInst<SqlValidatorException> e) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, e);
    }
  }
}
