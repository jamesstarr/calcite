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

>>>>>>> 273e6a302 (DX-34523:cherry pick 051b6919dfc5b60406e81d5e8b8d)
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
>>>>>>> 6d818df58 ([CALCITE-2696] Improve design of join-like relational expressions)
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import javax.annotation.Nonnull;

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
public class RelFactories {
  public static final ProjectFactory DEFAULT_PROJECT_FACTORY =
      new ProjectFactoryImpl();

  public static final FilterFactory DEFAULT_FILTER_FACTORY =
      new FilterFactoryImpl();

  public static final JoinFactory DEFAULT_JOIN_FACTORY = new JoinFactoryImpl();

  public static final CorrelateFactory DEFAULT_CORRELATE_FACTORY =
      new CorrelateFactoryImpl();

  public static final SortFactory DEFAULT_SORT_FACTORY =
      new SortFactoryImpl();

  public static final ExchangeFactory DEFAULT_EXCHANGE_FACTORY =
      new ExchangeFactoryImpl();

  public static final SortExchangeFactory DEFAULT_SORT_EXCHANGE_FACTORY =
      new SortExchangeFactoryImpl();

  public static final AggregateFactory DEFAULT_AGGREGATE_FACTORY =
      new AggregateFactoryImpl();

  public static final MatchFactory DEFAULT_MATCH_FACTORY =
      new MatchFactoryImpl();

  public static final SetOpFactory DEFAULT_SET_OP_FACTORY =
      new SetOpFactoryImpl();

  public static final ValuesFactory DEFAULT_VALUES_FACTORY =
      new ValuesFactoryImpl();

  public static final TableScanFactory DEFAULT_TABLE_SCAN_FACTORY =
      new TableScanFactoryImpl();

  public static final Struct DEFAULT_STRUCT =
      new Struct(DEFAULT_FILTER_FACTORY,
          DEFAULT_PROJECT_FACTORY,
          DEFAULT_AGGREGATE_FACTORY,
          DEFAULT_SORT_FACTORY,
          DEFAULT_SET_OP_FACTORY,
          DEFAULT_JOIN_FACTORY,
          DEFAULT_CORRELATE_FACTORY,
          DEFAULT_VALUES_FACTORY,
          DEFAULT_TABLE_SCAN_FACTORY,
          DEFAULT_MATCH_FACTORY);

  /** A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
   * create logical relational expressions for everything. */
  public static final RelBuilderFactory LOGICAL_BUILDER =
      RelBuilder.proto(Contexts.of(DEFAULT_STRUCT));

  private RelFactories() {
  }

  /**
   * Can create a
   * {@link org.apache.calcite.rel.logical.LogicalProject} of the
   * appropriate type for this rule's calling convention.
   */
  public interface ProjectFactory {
    /**
     * Creates a project.
     *
     * @param input The input
     * @param hints The hints
     * @param childExprs The projection expressions
     * @param fieldNames The projection field names
     * @return a project
     */
    RelNode createProject(RelNode input, List<RelHint> hints,
        List<? extends RexNode> childExprs, List<String> fieldNames);

    @Deprecated // to be removed before 1.23
    default RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType,
        boolean semiJoinDone) {
      return createJoin(left, right, ImmutableList.of(), condition, variablesSet,
          joinType, semiJoinDone);
    }

    @Deprecated // to be removed before 1.23
    default RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone) {
      return createJoin(left, right, ImmutableList.of(), condition,
          CorrelationId.setOf(variablesStopped), joinType, semiJoinDone);
    }
  }

  /**
   * Implementation of {@link JoinFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  private static class JoinFactoryImpl implements JoinFactory {
    public RelNode createJoin(RelNode left, RelNode right, List<RelHint> hints,
        RexNode condition, Set<CorrelationId> variablesSet,
        JoinRelType joinType, boolean semiJoinDone) {
      return LogicalJoin.create(left, right, hints, condition, variablesSet, joinType,
          semiJoinDone, ImmutableList.<RelDataTypeField>of());
    }

    public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone) {
      return createJoin(left, right, ImmutableList.of(), condition,
          CorrelationId.setOf(variablesStopped), joinType, semiJoinDone);
    }
  }

  /**
   * Can create a correlate of the appropriate type for a rule's calling
   * convention.
   *
   * <p>The result is typically a {@link Correlate}.
   */
  public interface CorrelateFactory {
    /**
     * Creates a correlate.
     *
     * @param left             Left input
     * @param right            Right input
     * @param correlationId    Variable name for the row of left input
     * @param requiredColumns  Required columns
     * @param joinType         Join type
     */
    RelNode createCorrelate(RelNode left, RelNode right,
        CorrelationId correlationId, ImmutableBitSet requiredColumns,
        JoinRelType joinType);
  }

  /**
   * Implementation of {@link CorrelateFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalCorrelate}.
   */
  private static class CorrelateFactoryImpl implements CorrelateFactory {
    public RelNode createCorrelate(RelNode left, RelNode right,
        CorrelationId correlationId, ImmutableBitSet requiredColumns,
        JoinRelType joinType) {
      return LogicalCorrelate.create(left, right, correlationId,
          requiredColumns, joinType);
    }
  }

  /**
   * Can create a semi-join of the appropriate type for a rule's calling
   * convention.
   *
   * @deprecated Use {@link JoinFactory} instead.
   */
  @Deprecated // to be removed before 2.0
  public interface SemiJoinFactory {
    /**
     * Creates a semi-join.
     *
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     */
    RelNode createSemiJoin(RelNode left, RelNode right, RexNode condition);
  }

  /**
    RelNode createScan(RelOptCluster cluster, RelOptTable table, List<RelHint> hints);

    @Deprecated // to be removed before 1.23
    default RelNode createScan(RelOptCluster cluster, RelOptTable table) {
      return createScan(cluster, table, ImmutableList.of());
    }
  }

  /**
   * Implementation of {@link TableScanFactory} that returns a
   * {@link LogicalTableScan}.
   */
  private static class TableScanFactoryImpl implements TableScanFactory {
    public RelNode createScan(RelOptCluster cluster, RelOptTable table, List<RelHint> hints) {
      return LogicalTableScan.create(cluster, table, hints);
    }
  }

  /**
   * Can create a {@link Match} of
   * the appropriate type for a rule's calling convention.
   */
  public interface MatchFactory {
    /** Creates a {@link Match}. */
    RelNode createMatch(RelNode input, RexNode pattern,
        RelDataType rowType, boolean strictStart, boolean strictEnd,
        Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
        boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
        RexNode interval);
  }

  /**
   * Implementation of {@link MatchFactory}
   * that returns a {@link LogicalMatch}.
   */
  private static class MatchFactoryImpl implements MatchFactory {
    public RelNode createMatch(RelNode input, RexNode pattern,
        RelDataType rowType, boolean strictStart, boolean strictEnd,
        Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
        boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
        RexNode interval) {
      return LogicalMatch.create(input, rowType, pattern, strictStart,
          strictEnd, patternDefinitions, measures, after, subsets, allRows,
          partitionKeys, orderKeys, interval);
    }
  }
<<<<<<< HEAD
=======

  /** Immutable record that contains an instance of each factory. */
  public static class Struct {
    public final FilterFactory filterFactory;
    public final ProjectFactory projectFactory;
    public final AggregateFactory aggregateFactory;
    public final SortFactory sortFactory;
    public final SetOpFactory setOpFactory;
    public final JoinFactory joinFactory;
    public final CorrelateFactory correlateFactory;
    public final ValuesFactory valuesFactory;
    public final TableScanFactory scanFactory;
    public final MatchFactory matchFactory;

    private Struct(FilterFactory filterFactory,
        ProjectFactory projectFactory,
        AggregateFactory aggregateFactory,
        SortFactory sortFactory,
        SetOpFactory setOpFactory,
        JoinFactory joinFactory,
        CorrelateFactory correlateFactory,
        ValuesFactory valuesFactory,
        TableScanFactory scanFactory,
        MatchFactory matchFactory) {
      this.filterFactory = Objects.requireNonNull(filterFactory);
      this.projectFactory = Objects.requireNonNull(projectFactory);
      this.aggregateFactory = Objects.requireNonNull(aggregateFactory);
      this.sortFactory = Objects.requireNonNull(sortFactory);
      this.setOpFactory = Objects.requireNonNull(setOpFactory);
      this.joinFactory = Objects.requireNonNull(joinFactory);
      this.correlateFactory = Objects.requireNonNull(correlateFactory);
      this.valuesFactory = Objects.requireNonNull(valuesFactory);
      this.scanFactory = Objects.requireNonNull(scanFactory);
      this.matchFactory = Objects.requireNonNull(matchFactory);
    }

    public static @Nonnull Struct fromContext(Context context) {
      Struct struct = context.unwrap(Struct.class);
      if (struct != null) {
        return struct;
      }
      return new Struct(
          Util.first(context.unwrap(FilterFactory.class),
              DEFAULT_FILTER_FACTORY),
          Util.first(context.unwrap(ProjectFactory.class),
              DEFAULT_PROJECT_FACTORY),
          Util.first(context.unwrap(AggregateFactory.class),
              DEFAULT_AGGREGATE_FACTORY),
          Util.first(context.unwrap(SortFactory.class),
              DEFAULT_SORT_FACTORY),
          Util.first(context.unwrap(SetOpFactory.class),
              DEFAULT_SET_OP_FACTORY),
          Util.first(context.unwrap(JoinFactory.class),
              DEFAULT_JOIN_FACTORY),
          Util.first(context.unwrap(CorrelateFactory.class),
              DEFAULT_CORRELATE_FACTORY),
          Util.first(context.unwrap(ValuesFactory.class),
              DEFAULT_VALUES_FACTORY),
          Util.first(context.unwrap(TableScanFactory.class),
              DEFAULT_TABLE_SCAN_FACTORY),
          Util.first(context.unwrap(MatchFactory.class),
              DEFAULT_MATCH_FACTORY));
    }
  }
}

  /**
   * Can create a {@link Spool} of
   * the appropriate type for a rule's calling convention.
   */
  public interface SpoolFactory {
    /** Creates a {@link TableSpool}. */
    RelNode createTableSpool(RelNode input, Spool.Type readType,
        Spool.Type writeType, RelOptTable table);
  }

  /**
   * Implementation of {@link SpoolFactory}
   * that returns Logical Spools.
   */
  private static class SpoolFactoryImpl implements SpoolFactory {
    public RelNode createTableSpool(RelNode input, Spool.Type readType,
        Spool.Type writeType, RelOptTable table) {
      return LogicalTableSpool.create(input, readType, writeType, table);
    }
  }

  /**
   * Can create a {@link RepeatUnion} of
   * the appropriate type for a rule's calling convention.
   */
  @Experimental
  public interface RepeatUnionFactory {
    /** Creates a {@link RepeatUnion}. */
    RelNode createRepeatUnion(RelNode seed, RelNode iterative, boolean all,
        int iterationLimit, RelOptTable table);
  }

  /**
   * Implementation of {@link RepeatUnion}
   * that returns a {@link LogicalRepeatUnion}.
   */
  private static class RepeatUnionFactoryImpl implements RepeatUnionFactory {
    public RelNode createRepeatUnion(RelNode seed, RelNode iterative,
        boolean all, int iterationLimit, RelOptTable table) {
      return LogicalRepeatUnion.create(seed, iterative, all, iterationLimit, table);
    }
  }

  /** Immutable record that contains an instance of each factory. */
  public static class Struct {
    public final FilterFactory filterFactory;
    public final ProjectFactory projectFactory;
    public final AggregateFactory aggregateFactory;
    public final SortFactory sortFactory;
    public final ExchangeFactory exchangeFactory;
    public final SortExchangeFactory sortExchangeFactory;
    public final SetOpFactory setOpFactory;
    public final JoinFactory joinFactory;
    public final CorrelateFactory correlateFactory;
    public final ValuesFactory valuesFactory;
    public final TableScanFactory scanFactory;
    public final TableFunctionScanFactory tableFunctionScanFactory;
    public final SnapshotFactory snapshotFactory;
    public final MatchFactory matchFactory;
    public final SpoolFactory spoolFactory;
    public final RepeatUnionFactory repeatUnionFactory;

    private Struct(FilterFactory filterFactory,
        ProjectFactory projectFactory,
        AggregateFactory aggregateFactory,
        SortFactory sortFactory,
        ExchangeFactory exchangeFactory,
        SortExchangeFactory sortExchangeFactory,
        SetOpFactory setOpFactory,
        JoinFactory joinFactory,
        CorrelateFactory correlateFactory,
        ValuesFactory valuesFactory,
        TableScanFactory scanFactory,
        TableFunctionScanFactory tableFunctionScanFactory,
        SnapshotFactory snapshotFactory,
        MatchFactory matchFactory,
        SpoolFactory spoolFactory,
        RepeatUnionFactory repeatUnionFactory) {
      this.filterFactory = Objects.requireNonNull(filterFactory);
      this.projectFactory = Objects.requireNonNull(projectFactory);
      this.aggregateFactory = Objects.requireNonNull(aggregateFactory);
      this.sortFactory = Objects.requireNonNull(sortFactory);
      this.exchangeFactory = Objects.requireNonNull(exchangeFactory);
      this.sortExchangeFactory = Objects.requireNonNull(sortExchangeFactory);
      this.setOpFactory = Objects.requireNonNull(setOpFactory);
      this.joinFactory = Objects.requireNonNull(joinFactory);
      this.correlateFactory = Objects.requireNonNull(correlateFactory);
      this.valuesFactory = Objects.requireNonNull(valuesFactory);
      this.scanFactory = Objects.requireNonNull(scanFactory);
      this.tableFunctionScanFactory =
          Objects.requireNonNull(tableFunctionScanFactory);
      this.snapshotFactory = Objects.requireNonNull(snapshotFactory);
      this.matchFactory = Objects.requireNonNull(matchFactory);
      this.spoolFactory = Objects.requireNonNull(spoolFactory);
      this.repeatUnionFactory = Objects.requireNonNull(repeatUnionFactory);
    }

    public static @Nonnull Struct fromContext(Context context) {
      Struct struct = context.unwrap(Struct.class);
      if (struct != null) {
        return struct;
      }
      return new Struct(
          Util.first(context.unwrap(FilterFactory.class),
              DEFAULT_FILTER_FACTORY),
          Util.first(context.unwrap(ProjectFactory.class),
              DEFAULT_PROJECT_FACTORY),
          Util.first(context.unwrap(AggregateFactory.class),
              DEFAULT_AGGREGATE_FACTORY),
          Util.first(context.unwrap(SortFactory.class),
              DEFAULT_SORT_FACTORY),
          Util.first(context.unwrap(ExchangeFactory.class),
              DEFAULT_EXCHANGE_FACTORY),
          Util.first(context.unwrap(SortExchangeFactory.class),
              DEFAULT_SORT_EXCHANGE_FACTORY),
          Util.first(context.unwrap(SetOpFactory.class),
              DEFAULT_SET_OP_FACTORY),
          Util.first(context.unwrap(JoinFactory.class),
              DEFAULT_JOIN_FACTORY),
          Util.first(context.unwrap(CorrelateFactory.class),
              DEFAULT_CORRELATE_FACTORY),
          Util.first(context.unwrap(ValuesFactory.class),
              DEFAULT_VALUES_FACTORY),
          Util.first(context.unwrap(TableScanFactory.class),
              DEFAULT_TABLE_SCAN_FACTORY),
          Util.first(context.unwrap(TableFunctionScanFactory.class),
              DEFAULT_TABLE_FUNCTION_SCAN_FACTORY),
          Util.first(context.unwrap(SnapshotFactory.class),
              DEFAULT_SNAPSHOT_FACTORY),
          Util.first(context.unwrap(MatchFactory.class),
              DEFAULT_MATCH_FACTORY),
          Util.first(context.unwrap(SpoolFactory.class),
              DEFAULT_SPOOL_FACTORY),
          Util.first(context.unwrap(RepeatUnionFactory.class),
              DEFAULT_REPEAT_UNION_FACTORY));
    }
  }
}
