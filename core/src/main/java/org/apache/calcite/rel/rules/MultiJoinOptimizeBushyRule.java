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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin.Edge;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static org.apache.calcite.plan.RelOptUtil.classifyFilters;
import static org.apache.calcite.rel.rules.LoptMultiJoin.Edge;
import static org.apache.calcite.util.mapping.Mappings.TargetMapping;

import javax.annotation.Nullable;

/**
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 * <p>It is triggered by the pattern
 * {@link org.apache.calcite.rel.logical.LogicalProject} ({@link MultiJoin}).
 *
 * <p>It is similar to
 * {@link org.apache.calcite.rel.rules.LoptOptimizeJoinRule}.
 * {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 *
 * <p>TODO:
 * <ol>
 *   <li>Join conditions that touch 1 factor.
 *   <li>Join conditions that touch 3 factors.
 *   <li>More than 1 join conditions that touch the same pair of factors,
 *       e.g. {@code t0.c1 = t1.c1 and t1.c2 = t0.c3}
 * </ol>
 *
 * @see CoreRules#MULTI_JOIN_OPTIMIZE_BUSHY
 */
public class MultiJoinOptimizeBushyRule
        extends RelRule<MultiJoinOptimizeBushyRule.Config>
        implements TransformationRule {

  private final @Nullable PrintWriter pw = CalciteSystemProperty.DEBUG.value()
          ? Util.printWriter(System.out)
          : null;

>>>>>>> 79e6848c7 (DX-34864 Add CoreRules and move static references of these rules to CoreRules)
  /** Creates a MultiJoinOptimizeBushyRule. */
  protected MultiJoinOptimizeBushyRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public MultiJoinOptimizeBushyRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public MultiJoinOptimizeBushyRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(joinFactory, projectFactory));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final RelMetadataQuery mq = call.getMetadataQuery();

    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

    final List<Vertex> vertexes = new ArrayList<>();
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      double cost = mq.getRowCount(rel);
      Double rowReduction = mq.getPercentageOriginalRows(rel);
      if (rowReduction == null) {
        rowReduction = 1.0;
      }

      vertexes.add(new LeafVertex(i, rel, cost, x, rowReduction));
      x += rel.getRowType().getFieldCount();
    }
    assert x == multiJoin.getNumTotalFields();

    final List<Edge> unusedEdges = new ArrayList<>();
    List<RexNode> remainingConditions = new ArrayList<>();
    for (RexNode node : multiJoin.getJoinFilters()) {
      Edge e = multiJoin.createEdge(node);
      if (e.factors.cardinality() != 2) {
        remainingConditions.add(node);
      } else {
        unusedEdges.add(multiJoin.createEdge(node));
      }
    }

    Iterator<Edge> iter = unusedEdges.iterator();
    Map<ImmutableBitSet, Edge> edgeMap = new HashMap<>();
    while (iter.hasNext()) {
      Edge edge = iter.next();
      if (!edge.condition.isA(SqlKind.EQUALS)) {
        continue;
      }
      iter.remove();
      Edge other = edgeMap.get(edge.factors);
      if (other == null) {
        edgeMap.put(edge.factors, edge);
        continue;
      }
      List<RexNode> conjunctions = new ArrayList<>();
      conjunctions.addAll(RelOptUtil.conjunctions(other.condition));
      conjunctions.add(edge.condition);
      ImmutableBitSet combinedColumns =
              edge.columns.rebuild().addAll(other.columns).build();
      Edge combined = new Edge(
              RexUtil.composeConjunction(
                      rexBuilder,
                      conjunctions,
                      false
              ),
              edge.factors,
              combinedColumns);
      edgeMap.put(combined.factors, combined);
    }

    Comparator<Edge> edgeComparator = createComparator(multiJoin,
            vertexes, mq);

    unusedEdges.addAll(edgeMap.values());


    final List<Edge> usedEdges = new ArrayList<>();
    for (;;) {
      final int edgeOrdinal = chooseBestEdge(unusedEdges, edgeComparator);
      if (pw != null) {
        trace(vertexes, unusedEdges, usedEdges, edgeOrdinal, pw);
      }
      final int[] factors;
      if (edgeOrdinal == -1) {
        // No more edges. Are there any un-joined vertexes?
        final Vertex lastVertex = Util.last(vertexes);
        final int z = lastVertex.factors.previousClearBit(lastVertex.id - 1);
        if (z < 0) {
          break;
        }
        factors = new int[] {z, lastVertex.id};
      } else {
        final LoptMultiJoin.Edge bestEdge = unusedEdges.get(edgeOrdinal);

        // For now, assume that the edge is between precisely two factors.
        // 1-factor conditions have probably been pushed down,
        // and 3-or-more-factor conditions are advanced. (TODO:)
        // Therefore, for now, the factors that are merged are exactly the
        // factors on this edge.
        assert bestEdge.factors.cardinality() == 2;
        factors = bestEdge.factors.toArray();
      }

      // Determine which factor is to be on the LHS of the join.
      final int majorFactor;
      final int minorFactor;
      if (vertexes.get(factors[1]).cost <= vertexes.get(factors[0]).cost) {
        majorFactor = factors[0];
        minorFactor = factors[1];
      } else {
        majorFactor = factors[1];
        minorFactor = factors[0];
      }
      final Vertex majorVertex = vertexes.get(majorFactor);
      final Vertex minorVertex = vertexes.get(minorFactor);

      // Find the join conditions. All conditions whose factors are now all in
      // the join can now be used.
      final int v = vertexes.size();
      final ImmutableBitSet newFactors =
          majorVertex.factors
              .rebuild()
              .addAll(minorVertex.factors)
              .set(v)
              .build();

      double rowReduction = 1.0;
      if (edgeOrdinal >= 0) {
        rowReduction = Math.min(rowReduction,
              rowReduction(unusedEdges.get(edgeOrdinal),
              multiJoin, vertexes, false, mq));
      }
      final List<RexNode> conditions = new ArrayList<>();
      final Iterator<LoptMultiJoin.Edge> edgeIterator = unusedEdges.iterator();
      while (edgeIterator.hasNext()) {
        LoptMultiJoin.Edge edge = edgeIterator.next();
        if (newFactors.contains(edge.factors)) {
          conditions.add(edge.condition);
          edgeIterator.remove();
          usedEdges.add(edge);
        }
      }

      double cost =
          Math.max(majorVertex.cost, minorVertex.cost);

      final Vertex newVertex =
          new JoinVertex(v, majorFactor, minorFactor, newFactors,
              cost, ImmutableList.copyOf(conditions),
              majorVertex.rowReduction * rowReduction,
              majorVertex.coreFactor());
      vertexes.add(newVertex);

      // Re-compute selectivity of edges above the one just chosen.
      // Suppose that we just chose the edge between "product" (10k rows) and
      // "product_class" (10 rows).
      // Both of those vertices are now replaced by a new vertex "P-PC".
      // This vertex has fewer rows (1k rows) -- a fact that is critical to
      // decisions made later. (Hence "greedy" algorithm not "simple".)
      // The adjacent edges are modified.
      final ImmutableBitSet merged =
          ImmutableBitSet.of(minorFactor, majorFactor);
      for (int i = 0; i < unusedEdges.size(); i++) {
        final LoptMultiJoin.Edge edge = unusedEdges.get(i);
        if (edge.factors.intersects(merged)) {
          ImmutableBitSet newEdgeFactors =
              edge.factors
                  .rebuild()
                  .removeAll(newFactors)
                  .set(v)
                  .build();
          assert newEdgeFactors.cardinality() == 2;
          final LoptMultiJoin.Edge newEdge =
              new LoptMultiJoin.Edge(edge.condition, newEdgeFactors,
                  edge.columns);
          unusedEdges.set(i, newEdge);
        }
      }
    }

    // We have a winner!
    List<Pair<RelNode, TargetMapping>> relNodes = new ArrayList<>();
    for (Vertex vertex : vertexes) {
      if (vertex instanceof LeafVertex) {
        LeafVertex leafVertex = (LeafVertex) vertex;
        final Mappings.TargetMapping mapping =
            Mappings.offsetSource(
                Mappings.createIdentity(
                    leafVertex.rel.getRowType().getFieldCount()),
                leafVertex.fieldOffset,
                multiJoin.getNumTotalFields());
        relNodes.add(Pair.of(leafVertex.rel, mapping));
      } else {
        JoinVertex joinVertex = (JoinVertex) vertex;
        final Pair<RelNode, Mappings.TargetMapping> leftPair =
            relNodes.get(joinVertex.leftFactor);
        RelNode left = leftPair.left;
        final Mappings.TargetMapping leftMapping = leftPair.right;
        final Pair<RelNode, Mappings.TargetMapping> rightPair =
            relNodes.get(joinVertex.rightFactor);
        RelNode right = rightPair.left;
        final Mappings.TargetMapping rightMapping = rightPair.right;
        final Mappings.TargetMapping mapping =
            Mappings.merge(leftMapping,
                Mappings.offsetTarget(rightMapping,
                    left.getRowType().getFieldCount()));
        if (pw != null) {
          pw.println("left: " + leftMapping);
          pw.println("right: " + rightMapping);
          pw.println("combined: " + mapping);
          pw.println();
        }
        final RexVisitor<RexNode> shuttle =
            new RexPermuteInputsShuttle(mapping, left, right);
        final RexNode condition =
            RexUtil.composeConjunction(rexBuilder, joinVertex.conditions);

        final RelNode join = relBuilder.push(left)
            .push(right)
            .join(JoinRelType.INNER, condition.accept(shuttle))
            .build();
        relNodes.add(Pair.of(join, mapping));
      }
      if (pw != null) {
        pw.println(Util.last(relNodes));
      }
    }

    final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);

    RelNode topJoin = relBuilder.push(top.left)
        .project(relBuilder.fields(top.right))
        .build();

    // remaining conditions are the join conditions that had more than two factors.
    // We couldn't use it to optimize the join ordering, but we can still push them down after
    // the optimization.
    if (!remainingConditions.isEmpty()) {
      PushJoinFiltersShuttle pushJoinFiltersShuttle = new PushJoinFiltersShuttle(relBuilder);

      relBuilder.push(topJoin)
          .filter(RexUtil.composeConjunction(rexBuilder, remainingConditions, false));

      topJoin = relBuilder.build().accept(pushJoinFiltersShuttle);
    }

    relBuilder.push(topJoin)
            .filter(multiJoinRel.getPostJoinFilter());

    call.transformTo(relBuilder.build());
  }

  /**
   * Pushes filters above a join past Joins, Filters and Projects.
   * These pushdown is a very basic level, since the plan should have already been
   * filter-optimized before join optimization.
   */
  private static class PushJoinFiltersShuttle extends RelHomogeneousShuttle {

    private final RelBuilder relBuilder;
    private final RexBuilder rexBuilder;

    PushJoinFiltersShuttle(RelBuilder relBuilder) {
      this.relBuilder = relBuilder;
      this.rexBuilder = relBuilder.getRexBuilder();
    }

    @Override public RelNode visit(RelNode other) {
      if (other instanceof HepRelVertex) {
        other = ((HepRelVertex) other).getCurrentRel();
      }
      if (other instanceof Filter) {
        return visit((Filter) other);
      }
      return other;
    }

    private RelNode visit(Filter filter) {
      RelNode child = filter.getInput();
      if (child instanceof Join) {
        if (((Join) child).getJoinType().isOuterJoin()) {
          return filter;
        }
        return visit((Join) child, filter.getCondition());
      } else if (child instanceof Filter) {
        RelNode grandChild = ((Filter) child).getInput();
        RelNode newChild =
            visit(filter.copy(filter.getTraitSet(), grandChild, filter.getCondition()));
        return child.copy(child.getTraitSet(), List.of(newChild));
      } else if (child instanceof Project) {
        ImmutableBitSet inputFields = RelOptUtil.InputFinder.bits(filter.getCondition());
        List<RexNode> projectExprs = ((Project) child).getProjects();
        for (int i : inputFields) {
          if (!(projectExprs.get(i) instanceof RexInputRef)) {
            return filter;
          }
        }
        return visit((Project) child, filter.getCondition());
      }
      return filter;
    }

    private RelNode visit(Join join, RexNode condition) {
      List<RexNode> joinFilters = new ArrayList<>();
      List<RexNode> leftFilters = new ArrayList<>();
      List<RexNode> rightFilters = new ArrayList<>();
      List<RexNode> topFilters = RelOptUtil.conjunctions(condition);
      RelNode left = join.getLeft();
      RelNode right = join.getRight();

      if (!classifyFilters(join, topFilters, join.getJoinType(),
          true, true, true, joinFilters, leftFilters, rightFilters)) {
        return relBuilder.push(join)
            .filter(condition)
            .build();
      }

      if (!leftFilters.isEmpty()) {
        left = visit(relBuilder.push(left)
            .filter(RexUtil.composeConjunction(rexBuilder, leftFilters, false))
            .build());
      }
      if (!rightFilters.isEmpty()) {
        right = visit(relBuilder.push(right)
            .filter(RexUtil.composeConjunction(rexBuilder, rightFilters, false))
            .build());
      }
      RexNode joinCondition = join.getCondition();
      if (!joinFilters.isEmpty()) {
        joinCondition = RexUtil.composeConjunction(rexBuilder, joinFilters, false);
      }
      RelNode newJoin = join.copy(
          join.getTraitSet(),
          joinCondition,
          left,
          right,
          join.getJoinType(),
          join.isSemiJoinDone());

      if (topFilters.isEmpty()) {
        return newJoin;
      }
      return relBuilder.push(newJoin)
          .filter(RexUtil.composeConjunction(rexBuilder, topFilters, false))
          .build();
    }

    private RelNode visit(Project project, RexNode condition) {
      RexNode newCondition = RelOptUtil.pushPastProject(condition, project);
      RelNode newChild = visit(relBuilder.push(project.getInput())
          .filter(newCondition)
          .build());

      return project.copy(project.getTraitSet(), List.of(newChild));
    }
  }

  /**
   * Create the comparator used to decide which is best next edge to merge.
   * The basic heuristic for determining which edges are best:
   * 1. Edge condition is on a primary key or distinct key
   *   a. For edges that satisfy 1, prefer edges where smaller input
   *     has row reduction, as this will probably be a filtering join
   *     condition
   * 2. Edge condition does not indicate a self join
   * 3. Condition is not on "fanout" column of the larger Vertex
   * 4. Prefer non-PrimaryKey/Distinct edges with row reduction,
   *   as it's possible that it's actually a primary key, but we
   *   just don't have the statistics/information to know for sure
   * 5. Finally, for edges that satisfy none of the above, prefer
   *   the edges where ratio of rowcount is the largest
   */
  protected Comparator<Edge> createComparator(LoptMultiJoin multiJoin,
                                              List<Vertex> vertexes,
                                              RelMetadataQuery mq) {
    return new Comparator<LoptMultiJoin.Edge>() {
      public int compare(LoptMultiJoin.Edge e0, LoptMultiJoin.Edge e1) {
        int comp = -Double.compare(
                rowReduction(e0, multiJoin, vertexes, true, mq),
                rowReduction(e1, multiJoin, vertexes, true, mq));
        if (comp != 0) {
          return comp;
        }
        comp = Integer.compare(selfJoin(e0, multiJoin, mq),
                selfJoin(e1, multiJoin, mq));
        if (comp != 0) {
          return comp;
        }
        comp = Integer.compare(hasFanout(e0, multiJoin, vertexes),
                hasFanout(e1, multiJoin, vertexes));
        if (comp != 0) {
          return comp;
        }
        comp = -Double.compare(
                rowReduction(e0, multiJoin, vertexes, false, mq),
                rowReduction(e1, multiJoin, vertexes, false, mq));
        if (comp != 0) {
          return comp;
        }
        return Double.compare(rowCountRatio(e0), rowCountRatio(e1));
      }


      private double rowCountRatio(LoptMultiJoin.Edge edge) {
        assert edge.factors.cardinality() == 2 : edge.factors;
        final int factor0 = edge.factors.nextSetBit(0);
        final int factor1 = edge.factors.nextSetBit(factor0 + 1);
        double ratio = vertexes.get(factor0).cost
                / vertexes.get(factor1).cost;
        return Math.max(ratio, 1 / ratio);
      }
    };
  }

  /**
   * if the column origin of join conditions refer to the same
   * underlying table, this is considered a self join. These
   * are often many-to-many joins, and thus it is safest
   * to de-prioritize these joins
   */
  public static int selfJoin(Edge edge,
                             LoptMultiJoin multiJoin,
                             RelMetadataQuery mq) {
    Map<Integer, ImmutableBitSet> factorRefMap =
            edge.getFactorRefMap(multiJoin);

    Set<RelOptTable> refTables = new HashSet<>();
    for (int oFactor : factorRefMap.keySet()) {
      RelNode rel = multiJoin.getJoinFactor(oFactor);
      Set<RelOptTable> set = factorRefMap.get(oFactor).asList().stream()
              .map(c -> mq.getColumnOrigins(rel, c))
              .filter(Objects::nonNull)
              .flatMap(Collection::stream)
              .map(RelColumnOrigin::getOriginTable)
              .collect(Collectors.toSet());
      if (intersects(refTables, set)) {
        return 0;
      }
      refTables.addAll(set);
    }
    return 1;
  }

  private static <T> boolean intersects(Set<T> set1, Set<T> set2) {
    Set<T> tmp = new HashSet<>(set1);
    tmp.retainAll(set2);
    return !tmp.isEmpty();
  }

  /**
   * When a smaller table joins with a larger table, if we assume the larger table is a fact
   * table and the smaller is a dimension table, and the join key of the dimension is a primary
   * key, the columns of the smaller dimension table will have "fanout", i.e. duplicated values
   * after the join. If we then add another join that includes a join on a column from the "fanout"
   * table, there is a high potential of this being a many-to-many join, which we should
   * deprioritize
   */
  public static int hasFanout(Edge edge,
                                  LoptMultiJoin multiJoin,
                                  List<Vertex> vertexes) {
    Map<Integer, ImmutableBitSet> factorRefMap =
            edge.getFactorRefMap(multiJoin);
    Set<Integer> coreFactors = edge.factors.asList().stream()
            .map(vertexes::get)
            .map(Vertex::coreFactor)
            .collect(Collectors.toSet());
    return factorRefMap.keySet().stream().anyMatch(f -> !coreFactors.contains(f))
            ? 0 : 1;
  }

  /**
   * Return the underlying factors from this edge
   * where the condition is on a primary key or
   * distinct column
   */
  private static Set<Integer> distinctFactors(LoptMultiJoin.Edge edge,
                         LoptMultiJoin multiJoin,
                         List<Vertex> vertexes,
                         RelMetadataQuery mq) {
    Set<Integer> distinctFactors = new HashSet<>();
    Map<Integer, Integer> oFactorFactorMap = new HashMap<>();
    for (int factor : edge.factors) {
      Vertex v = vertexes.get(factor);
      for (int oFactor : v.factors) {
        oFactorFactorMap.put(oFactor, factor);
      }
    }

    Map<Integer, ImmutableBitSet> factorRefMap =
            edge.getFactorRefMap(multiJoin);

    Set<Integer> coreFactors = edge.factors.asList().stream()
            .map(vertexes::get)
            .map(Vertex::coreFactor)
            .collect(Collectors.toSet());

    for (int oFactor : factorRefMap.keySet()) {
      ImmutableBitSet factorRefs = factorRefMap.get(oFactor);
      boolean unique = isKey(multiJoin.getJoinFactor(oFactor),
              factorRefs, mq)
              && coreFactors.contains(oFactor);
      unique |= Optional.ofNullable(
                mq.areColumnsUnique(multiJoin.getJoinFactor(oFactor),
                factorRefs, true))
              .orElse(false);
      if (unique) {
        Integer factor = oFactorFactorMap.get(oFactor);
        distinctFactors.add(factor);
      }
    }
    return distinctFactors;
  }

  /**
   * Assume the larger vertex contains the fact table. If
   * there is row reduction on the smaller side, we can
   * treat this is a filtering join
   */
  private static Double rowReduction(LoptMultiJoin.Edge edge,
                                     LoptMultiJoin multiJoin,
                                     List<Vertex> vertexes,
                                     boolean requireDistinct,
                                     RelMetadataQuery mq) {
    if (!edge.condition.isA(EQUI_JOIN)) {
      return Double.POSITIVE_INFINITY;
    }
    if (edge.condition.isA(SqlKind.AND)) {
      if (RelOptUtil.conjunctions(edge.condition).stream()
              .noneMatch(c -> c.isA(EQUALITY))
      ) {
        return Double.POSITIVE_INFINITY;
      }
    }
    Set<Integer> distinctFactors = distinctFactors(edge, multiJoin, vertexes, mq);
    if (requireDistinct && distinctFactors.isEmpty()) {
      return Double.POSITIVE_INFINITY;
    }

    double reduction = 1.0;
    if (requireDistinct) {
      for (int factor : edge.factors) {
        if (distinctFactors.contains(factor)) {
          double newReduction = vertexes.get(factor).rowReduction;
          reduction = Math.min(reduction, newReduction);
        }
      }
    } else {
      double minCost = edge.factors.asList()
              .stream()
              .mapToDouble(f -> vertexes.get(f).cost)
              .min()
              .orElse(0);
      for (int factor : edge.factors) {
        Vertex v = vertexes.get(factor);
        if (isEqWithEpsilon(minCost, v.cost)) {
          reduction = Math.min(reduction, v.rowReduction);
        }
      }
    }

    return reduction < 1.0 ? 0.5 : 1.0;
  }

  public static boolean isEqWithEpsilon(double d1, double d2) {
    return Math.abs(d1 - d2) < RelOptUtil.EPSILON;
  }
  public static boolean isKey(RelNode dim, ImmutableBitSet dimKeys, RelMetadataQuery mq) {
    List<RelColumnOrigin> columnOrigins =
            dimKeys.asList().stream()
                    .map(k -> mq.getColumnOrigin(dim, k))
                    .filter(Objects::nonNull).collect(Collectors.toList());
    if (columnOrigins.size() != dimKeys.cardinality()) {
      return false;
    }
    Set<RelOptTable> tables = columnOrigins.stream()
            .filter(Objects::nonNull)
            .map(RelColumnOrigin::getOriginTable)
            .collect(Collectors.toSet());
    if (tables.size() != 1) {
      return false;
    }
    RelOptTable table = tables.iterator().next();
    ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
    columnOrigins.forEach(o -> keyBuilder.set(o.getOriginColumnOrdinal()));
    return table != null && table.isKey(keyBuilder.build());
  }

  private void trace(List<Vertex> vertexes,
      List<LoptMultiJoin.Edge> unusedEdges, List<LoptMultiJoin.Edge> usedEdges,
      int edgeOrdinal, PrintWriter pw) {
    pw.println("bestEdge: " + edgeOrdinal);
    pw.println("vertexes:");
    for (Vertex vertex : vertexes) {
      pw.println(vertex);
    }
    pw.println("unused edges:");
    for (LoptMultiJoin.Edge edge : unusedEdges) {
      pw.println(edge);
    }
    pw.println("edges:");
    for (LoptMultiJoin.Edge edge : usedEdges) {
      pw.println(edge);
    }
    pw.println();
    pw.flush();
  }

  int chooseBestEdge(List<LoptMultiJoin.Edge> edges,
      Comparator<LoptMultiJoin.Edge> comparator) {
    return maxPos(edges, comparator);
  }

  /** Returns the index within a list at which compares least according to a
   * comparator.
   *
   * <p>In the case of a tie, returns the earliest such element.</p>
   *
   * <p>If the list is empty, returns -1.</p>
   */
  static <E> int maxPos(List<E> list, Comparator<E> fn) {
    if (list.isEmpty()) {
      return -1;
    }
    E eBest = list.get(0);
    int iBest = 0;
    for (int i = 1; i < list.size(); i++) {
      E e = list.get(i);
      if (fn.compare(e, eBest) > 0) {
        eBest = e;
        iBest = i;
      }
    }
    return iBest;
  }

  /** Participant in a join (relation or join). */
  abstract static class Vertex {
    final int id;

    protected final ImmutableBitSet factors;
    final double cost;

    final double rowReduction;

    Vertex(int id, ImmutableBitSet factors, double cost, double rowReduction) {
      this.id = id;
      this.factors = factors;
      this.cost = cost;
      this.rowReduction = rowReduction;
    }

    int coreFactor() {
      return id;
    }
  }

  /** Relation participating in a join. */
  static class LeafVertex extends Vertex {
    private final RelNode rel;
    final int fieldOffset;

    LeafVertex(int id, RelNode rel, double cost, int fieldOffset, double rowReduction) {
      super(id, ImmutableBitSet.of(id), cost, rowReduction);
      this.rel = rel;
      this.fieldOffset = fieldOffset;
    }

    @Override public String toString() {
      return "LeafVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", fieldOffset: " + fieldOffset
          + ", rowReduceCount: " + rowReduction
          + ")";
    }
  }

  /** Participant in a join which is itself a join. */
  static class JoinVertex extends Vertex {
    private final int leftFactor;
    private final int rightFactor;
    /** Zero or more join conditions. All are in terms of the original input
     * columns (not in terms of the outputs of left and right input factors). */

    private final int coreFactor;
    final ImmutableList<RexNode> conditions;

    JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet factors,
        double cost, ImmutableList<RexNode> conditions, double rowReduction, int coreFactor) {
      super(id, factors, cost, rowReduction);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.conditions = Objects.requireNonNull(conditions);
      this.coreFactor = coreFactor;
    }

    @Override int coreFactor() {
      return coreFactor;
    }

    @Override public String toString() {
      return "JoinVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", leftFactor: " + leftFactor
          + ", rightFactor: " + rightFactor
          + ", rowReduceCount: " + rowReduction
          + ")";
    }
  }
<<<<<<< HEAD
=======

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
            .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
            .as(Config.class);

    @Override default MultiJoinOptimizeBushyRule toRule() {
      return new MultiJoinOptimizeBushyRule(this);
    }
  }
}

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
            .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
            .as(Config.class);

    @Override default MultiJoinOptimizeBushyRule toRule() {
      return new MultiJoinOptimizeBushyRule(this);
    }
  }
}
