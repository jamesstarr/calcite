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
package org.apache.calcite.sql2rel.decorrelator;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Holder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SortedSetMultimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A map of the locations of
 * {@link org.apache.calcite.rel.core.Correlate}
 * in a tree of {@link RelNode}s.
 *
 * <p>It is used to drive the decorrelation process.
 * Treat it as immutable; rebuild if you modify the tree.
 *
 * <p>There are three maps:<ol>
 *
 * <li>{@link #mapRefRelToCorRef} maps a {@link RelNode} to the correlated
 * variables it references;
 *
 * <li>{@link #mapCorToCorRel} maps a correlated variable to the
 * {@link Correlate} providing it;
 *
 * <li>{@link #mapFieldAccessToCorRef} maps a rex field access to
 * the corVar it represents. Because typeFlattener does not clone or
 * modify a correlated field access this map does not need to be
 * updated.
 *
 * </ol>
 */
class CorelMap {
  final Multimap<RelNode, RelDecorrelator.CorRef> mapRefRelToCorRef;
  final NavigableMap<CorrelationId, RelNode> mapCorToCorRel;
  final Map<RexFieldAccess, RelDecorrelator.CorRef> mapFieldAccessToCorRef;

  // TODO: create immutable copies of all maps
  CorelMap(Multimap<RelNode, RelDecorrelator.CorRef> mapRefRelToCorRef,
      NavigableMap<CorrelationId, RelNode> mapCorToCorRel,
      Map<RexFieldAccess, RelDecorrelator.CorRef> mapFieldAccessToCorRef) {
    this.mapRefRelToCorRef = mapRefRelToCorRef;
    this.mapCorToCorRel = mapCorToCorRel;
    this.mapFieldAccessToCorRef = ImmutableMap.copyOf(mapFieldAccessToCorRef);
  }

  @Override public String toString() {
    return "mapRefRelToCorRef=" + mapRefRelToCorRef
        + "\nmapCorToCorRel=" + mapCorToCorRel
        + "\nmapFieldAccessToCorRef=" + mapFieldAccessToCorRef
        + "\n";
  }

  @SuppressWarnings("UndefinedEquals")
  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof CorelMap
        // TODO: Multimap does not have well-defined equals behavior
        && mapRefRelToCorRef.equals(((CorelMap) obj).mapRefRelToCorRef)
        && mapCorToCorRel.equals(((CorelMap) obj).mapCorToCorRel)
        && mapFieldAccessToCorRef.equals(
        ((CorelMap) obj).mapFieldAccessToCorRef);
  }

  @Override public int hashCode() {
    return Objects.hash(mapRefRelToCorRef, mapCorToCorRel,
        mapFieldAccessToCorRef);
  }

  NavigableMap<CorrelationId, RelNode> getMapCorToCorRel() {
    return mapCorToCorRel;
  }

  /**
   * Returns whether there are any correlating variables in this statement.
   *
   * @return whether there are any correlating variables
   */
  boolean hasCorrelation() {
    return !mapCorToCorRel.isEmpty();
  }

  static CorelMap build(RelNode...relNodes) {
    return new CorelMapBuilder().build(relNodes);
  }

  /** Builds a {@link CorelMap}. */
  private static class CorelMapBuilder extends RelHomogeneousShuttle {
    private final NavigableMap<CorrelationId, RelNode> mapCorToCorRel =
        new TreeMap<>();

    final SortedSetMultimap<RelNode, RelDecorrelator.CorRef> mapRefRelToCorRef =
        MultimapBuilder.SortedSetMultimapBuilder.hashKeys()
            .treeSetValues()
            .build();

    final Map<RexFieldAccess, RelDecorrelator.CorRef> mapFieldAccessToCorVar = new HashMap<>();

    final Holder<Integer> offset = Holder.of(0);
    int corrIdGenerator = 0;

    /** Creates a CorelMap by iterating over a {@link RelNode} tree. */
    public CorelMap build(RelNode... rels) {
      for (RelNode rel : rels) {
        stripHep(rel).accept(this);
      }
      return new CorelMap(mapRefRelToCorRef, mapCorToCorRel,
          mapFieldAccessToCorVar);
    }

    @Override public RelNode visit(RelNode other) {
      if (other instanceof Join) {
        Join join = (Join) other;
        try {
          stack.push(join);
          join.getCondition().accept(rexVisitor(join));
        } finally {
          stack.pop();
        }
        return visitJoin(join);
      } else if (other instanceof Correlate) {
        Correlate correlate = (Correlate) other;
        mapCorToCorRel.put(correlate.getCorrelationId(), correlate);
        return visitJoin(correlate);
      } else if (other instanceof Filter) {
        Filter filter = (Filter) other;
        try {
          stack.push(filter);
          filter.getCondition().accept(rexVisitor(filter));
        } finally {
          stack.pop();
        }
      } else if (other instanceof Project) {
        Project project = (Project) other;
        try {
          stack.push(project);
          for (RexNode node : project.getProjects()) {
            node.accept(rexVisitor(project));
          }
        } finally {
          stack.pop();
        }
      }
      return super.visit(other);
    }

    @Override protected RelNode visitChild(RelNode parent, int i,
        RelNode input) {
      return super.visitChild(parent, i, stripHep(input));
    }

    private RelNode visitJoin(BiRel join) {
      final int x = offset.get();
      visitChild(join, 0, join.getLeft());
      offset.set(x + join.getLeft().getRowType().getFieldCount());
      visitChild(join, 1, join.getRight());
      offset.set(x);
      return join;
    }

    private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
      return new RexVisitorImpl<Void>(true) {
        @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
          final RexNode ref = fieldAccess.getReferenceExpr();
          if (ref instanceof RexCorrelVariable) {
            final RexCorrelVariable var = (RexCorrelVariable) ref;
            if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
              // for cases where different Rel nodes are referring to
              // same correlation var (e.g. in case of NOT IN)
              // avoid generating another correlation var
              // and record the 'rel' is using the same correlation
              mapRefRelToCorRef.put(rel,
                  mapFieldAccessToCorVar.get(fieldAccess));
            } else {
              final RelDecorrelator.CorRef correlation =
                  new RelDecorrelator.CorRef(var.id, fieldAccess.getField().getIndex(),
                      corrIdGenerator++);
              mapFieldAccessToCorVar.put(fieldAccess, correlation);
              mapRefRelToCorRef.put(rel, correlation);
            }
          }
          return super.visitFieldAccess(fieldAccess);
        }

        @Override public Void visitSubQuery(RexSubQuery subQuery) {
          subQuery.rel.accept(CorelMapBuilder.this);
          return super.visitSubQuery(subQuery);
        }
      };
    }


    private static RelNode stripHep(RelNode rel) {
      if (rel instanceof HepRelVertex) {
        HepRelVertex hepRelVertex = (HepRelVertex) rel;
        rel = hepRelVertex.getCurrentRel();
      }
      return rel;
    }
  }


}
