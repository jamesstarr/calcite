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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RelMetadataQuery provides a strongly-typed facade on top of
 * {@link RelMetadataProvider} for the set of relational expression metadata
 * queries defined as standard within Calcite. The Javadoc on these methods
 * serves as their primary specification.
 *
 * <p>To add a new standard query <code>Xyz</code> to this interface, follow
 * these steps:
 *
 * <ol>
 * <li>Add a static method <code>getXyz</code> specification to this class.
 * <li>Add unit tests to {@code org.apache.calcite.test.RelMetadataTest}.
 * <li>Write a new provider class <code>RelMdXyz</code> in this package. Follow
 * the pattern from an existing class such as {@link RelMdColumnOrigins},
 * overloading on all of the logical relational expressions to which the query
 * applies.
 * <li>Add a {@code SOURCE} static member, similar to
 *     {@link RelMdColumnOrigins#SOURCE}.
 * <li>Register the {@code SOURCE} object in {@link DefaultRelMetadataProvider}.
 * <li>Get unit tests working.
 * </ol>
 *
 * <p>Because relational expression metadata is extensible, extension projects
 * can define similar facades in order to specify access to custom metadata.
 * Please do not add queries here (nor on {@link RelNode}) which lack meaning
 * outside of your extension.
 *
 * <p>Besides adding new metadata queries, extension projects may need to add
 * custom providers for the standard queries in order to handle additional
 * relational expressions (either logical or physical). In either case, the
 * process is the same: write a reflective provider and chain it on to an
 * instance of {@link DefaultRelMetadataProvider}, pre-pending it to the default
 * providers. Then supply that instance to the planner via the appropriate
 * plugin mechanism.
 */
public class RelMetadataQuery {

  /** Invocation handler to create {@code Metadata} instances guarding for
   * cycles and managing local cache
   */
  private final class MetadataInvocationHandler implements InvocationHandler {
    private final Metadata metadata;

    private MetadataInvocationHandler(Metadata metadata) {
      this.metadata = metadata;
    }

    @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // Skip cycle check/caching for metadata.rel() and metadata.toString()
      if (BuiltInMethod.METADATA_REL.method.equals(method)
          || BuiltInMethod.OBJECT_TO_STRING.method.equals(method)) {
        return method.invoke(metadata, args);
      }

      final List<?> key = newKey(method, args);
      final Object cached = visited.putIfAbsent(key, NullSentinel.ACTIVE);
      if (cached == NullSentinel.ACTIVE) {
        throw CyclicMetadataException.INSTANCE;
      }

      // Execute underlying metadata method
      try {
        final Object result = method.invoke(metadata, args);
        return result;
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      } finally {
        visited.remove(key);
      }
    }

    private List<?> newKey(Method method, Object... args) {
      final RelNode rel = metadata.rel();
      final Object[] safeArgs = args != null ? args : new Object[0];
      final int length = safeArgs.length;
      switch (length) {
      case 4:
        return FlatLists.of(rel, method, mask(safeArgs[0]), mask(safeArgs[1]), mask(safeArgs[2]),
            mask(safeArgs[3]));

      case 3:
        return FlatLists.of(rel, method, mask(safeArgs[0]), mask(safeArgs[1]), mask(safeArgs[2]));

      case 2:
        return FlatLists.of(rel, method, mask(safeArgs[0]), mask(safeArgs[1]));

      case 1:
        return FlatLists.of(rel, method, mask(safeArgs[0]));

      case 0:
        return FlatLists.of(rel, method);

      default:
        Object[] key = new Object[2 + length];
        key[0] = rel;
        key[1] = method;
        for (int i = 0; i < length; i++) {
          key[i + 2] = mask(safeArgs[i]);
        }
        return Collections.unmodifiableList(Arrays.asList(key));
      }
    }

    private Object mask(Object value) {
      // Can't use RexNode.equals - it is not deep
      if (value instanceof RexNode) {
        return value.toString();
      }
      return NullSentinel.mask(value);
    }
  }

  /** Set of active metadata queries, and cache of previous results. */
  private final Map<List<?>, Object> visited = new HashMap<>();

  public RelMetadataQuery() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns an instance of RelMetadataQuery. It ensures that cycles do not
   * occur while computing metadata.
   */

  public static RelMetadataQuery instance() {
    return new RelMetadataQuery();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.NodeTypes#getNodeTypes()}
   * statistic.
   *
   * @param rel the relational expression
   */
  public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel) {
    final BuiltInMetadata.NodeTypes metadata =
        metadata(rel, BuiltInMetadata.NodeTypes.class);
    return metadata.getNodeTypes();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.RowCount#getRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated row count, or null if no reliable estimate can be
   * determined
   */
  public Double getRowCount(RelNode rel) {
    final BuiltInMetadata.RowCount metadata =
        metadata(rel, BuiltInMetadata.RowCount.class);
    Double result = metadata.getRowCount();
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.MaxRowCount#getMaxRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return max row count
   */
  public Double getMaxRowCount(RelNode rel) {
    final BuiltInMetadata.MaxRowCount metadata =
        metadata(rel, BuiltInMetadata.MaxRowCount.class);
    return metadata.getMaxRowCount();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.MinRowCount#getMinRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return max row count
   */
  public Double getMinRowCount(RelNode rel) {
    final BuiltInMetadata.MinRowCount metadata =
        metadata(rel, BuiltInMetadata.MinRowCount.class);
    return metadata.getMinRowCount();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.CumulativeCost#getCumulativeCost()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated cost, or null if no reliable estimate can be determined
   */
  public RelOptCost getCumulativeCost(RelNode rel) {
    final BuiltInMetadata.CumulativeCost metadata =
        metadata(rel, BuiltInMetadata.CumulativeCost.class);
    return metadata.getCumulativeCost();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.NonCumulativeCost#getNonCumulativeCost()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated cost, or null if no reliable estimate can be determined
   */
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    final BuiltInMetadata.NonCumulativeCost metadata =
        metadata(rel, BuiltInMetadata.NonCumulativeCost.class);
    return metadata.getNonCumulativeCost();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.PercentageOriginalRows#getPercentageOriginalRows()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated percentage (between 0.0 and 1.0), or null if no
   * reliable estimate can be determined
   */
  public Double getPercentageOriginalRows(RelNode rel) {
    final BuiltInMetadata.PercentageOriginalRows metadata =
        metadata(rel, BuiltInMetadata.PercentageOriginalRows.class);
    Double result = metadata.getPercentageOriginalRows();
    assert isPercentage(result, true);
    return result;
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnOrigin#getColumnOrigins(int)}
   * statistic.
   *
   * @param rel           the relational expression
   * @param column 0-based ordinal for output column of interest
   * @return set of origin columns, or null if this information cannot be
   * determined (whereas empty set indicates definitely no origin columns at
   * all)
   */
  public Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column) {
    final BuiltInMetadata.ColumnOrigin metadata =
        metadata(rel, BuiltInMetadata.ColumnOrigin.class);
    return metadata.getColumnOrigins(column);
  }

  /**
   * Determines the origin of a column, provided the column maps to a single
   * column that isn't derived.
   *
   * @see #getColumnOrigins(org.apache.calcite.rel.RelNode, int)
   *
   * @param rel the RelNode of the column
   * @param column the offset of the column whose origin we are trying to
   * determine
   *
   * @return the origin of a column provided it's a simple column; otherwise,
   * returns null
   */
  public RelColumnOrigin getColumnOrigin(RelNode rel, int column) {
    final Set<RelColumnOrigin> origins = getColumnOrigins(rel, column);
    if (origins == null || origins.size() != 1) {
      return null;
    }
    final RelColumnOrigin origin = Iterables.getOnlyElement(origins);
    return origin.isDerived() ? null : origin;
  }

  /**
   * Determines the origin of a column.
   */
  public Set<RexNode> getExpressionLineage(RelNode rel, RexNode expression) {
    final BuiltInMetadata.ExpressionLineage metadata =
        metadata(rel, BuiltInMetadata.ExpressionLineage.class);
    return metadata.getExpressionLineage(expression);
  }

  /**
   * Determines the tables used by a plan.
   */
  public Set<RelTableRef> getTableReferences(RelNode rel) {
    final BuiltInMetadata.TableReferences metadata =
        metadata(rel, BuiltInMetadata.TableReferences.class);
    return metadata.getTableReferences();
  }

  /**
   * Determines the origin of a {@link RelNode}, provided it maps to a single
   * table, optionally with filtering and projection.
   *
   * @param rel the RelNode
   *
   * @return the table, if the RelNode is a simple table; otherwise null
   */
  public RelOptTable getTableOrigin(RelNode rel) {
    // Determine the simple origin of the first column in the
    // RelNode.  If it's simple, then that means that the underlying
    // table is also simple, even if the column itself is derived.
    if (rel.getRowType().getFieldCount() == 0) {
      return null;
    }
    final Set<RelColumnOrigin> colOrigins = getColumnOrigins(rel, 0);
    if (colOrigins == null || colOrigins.size() == 0) {
      return null;
    }
    return colOrigins.iterator().next().getOriginTable();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Selectivity#getSelectivity(RexNode)}
   * statistic.
   *
   * @param rel       the relational expression
   * @param predicate predicate whose selectivity is to be estimated against
   *                  {@code rel}'s output
   * @return estimated selectivity (between 0.0 and 1.0), or null if no
   * reliable estimate can be determined
   */
  public Double getSelectivity(RelNode rel, RexNode predicate) {
    final BuiltInMetadata.Selectivity metadata =
        metadata(rel, BuiltInMetadata.Selectivity.class);
    Double result = metadata.getSelectivity(predicate);
    assert isPercentage(result, true);
    return result;
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.UniqueKeys#getUniqueKeys(boolean)}
   * statistic.
   *
   * @param rel the relational expression
   * @return set of keys, or null if this information cannot be determined
   * (whereas empty set indicates definitely no keys at all)
   */
  public Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    return getUniqueKeys(rel, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.UniqueKeys#getUniqueKeys(boolean)}
   * statistic.
   *
   * @param rel         the relational expression
   * @param ignoreNulls if true, ignore null values when determining
   *                    whether the keys are unique
   *
   * @return set of keys, or null if this information cannot be determined
   * (whereas empty set indicates definitely no keys at all)
   */
  public Set<ImmutableBitSet> getUniqueKeys(RelNode rel,
      boolean ignoreNulls) {
    final BuiltInMetadata.UniqueKeys metadata =
        metadata(rel, BuiltInMetadata.UniqueKeys.class);
    return metadata.getUniqueKeys(false);
  }

  /**
   * Returns whether the rows of a given relational expression are distinct.
   * This is derived by applying the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(org.apache.calcite.util.ImmutableBitSet, boolean)}
   * statistic over all columns.
   *
   * @param rel     the relational expression
   *
   * @return true or false depending on whether the rows are unique, or
   * null if not enough information is available to make that determination
   */
  public Boolean areRowsUnique(RelNode rel) {
    final ImmutableBitSet columns =
        ImmutableBitSet.range(rel.getRowType().getFieldCount());
    return areColumnsUnique(rel, columns, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)}
   * statistic.
   *
   * @param rel     the relational expression
   * @param columns column mask representing the subset of columns for which
   *                uniqueness will be determined
   *
   * @return true or false depending on whether the columns are unique, or
   * null if not enough information is available to make that determination
   */
  public Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns) {
    return areColumnsUnique(rel, columns, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)}
   * statistic.
   *
   * @param rel         the relational expression
   * @param columns     column mask representing the subset of columns for which
   *                    uniqueness will be determined
   * @param ignoreNulls if true, ignore null values when determining column
   *                    uniqueness
   * @return true or false depending on whether the columns are unique, or
   * null if not enough information is available to make that determination
   */
  public Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns,
      boolean ignoreNulls) {
    final BuiltInMetadata.ColumnUniqueness metadata =
        metadata(rel, BuiltInMetadata.ColumnUniqueness.class);
    return metadata.areColumnsUnique(columns, ignoreNulls);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Collation#collations()}
   * statistic.
   *
   * @param rel         the relational expression
   * @return List of sorted column combinations, or
   * null if not enough information is available to make that determination
   */
  public ImmutableList<RelCollation> collations(RelNode rel) {
    final BuiltInMetadata.Collation metadata =
        metadata(rel, BuiltInMetadata.Collation.class);
    return metadata.collations();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Distribution#distribution()}
   * statistic.
   *
   * @param rel         the relational expression
   * @return List of sorted column combinations, or
   * null if not enough information is available to make that determination
   */
  public RelDistribution distribution(RelNode rel) {
    final BuiltInMetadata.Distribution metadata =
        metadata(rel, BuiltInMetadata.Distribution.class);
    return metadata.distribution();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.PopulationSize#getPopulationSize(ImmutableBitSet)}
   * statistic.
   *
   * @param rel      the relational expression
   * @param groupKey column mask representing the subset of columns for which
   *                 the row count will be determined
   * @return distinct row count for the given groupKey, or null if no reliable
   * estimate can be determined
   *
   */
  public Double getPopulationSize(RelNode rel,
      ImmutableBitSet groupKey) {
    final BuiltInMetadata.PopulationSize metadata =
        metadata(rel, BuiltInMetadata.PopulationSize.class);
    Double result = metadata.getPopulationSize(groupKey);
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Size#averageRowSize()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return average size of a row, in bytes, or null if not known
     */
  public Double getAverageRowSize(RelNode rel) {
    final BuiltInMetadata.Size metadata =
        metadata(rel, BuiltInMetadata.Size.class);
    return metadata.averageRowSize();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Size#averageColumnSizes()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return a list containing, for each column, the average size of a column
   * value, in bytes. Each value or the entire list may be null if the
   * metadata is not available
   */
  public List<Double> getAverageColumnSizes(RelNode rel) {
    final BuiltInMetadata.Size metadata =
        metadata(rel, BuiltInMetadata.Size.class);
    return metadata.averageColumnSizes();
  }

  /** As {@link #getAverageColumnSizes(org.apache.calcite.rel.RelNode)} but
   * never returns a null list, only ever a list of nulls. */
  public List<Double> getAverageColumnSizesNotNull(RelNode rel) {
    final List<Double> averageColumnSizes = getAverageColumnSizes(rel);
    return averageColumnSizes == null
        ? Collections.<Double>nCopies(rel.getRowType().getFieldCount(), null)
        : averageColumnSizes;
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Parallelism#isPhaseTransition()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return whether each physical operator implementing this relational
   * expression belongs to a different process than its inputs, or null if not
   * known
   */
  public Boolean isPhaseTransition(RelNode rel) {
    final BuiltInMetadata.Parallelism metadata =
        metadata(rel, BuiltInMetadata.Parallelism.class);
    return metadata.isPhaseTransition();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Parallelism#splitCount()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the number of distinct splits of the data, or null if not known
   */
  public Integer splitCount(RelNode rel) {
    final BuiltInMetadata.Parallelism metadata =
        metadata(rel, BuiltInMetadata.Parallelism.class);
    return metadata.splitCount();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Memory#memory()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the expected amount of memory, in bytes, required by a physical
   * operator implementing this relational expression, across all splits,
   * or null if not known
   */
  public Double memory(RelNode rel) {
    final BuiltInMetadata.Memory metadata =
        metadata(rel, BuiltInMetadata.Memory.class);
    return metadata.memory();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhase()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the cumulative amount of memory, in bytes, required by the
   * physical operator implementing this relational expression, and all other
   * operators within the same phase, across all splits, or null if not known
   */
  public Double cumulativeMemoryWithinPhase(RelNode rel) {
    final BuiltInMetadata.Memory metadata =
        metadata(rel, BuiltInMetadata.Memory.class);
    return metadata.cumulativeMemoryWithinPhase();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhaseSplit()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the expected cumulative amount of memory, in bytes, required by
   * the physical operator implementing this relational expression, and all
   * operators within the same phase, within each split, or null if not known
   */
  public Double cumulativeMemoryWithinPhaseSplit(RelNode rel) {
    final BuiltInMetadata.Memory metadata =
        metadata(rel, BuiltInMetadata.Memory.class);
    return metadata.cumulativeMemoryWithinPhaseSplit();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.DistinctRowCount#getDistinctRowCount(ImmutableBitSet, RexNode)}
   * statistic.
   *
   * @param rel       the relational expression
   * @param groupKey  column mask representing group by columns
   * @param predicate pre-filtered predicates
   * @return distinct row count for groupKey, filtered by predicate, or null
   * if no reliable estimate can be determined
   */
  public Double getDistinctRowCount(
      RelNode rel,
      ImmutableBitSet groupKey,
      RexNode predicate) {
    final BuiltInMetadata.DistinctRowCount metadata =
        metadata(rel, BuiltInMetadata.DistinctRowCount.class);
    Double result = metadata.getDistinctRowCount(groupKey, predicate);
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Predicates#getPredicates()}
   * statistic.
   *
   * @param rel the relational expression
   * @return Predicates that can be pulled above this RelNode
   */
  public RelOptPredicateList getPulledUpPredicates(RelNode rel) {
    final BuiltInMetadata.Predicates metadata =
        metadata(rel, BuiltInMetadata.Predicates.class);
    return metadata.getPredicates();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.AllPredicates#getAllPredicates()}
   * statistic.
   *
   * @param rel the relational expression
   * @return All predicates within and below this RelNode
   */
  public RelOptPredicateList getAllPredicates(RelNode rel) {
    final BuiltInMetadata.AllPredicates metadata =
        metadata(rel, BuiltInMetadata.AllPredicates.class);
    return metadata.getAllPredicates();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ExplainVisibility#isVisibleInExplain(SqlExplainLevel)}
   * statistic.
   *
   * @param rel          the relational expression
   * @param explainLevel level of detail
   * @return true for visible, false for invisible; if no metadata is available,
   * defaults to true
   */
  public boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel) {
    final BuiltInMetadata.ExplainVisibility metadata =
        metadata(rel, BuiltInMetadata.ExplainVisibility.class);
    Boolean b = metadata.isVisibleInExplain(explainLevel);
    return b == null || b;
  }

  /**
   * Get a metadata instance guarding for cycles
   *
   * @param rel the relational expression
   * @param metadataClass the metadata class
   * @return a metadata instance guarding for cycles
   */
  protected <M extends Metadata> M metadata(RelNode rel,
      Class<? extends M> metadataClass) {
    Metadata metadata = rel.metadata(metadataClass, this);
    Preconditions.checkArgument(metadata != null,
        "no provider found (rel=%s, m=%s); a backstop provider is recommended",
        rel,
        metadataClass);
    return (M) Proxy.newProxyInstance(
        metadataClass.getClassLoader(),
        new Class[] { metadataClass },
        new MetadataInvocationHandler(metadata));
  }

  protected static Double validatePercentage(Double result) {
    assert isPercentage(result, true);
    return result;
  }

  protected static boolean isPercentage(Double result, boolean fail) {
    if (result != null) {
      final double d = result;
      if (d < 0.0) {
        assert !fail;
        return false;
      }
      if (d > 1.0) {
        assert !fail;
        return false;
      }
    }
    return true;
  }

  protected static boolean isNonNegative(Double result, boolean fail) {
    if (result != null) {
      final double d = result;
      if (d < 0.0) {
        assert !fail;
        return false;
      }
    }
    return true;
  }

  protected static Double validateResult(Double result) {
    if (result == null) {
      return null;
    }

    // Never let the result go below 1, as it will result in incorrect
    // calculations if the row-count is used as the denominator in a
    // division expression.  Also, cap the value at the max double value
    // to avoid calculations using infinity.
    if (result.isInfinite()) {
      result = Double.MAX_VALUE;
    }
    assert isNonNegative(result, true);
    if (result < 1.0) {
      result = 1.0;
    }
    return result;
  }
}

// End RelMetadataQuery.java
