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
package org.apache.calcite.tools;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
>>>>>>> e323a2a7b ([CALCITE-2812] Add algebraic operators to allow expressing recursive queries)
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
    if (simplify) {
=======
    if (config.simplify()) {
      condition = simplifier.simplifyUnknownAsFalse(condition);
    }
    if (correlate) {
      final CorrelationId id = Iterables.getOnlyElement(variablesSet);
      if (!RelOptUtil.notContainsCorrelation(left.rel, id, Litmus.IGNORE)) {
        throw new IllegalArgumentException("variable " + id
            + " must not be used by left input to correlation");
      }
      // Correlate does not have an ON clause.
      switch (joinType) {
      case LEFT:
        // For a LEFT/SEMI/ANTI, predicate must be evaluated first.
        stack.push(right);
        filter(condition.accept(new Shifter(getRexBuilder(), left.rel, id, right.rel)));
        right = stack.pop();
        break;
      case INNER:
        // For INNER, we can defer.
        postCondition = condition;
        break;
      default:
        throw new IllegalArgumentException("Correlated " + joinType + " join is not supported");
      }
      final ImmutableBitSet requiredColumns = RelOptUtil.correlationColumns(id, right.rel);
      join = struct.correlateFactory.createCorrelate(left.rel, right.rel, id,
          requiredColumns, joinType);
    } else {
      join = struct.joinFactory.createJoin(left.rel, right.rel, ImmutableList.of(), condition,
          variablesSet, joinType, false);
    }
    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    fields.addAll(left.fields);
    fields.addAll(right.fields);
    stack.push(new Frame(join, fields.build()));
    filter(postCondition);
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Correlate}
   * with a {@link CorrelationId} and an array of fields that are used by correlation. */
  public RelBuilder correlate(JoinRelType joinType,
      CorrelationId correlationId, RexNode... requiredFields) {
    return correlate(joinType, correlationId, ImmutableList.copyOf(requiredFields));
  }

  /** Creates a {@link org.apache.calcite.rel.core.Correlate}
   * with a {@link CorrelationId} and a list of fields that are used by correlation. */
  public RelBuilder correlate(JoinRelType joinType,
      CorrelationId correlationId, Iterable<? extends RexNode> requiredFields) {
    Frame right = stack.pop();
    final Registrar registrar = new Registrar();
    registrar.extraNodes.addAll(fields());
    registrar.names.addAll(peek().getRowType().getFieldNames());

    List<Integer> requiredOrdinals =
        registrar.registerExpressions(ImmutableList.copyOf(requiredFields));

    project(registrar.extraNodes);
    rename(registrar.names);
    Frame left = stack.pop();

    final RelNode correlate = struct.correlateFactory
        .createCorrelate(left.rel, right.rel, correlationId,
            ImmutableBitSet.of(requiredOrdinals), joinType);

    final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    fields.addAll(left.fields);
    fields.addAll(right.fields);
    stack.push(new Frame(correlate, fields.build()));

    return this;
  }

  /** Creates a {@link Join} using USING syntax.
   *
   * <p>For each of the field names, both left and right inputs must have a
   * field of that name. Constructs a join condition that the left and right
   * fields are equal.
   *
   * @param joinType Join type
   * @param fieldNames Field names
   */
  public RelBuilder join(JoinRelType joinType, String... fieldNames) {
    final List<RexNode> conditions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      conditions.add(
          call(SqlStdOperatorTable.EQUALS,
              field(2, 0, fieldName),
              field(2, 1, fieldName)));
    }
    return join(joinType, conditions);
  }

  /** Creates a {@link SemiJoin}.
   *
   * <p>A semi-join is a form of join that combines two relational expressions
   * according to some condition, and outputs only rows from the left input for
   * which at least one row from the right input matches. It only outputs
   * columns from the left input, and ignores duplicates on the right.
   *
   * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record, similar to the
   * following SQL:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  public RelBuilder semiJoin(Iterable<? extends RexNode> conditions) {
    final Frame right = stack.pop();
    final RelNode semiJoin =
        struct.joinFactory.createJoin(peek(),
            right.rel,
            ImmutableList.of(),
            and(conditions),
            ImmutableSet.of(),
            JoinRelType.SEMI,
            false);
    replaceTop(semiJoin);
    return this;
  }

  /** Creates a {@link Join} with {@link JoinRelType#SEMI}.
   *
   * @see #semiJoin(Iterable) */
  public RelBuilder semiJoin(RexNode... conditions) {
    return semiJoin(ImmutableList.copyOf(conditions));
  }

  /** Creates an anti-join.
   *
   * <p>An anti-join is a form of join that combines two relational expressions
   * according to some condition, but outputs only rows from the left input
   * for which no rows from the right input match.
   *
   * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record, similar to the
   * following SQL:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE NOT EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  public RelBuilder antiJoin(Iterable<? extends RexNode> conditions) {
    final Frame right = stack.pop();
    final RelNode antiJoin =
        struct.joinFactory.createJoin(peek(),
            right.rel,
            ImmutableList.of(),
            and(conditions),
            ImmutableSet.of(),
            JoinRelType.ANTI,
            false);
    replaceTop(antiJoin);
    return this;
  }

  /** Creates an anti-join.
   *
   * @see #antiJoin(Iterable) */
  public RelBuilder antiJoin(RexNode... conditions) {
    return antiJoin(ImmutableList.copyOf(conditions));
  }

  /** Assigns a table alias to the top entry on the stack. */
  public RelBuilder as(final String alias) {
    final Frame pair = stack.pop();
    List<Field> newFields =
            Util.transform(pair.fields, field -> field.addAlias(alias));

    stack.push(new Frame(pair.rel, ImmutableList.copyOf(newFields)));
    return this;
  }

  /** Creates a {@link Values}.
   *
   * <p>The {@code values} array must have the same number of entries as
   * {@code fieldNames}, or an integer multiple if you wish to create multiple
   * rows.
   *
   * <p>If there are zero rows, or if all values of a any column are
   * null, this method cannot deduce the type of columns. For these cases,
   * call {@link #values(Iterable, RelDataType)}.
   *
   * @param fieldNames Field names
   * @param values Values
   */
  public RelBuilder values(String[] fieldNames, Object... values) {
    if (fieldNames == null
        || fieldNames.length == 0
        || values.length % fieldNames.length != 0
        || values.length < fieldNames.length) {
      throw new IllegalArgumentException(
          "Value count must be a positive multiple of field count");
    }
    final int rowCount = values.length / fieldNames.length;
    for (Ord<String> fieldName : Ord.zip(fieldNames)) {
      if (allNull(values, fieldName.i, fieldNames.length)) {
        throw new IllegalArgumentException("All values of field '" + fieldName.e
            + "' are null; cannot deduce type");
      }
    }
    final ImmutableList<ImmutableList<RexLiteral>> tupleList =
        tupleList(fieldNames.length, values);
    assert tupleList.size() == rowCount;
    final List<String> fieldNameList =
        Util.transformIndexed(Arrays.asList(fieldNames), (name, i) ->
            name != null ? name : "expr$" + i);
    return values(tupleList, fieldNameList);
  }

  private RelBuilder values(List<? extends List<RexLiteral>> tupleList,
      List<String> fieldNames) {
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    Ord.forEach(fieldNames, (fieldName, i) -> {
      final RelDataType type = typeFactory.leastRestrictive(
          new AbstractList<RelDataType>() {
            @Override public RelDataType get(int index) {
              return tupleList.get(index).get(i).getType();
            }

            @Override public int size() {
              return tupleList.size();
            }
          });
      assert type != null
          : "can't infer type for field " + i + ", " + fieldName;
      builder.add(fieldName, type);
    });
    final RelDataType rowType = builder.build();
    return values(tupleList, rowType);
  }

  private ImmutableList<ImmutableList<RexLiteral>> tupleList(int columnCount,
      Object[] values) {
    final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
        ImmutableList.builder();
    final List<RexLiteral> valueList = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      valueList.add((RexLiteral) literal(value));
      if ((i + 1) % columnCount == 0) {
        listBuilder.add(ImmutableList.copyOf(valueList));
        valueList.clear();
      }
    }
    return listBuilder.build();
  }

  /** Returns whether all values for a given column are null. */
  private boolean allNull(Object[] values, int column, int columnCount) {
    for (int i = column; i < values.length; i += columnCount) {
      if (values[i] != null) {
        return false;
      }
    }
    return true;
  }

  /** Creates a relational expression that reads from an input and throws
   * all of the rows away.
   *
   * <p>Note that this method always pops one relational expression from the
   * stack. {@code values}, in contrast, does not pop any relational
   * expressions, and always produces a leaf.
   *
   * <p>The default implementation creates a {@link Values} with the same
   * specified row type and aliases as the input, and ignores the input entirely.
   * But schema-on-query systems such as Drill might override this method to
   * create a relation expression that retains the input, just to read its
   * schema.
   */
  public RelBuilder empty() {
    final Frame frame = stack.pop();
    final RelNode values = struct.valuesFactory.createValues(cluster, frame.rel.getRowType(),
            ImmutableList.of());
    stack.push(new Frame(values, frame.fields));
    return this;
  }

  /** Creates a {@link Values} with a specified row type.
   *
   * <p>This method can handle cases that {@link #values(String[], Object...)}
   * cannot, such as all values of a column being null, or there being zero
   * rows.
   *
   * @param rowType Row type
   * @param columnValues Values
   */
  public RelBuilder values(RelDataType rowType, Object... columnValues) {
    final ImmutableList<ImmutableList<RexLiteral>> tupleList =
        tupleList(rowType.getFieldCount(), columnValues);
    RelNode values = struct.valuesFactory.createValues(cluster, rowType,
        ImmutableList.copyOf(tupleList));
    push(values);
    return this;
  }

  /** Creates a {@link Values} with a specified row type.
   *
   * <p>This method can handle cases that {@link #values(String[], Object...)}
   * cannot, such as all values of a column being null, or there being zero
   * rows.
   *
   * @param tupleList Tuple list
   * @param rowType Row type
   */
  public RelBuilder values(Iterable<? extends List<RexLiteral>> tupleList,
      RelDataType rowType) {
    RelNode values =
        struct.valuesFactory.createValues(cluster, rowType, copy(tupleList));
    push(values);
    return this;
  }

  /** Creates a {@link Values} with a specified row type and
   * zero rows.
   *
   * @param rowType Row type
   */
  public RelBuilder values(RelDataType rowType) {
    return values(ImmutableList.<ImmutableList<RexLiteral>>of(), rowType);
  }

  /** Converts an iterable of lists into an immutable list of immutable lists
   * with the same contents. Returns the same object if possible. */
  private static <E> ImmutableList<ImmutableList<E>> copy(
      Iterable<? extends List<E>> tupleList) {
    final ImmutableList.Builder<ImmutableList<E>> builder =
        ImmutableList.builder();
    int changeCount = 0;
    for (List<E> literals : tupleList) {
      final ImmutableList<E> literals2 =
          ImmutableList.copyOf(literals);
      builder.add(literals2);
      if (literals != literals2) {
        ++changeCount;
      }
    }
    if (changeCount == 0) {
      // don't make a copy if we don't have to
      //noinspection unchecked
      return (ImmutableList<ImmutableList<E>>) tupleList;
    }
    return builder.build();
  }

  /** Creates a limit without a sort. */
  public RelBuilder limit(int offset, int fetch) {
    return sortLimit(offset, fetch, ImmutableList.of());
  }

  /** Creates an Exchange by distribution. */
  public RelBuilder exchange(RelDistribution distribution) {
    RelNode exchange = exchangeFactory.createExchange(peek(), distribution);
    replaceTop(exchange);
    return this;
  }

  /** Creates a SortExchange by distribution and collation. */
  public RelBuilder sortExchange(RelDistribution distribution,
      RelCollation collation) {
    RelNode exchange = sortExchangeFactory
        .createSortExchange(peek(), distribution, collation);
    replaceTop(exchange);
    return this;
  }

  /** Creates a {@link Sort} by field ordinals.
   *
   * <p>Negative fields mean descending: -1 means field(0) descending,
   * -2 means field(1) descending, etc.
   */
  public RelBuilder sort(int... fields) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (int field : fields) {
      builder.add(field < 0 ? desc(field(-field - 1)) : field(field));
    }
    return sortLimit(-1, -1, builder.build());
  }

  /** Creates a {@link Sort} by expressions. */
  public RelBuilder sort(RexNode... nodes) {
    return sortLimit(-1, -1, ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Sort} by expressions. */
  public RelBuilder sort(Iterable<? extends RexNode> nodes) {
    return sortLimit(-1, -1, nodes);
  }

  /** Creates a {@link Sort} by expressions, with limit and offset. */
  public RelBuilder sortLimit(int offset, int fetch, RexNode... nodes) {
    return sortLimit(offset, fetch, ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Sort} by collation, with limit and offset. */
  public RelBuilder sortLimit(RelCollation collation, RexNode offset, RexNode fetch) {
    final RelNode sort = struct.sortFactory.createSort(peek(), collation, offset, fetch);
    replaceTop(sort);
    return this;
  }

  /** Creates a {@link Sort} by a list of expressions, with limit and offset.
   *
   * @param offset Number of rows to skip; non-positive means don't skip any
   * @param fetch Maximum number of rows to fetch; negative means no limit
   * @param nodes Sort expressions
   */
  public RelBuilder sortLimit(int offset, int fetch,
      Iterable<? extends RexNode> nodes) {
    final Registrar registrar = new Registrar(fields());
    final List<RelFieldCollation> fieldCollations =
        registrar.registerFieldCollations(nodes);

    final RexNode offsetNode = offset <= 0 ? null : literal(offset);
    final RexNode fetchNode = fetch < 0 ? null : literal(fetch);
    if (offsetNode == null && fetch == 0) {
      return empty();
    }
    if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
      return this; // sort is trivial
    }

    if (fieldCollations.isEmpty()) {
      assert registrar.addedFieldCount() == 0;
      RelNode top = peek();
      if (top instanceof Sort) {
        final Sort sort2 = (Sort) top;
        if (sort2.offset == null && sort2.fetch == null) {
          replaceTop(sort2.getInput());
          final RelNode sort =
              struct.sortFactory.createSort(peek(), sort2.collation,
                  offsetNode, fetchNode);
          replaceTop(sort);
          return this;
        }
      }
      if (top instanceof Project) {
        final Project project = (Project) top;
        if (project.getInput() instanceof Sort) {
          final Sort sort2 = (Sort) project.getInput();
          if (sort2.offset == null && sort2.fetch == null) {
            final RelNode sort =
                struct.sortFactory.createSort(sort2.getInput(), sort2.collation,
                    offsetNode, fetchNode);
            replaceTop(
                struct.projectFactory.createProject(sort,
                    project.getHints(),
                    project.getProjects(),
                    Pair.right(project.getNamedProjects())));
            return this;
          }
        }
      }
    }
    if (registrar.addedFieldCount() > 0) {
      project(registrar.extraNodes);
    }
    final RelNode sort =
        struct.sortFactory.createSort(peek(), RelCollations.of(fieldCollations),
            offsetNode, fetchNode);
    replaceTop(sort);
    if (registrar.addedFieldCount() > 0) {
      project(registrar.originalExtraNodes);
    }
    return this;
  }

  private static RelFieldCollation collation(RexNode node,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection, List<RexNode> extraNodes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return new RelFieldCollation(((RexInputRef) node).getIndex(), direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    case DESCENDING:
      return collation(((RexCall) node).getOperands().get(0),
          RelFieldCollation.Direction.DESCENDING,
          nullDirection, extraNodes);
    case NULLS_FIRST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.FIRST, extraNodes);
    case NULLS_LAST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.LAST, extraNodes);
    default:
      final int fieldIndex = extraNodes.size();
      extraNodes.add(node);
      return new RelFieldCollation(fieldIndex, direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    }
  }

  /**
   * Creates a projection that converts the current relational expression's
   * output to a desired row type.
   *
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   */
  public RelBuilder convert(RelDataType castRowType, boolean rename) {
    final RelNode r = build();
    final RelNode r2 =
        RelOptUtil.createCastRel(r, castRowType, rename, struct.projectFactory);
    push(r2);
    return this;
  }

  public RelBuilder permute(Mapping mapping) {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();
    if (mapping.isIdentity()) {
      return this;
    }
    final List<RexNode> exprList = new ArrayList<>();
    for (int i = 0; i < mapping.getTargetCount(); i++) {
      exprList.add(field(mapping.getSource(i)));
    }
    return project(exprList);
  }

  /** Clears the stack.
   *
   * <p>The builder's state is now the same as when it was created. */
  public void clear() {
    stack.clear();
  }

  /** Information necessary to create a call to an aggregate function.
   *
   * @see RelBuilder#aggregateCall */
  public interface AggCall {
    /** Returns a copy of this AggCall that applies a filter before aggregating
     * values. */
    AggCall filter(RexNode condition);

    /** Returns a copy of this AggCall that sorts its input values by
     * {@code orderKeys} before aggregating, as in SQL's {@code WITHIN GROUP}
     * clause. */
    AggCall sort(Iterable<RexNode> orderKeys);

    /** Returns a copy of this AggCall that sorts its input values by
     * {@code orderKeys} before aggregating, as in SQL's {@code WITHIN GROUP}
     * clause. */
    AggCall sort(RexNode... orderKeys);

    /** Returns a copy of this AggCall that may return approximate results
     * if {@code approximate} is true. */
    AggCall approximate(boolean approximate);

    /** Returns a copy of this AggCall that ignores nulls. */
    AggCall ignoreNulls(boolean ignoreNulls);

    /** Returns a copy of this AggCall with a given alias. */
    AggCall as(String alias);

    /** Returns a copy of this AggCall that is optionally distinct. */
    AggCall distinct(boolean distinct);

    /** Returns a copy of this AggCall that is distinct. */
    AggCall distinct();
  }

  /** Internal methods shared by all implementations of {@link AggCall}. */
  private interface AggCallPlus extends AggCall {
    /** Returns the aggregate function. */
    SqlAggFunction op();

    /** Returns the alias. */
    String alias();

    /** Returns an {@link AggregateCall} that is approximately equivalent
     * to this {@code AggCall} and is good for certain things, such as deriving
     * field names. */
    AggregateCall aggregateCall();

    /** Converts this {@code AggCall} to a good {@link AggregateCall}. */
    AggregateCall aggregateCall(Registrar registrar, ImmutableBitSet groupSet,
        RelNode r);

    /** Registers expressions in operands and filters. */
    void register(Registrar registrar);
  }

  /** Information necessary to create the GROUP BY clause of an Aggregate.
   *
   * @see RelBuilder#groupKey */
  public interface GroupKey {
    /** Assigns an alias to this group key.
     *
     * <p>Used to assign field names in the {@code group} operation. */
    GroupKey alias(String alias);
  }

  /** Implementation of {@link GroupKey}. */
  protected static class GroupKeyImpl implements GroupKey {
    final ImmutableList<RexNode> nodes;
    final ImmutableList<ImmutableList<RexNode>> nodeLists;
    final String alias;

    GroupKeyImpl(ImmutableList<RexNode> nodes,
        ImmutableList<ImmutableList<RexNode>> nodeLists, String alias) {
      this.nodes = Objects.requireNonNull(nodes);
      this.nodeLists = nodeLists;
      this.alias = alias;
    }

    @Override public String toString() {
      return alias == null ? nodes.toString() : nodes + " as " + alias;
    }

    public GroupKey alias(String alias) {
      return Objects.equals(this.alias, alias)
          ? this
          : new GroupKeyImpl(nodes, nodeLists, alias);
    }
  }

  /** Implementation of {@link AggCall}. */
  private class AggCallImpl implements AggCallPlus {
    private final SqlAggFunction aggFunction;
    private final boolean distinct;
    private final boolean approximate;
    private final boolean ignoreNulls;
    private final RexNode filter; // may be null
    private final String alias; // may be null
    private final ImmutableList<RexNode> operands; // may be empty, never null
    private final ImmutableList<RexNode> orderKeys; // may be empty, never null

    AggCallImpl(SqlAggFunction aggFunction, boolean distinct,
        boolean approximate, boolean ignoreNulls, RexNode filter,
                String alias, ImmutableList<RexNode> operands,
                ImmutableList<RexNode> orderKeys) {
      this.aggFunction = Objects.requireNonNull(aggFunction);
      this.distinct = distinct;
      this.approximate = approximate;
      this.ignoreNulls = ignoreNulls;
      this.alias = alias;
      this.operands = Objects.requireNonNull(operands);
      this.orderKeys = Objects.requireNonNull(orderKeys);
      if (filter != null) {
        if (filter.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
          throw RESOURCE.filterMustBeBoolean().ex();
        }
        if (filter.getType().isNullable()) {
          filter = call(SqlStdOperatorTable.IS_TRUE, filter);
        }
      }
      this.filter = filter;
    }

    @Override public String toString() {
      final StringBuilder b = new StringBuilder();
      b.append(aggFunction.getName())
          .append('(');
      if (distinct) {
        b.append("DISTINCT ");
      }
      b.append(operands)
          .append(')');
      if (filter != null) {
        b.append(" FILTER (WHERE" + filter + ")");
      }
      return b.toString();
    }

    @Override public SqlAggFunction op() {
      return aggFunction;
    }

    @Override public String alias() {
      return alias;
    }

    @Override public AggregateCall aggregateCall() {
      return null;
    }

    @Override public AggregateCall aggregateCall(
        Registrar registrar,
        ImmutableBitSet groupSet,
        RelNode r) {
      return null;
    }

    @Override public void register(Registrar registrar) {
      registrar.registerExpressions(operands);
      if (filter != null) {
        registrar.registerExpression(filter);
      }
    }

    public AggCall sort(Iterable<RexNode> orderKeys) {
      final ImmutableList<RexNode> orderKeyList =
              ImmutableList.copyOf(orderKeys);
      return orderKeyList.equals(this.orderKeys)
        ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeyList);
    }

    public AggCall sort(RexNode... orderKeys) {
      return sort(ImmutableList.copyOf(orderKeys));
    }

    public AggCall approximate(boolean approximate) {
      return approximate == this.approximate
        ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }

    public AggCall filter(RexNode condition) {
      return Objects.equals(condition, this.filter)
        ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              condition, alias, operands, orderKeys);
    }

    public AggCall as(String alias) {
      return Objects.equals(alias, this.alias)
        ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }

    public AggCall distinct(boolean distinct) {
      return distinct == this.distinct
        ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }

    public AggCall distinct() {
      return distinct(true);
    }

    public AggCall ignoreNulls(boolean ignoreNulls) {
      return ignoreNulls == this.ignoreNulls
          ? this
          : new AggCallImpl(aggFunction, distinct, approximate, ignoreNulls,
              filter, alias, operands, orderKeys);
    }
  }

  /** Implementation of {@link AggCall} that wraps an
   * {@link AggregateCall}. */
  private static class AggCallImpl2 implements AggCallPlus {
    private final AggregateCall aggregateCall;

    AggCallImpl2(AggregateCall aggregateCall) {
      this.aggregateCall = Objects.requireNonNull(aggregateCall);
    }

    @Override public String toString() {
      return aggregateCall.toString();
    }

    @Override public SqlAggFunction op() {
      return aggregateCall.getAggregation();
    }

    @Override public String alias() {
      return aggregateCall.name;
    }

    @Override public AggregateCall aggregateCall() {
      return aggregateCall;
    }

    @Override public AggregateCall aggregateCall(Registrar registrar,
        ImmutableBitSet groupSet, RelNode r) {
      return aggregateCall;
    }

    @Override public void register(Registrar registrar) {
      // nothing to do
    }

    public AggCall sort(Iterable<RexNode> orderKeys) {
      throw new UnsupportedOperationException();
    }

    public AggCall sort(RexNode... orderKeys) {
      throw new UnsupportedOperationException();
    }

    public AggCall approximate(boolean approximate) {
      throw new UnsupportedOperationException();
    }

    public AggCall filter(RexNode condition) {
      throw new UnsupportedOperationException();
    }

    public AggCall as(String alias) {
      throw new UnsupportedOperationException();
    }

    public AggCall distinct(boolean distinct) {
      throw new UnsupportedOperationException();
    }

    public AggCall distinct() {
      throw new UnsupportedOperationException();
    }

    public AggCall ignoreNulls(boolean ignoreNulls) {
      throw new UnsupportedOperationException();
    }
  }

  /** Collects the extra expressions needed for {@link #aggregate}.
   *
   * <p>The extra expressions come from the group key and as arguments to
   * aggregate calls, and later there will be a {@link #project} or a
   * {@link #rename(List)} if necessary. */
  private static class Registrar {
    final List<RexNode> originalExtraNodes;
    final List<RexNode> extraNodes;
    final List<String> names = new ArrayList<>();

    Registrar(Iterable<RexNode> fields) {
      this(fields, ImmutableList.of());
    }

    Registrar(Iterable<RexNode> fields, List<String> fieldNames) {
      originalExtraNodes = ImmutableList.copyOf(fields);
      extraNodes = new ArrayList<>(originalExtraNodes);
      names.addAll(fieldNames);
    }

    int registerExpression(RexNode node) {
      switch (node.getKind()) {
      case AS:
        final List<RexNode> operands = ((RexCall) node).operands;
        int i = registerExpression(operands.get(0));
        names.set(i, RexLiteral.stringValue(operands.get(1)));
        return i;
      }
      int i = extraNodes.indexOf(node);
      if (i < 0) {
        i = extraNodes.size();
        extraNodes.add(node);
        names.add(null);
      }
      return i;
    }

    List<Integer> registerExpressions(Iterable<? extends RexNode> nodes) {
      final List<Integer> builder = new ArrayList<>();
      for (RexNode node : nodes) {
        builder.add(registerExpression(node));
      }
      return builder;
    }

    List<RelFieldCollation> registerFieldCollations(
        Iterable<? extends RexNode> orderKeys) {
      final List<RelFieldCollation> fieldCollations = new ArrayList<>();
      for (RexNode orderKey : orderKeys) {
        final RelFieldCollation collation =
            collation(orderKey, RelFieldCollation.Direction.ASCENDING, null,
                extraNodes);
        if (!RelCollations.ordinals(fieldCollations)
            .contains(collation.getFieldIndex())) {
          fieldCollations.add(collation);
        }
      }
      return ImmutableList.copyOf(fieldCollations);
    }

    /** Returns the number of fields added. */
    int addedFieldCount() {
      return extraNodes.size() - originalExtraNodes.size();
    }
  }

  /** Builder stack frame.
   *
   * <p>Describes a previously created relational expression and
   * information about how table aliases map into its row type. */
  private static class Frame {
    final RelNode rel;
    final ImmutableList<Field> fields;

    private Frame(RelNode rel, ImmutableList<Field> fields) {
      this.rel = rel;
      this.fields = fields;
    }

    private Frame(RelNode rel) {
      String tableAlias = deriveAlias(rel);
      ImmutableList.Builder<Field> builder = ImmutableList.builder();
      ImmutableSet<String> aliases = tableAlias == null
          ? ImmutableSet.of()
          : ImmutableSet.of(tableAlias);
      for (RelDataTypeField field : rel.getRowType().getFieldList()) {
        builder.add(new Field(aliases, field));
      }
      this.rel = rel;
      this.fields = builder.build();
    }

    @Override public String toString() {
      return rel + ": " + fields;
    }

    private static String deriveAlias(RelNode rel) {
      if (rel instanceof TableScan) {
        final List<String> names = rel.getTable().getQualifiedName();
        if (!names.isEmpty()) {
          return Util.last(names);
        }
      }
      return null;
    }

    List<RelDataTypeField> fields() {
      return Pair.right(fields);
    }
  }

  /** A field that belongs to a stack {@link Frame}. */
  private static class Field
      extends Pair<ImmutableSet<String>, RelDataTypeField> {
    Field(ImmutableSet<String> left, RelDataTypeField right) {
      super(left, right);
    }

    Field addAlias(String alias) {
      if (left.contains(alias)) {
        return this;
      }
      final ImmutableSet<String> aliasList =
          ImmutableSet.<String>builder().addAll(left).add(alias).build();
      return new Field(aliasList, right);
    }
  }

  /** Shuttle that shifts a predicate's inputs to the left, replacing early
   * ones with references to a
<<<<<<< HEAD
<<<<<<< HEAD
   * {@link RexCorrelVariable}. */
=======
   * {@link org.apache.calcite.rex.RexCorrelVariable}. */
=======
   * {@link RexCorrelVariable}. */
  public static class Shifter extends RexShuttle {
    private final RelNode left;
    private final CorrelationId id;
    private final RelNode right;
    private final RexBuilder rexBuilder;

    public Shifter(RexBuilder rexbuilder, RelNode left, CorrelationId id, RelNode right) {
      this.rexBuilder = rexbuilder;
      this.left = left;
      this.id = id;
      this.right = right;
    }

    public RexNode visitInputRef(RexInputRef inputRef) {
      final RelDataType leftRowType = left.getRowType();
      final int leftCount = leftRowType.getFieldCount();
      if (inputRef.getIndex() < leftCount) {
        final RexNode v = rexBuilder.makeCorrel(leftRowType, id);
        return rexBuilder.makeFieldAccess(v, inputRef.getIndex());
      } else {
        return rexBuilder.makeInputRef(right, inputRef.getIndex() - leftCount);
      }
    }
  }
<<<<<<< HEAD
=======

  /** Configuration of RelBuilder.
   *
   * <p>It is immutable, and all fields are public.
   *
   * <p>Start with the {@link #DEFAULT} instance,
   * and call {@code withXxx} methods to set its properties. */
  public interface Config {
    /** Default configuration. */
    Config DEFAULT = ImmutableBeans.create(Config.class);

    @Deprecated // to be removed before 2.0
    static ConfigBuilder builder() {
      return DEFAULT.toBuilder();
    }

    @Deprecated // to be removed before 2.0
    default ConfigBuilder toBuilder() {
      return new ConfigBuilder(this);
    }

    /** Whether {@link RelBuilder#aggregate} should eliminate duplicate
     * aggregate calls; default true. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean dedupAggregateCalls();

    /** Sets {@link #dedupAggregateCalls}. */
    Config withDedupAggregateCalls(boolean dedupAggregateCalls);

    /** Whether to simplify expressions; default true. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean simplify();

    /** Sets {@link #simplify}. */
    Config withSimplify(boolean simplify);
  }

  /** Creates a {@link RelBuilder.Config}.
   *
   * @deprecated Use the {@code withXxx} methods in
   * {@link RelBuilder.Config}. */
  @Deprecated // to be removed before 2.0
  public static class ConfigBuilder {
    private Config config;

    private ConfigBuilder(@Nonnull Config config) {
      this.config = config;
    }

    /** Creates a {@link RelBuilder.Config}. */
    public Config build() {
      return config;
    }

    /** Sets the value that will become
     * {@link org.apache.calcite.tools.RelBuilder.Config#dedupAggregateCalls}. */
    public ConfigBuilder withDedupAggregateCalls(boolean dedupAggregateCalls) {
      config = config.withDedupAggregateCalls(dedupAggregateCalls);
      return this;
    }

    /** Sets the value that will become
     * {@link org.apache.calcite.tools.RelBuilder.Config#simplify}. */
    public ConfigBuilder withSimplify(boolean simplify) {
      config = config.withSimplify(simplify);
      return this;
    }
  }
}

  /** Configuration of RelBuilder.
   *
   * <p>It is immutable, and all fields are public.
   *
   * <p>Start with the {@link #DEFAULT} instance,
   * and call {@code withXxx} methods to set its properties. */
  public interface Config {
    /** Default configuration. */
    Config DEFAULT = ImmutableBeans.create(Config.class);

    @Deprecated // to be removed before 2.0
    static ConfigBuilder builder() {
      return DEFAULT.toBuilder();
    }

    @Deprecated // to be removed before 2.0
    default ConfigBuilder toBuilder() {
      return new ConfigBuilder(this);
    }

    /** Whether {@link RelBuilder#aggregate} should eliminate duplicate
     * aggregate calls; default true. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean dedupAggregateCalls();

    /** Sets {@link #dedupAggregateCalls}. */
    Config withDedupAggregateCalls(boolean dedupAggregateCalls);

    /** Whether {@link RelBuilder#aggregate} should prune unused
     * input columns; default true. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean pruneInputOfAggregate();

    /** Sets {@link #pruneInputOfAggregate}. */
    Config withPruneInputOfAggregate(boolean pruneInputOfAggregate);

    /** Whether to simplify expressions; default true. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean simplify();

    /** Sets {@link #simplify}. */
    Config withSimplify(boolean simplify);
  }

  /** Creates a {@link RelBuilder.Config}.
   *
   * @deprecated Use the {@code withXxx} methods in
   * {@link RelBuilder.Config}. */
  @Deprecated // to be removed before 2.0
  public static class ConfigBuilder {
    private Config config;

    private ConfigBuilder(@Nonnull Config config) {
      this.config = config;
    }

    /** Creates a {@link RelBuilder.Config}. */
    public Config build() {
      return config;
    }

    /** Sets the value that will become
     * {@link org.apache.calcite.tools.RelBuilder.Config#dedupAggregateCalls}. */
    public ConfigBuilder withDedupAggregateCalls(boolean dedupAggregateCalls) {
      config = config.withDedupAggregateCalls(dedupAggregateCalls);
      return this;
    }

    /** Sets the value that will become
     * {@link org.apache.calcite.tools.RelBuilder.Config#simplify}. */
    public ConfigBuilder withSimplify(boolean simplify) {
      config = config.withSimplify(simplify);
      return this;
    }
  }
}
