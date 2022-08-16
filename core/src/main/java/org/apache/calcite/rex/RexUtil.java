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
package org.apache.calcite.rex;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

>>>>>>> 232030fd4 ([CALCITE-2632]: Add hashCode and equals implementations to RexNode)
      if (!allowDups && (previous != null)) {
        throw new SubExprExistsException(expr);
      }
      return expr;
    }

    protected RexNode lookup(RexNode expr) {
      return map.get(expr);
    }

    public RexNode visitInputRef(RexInputRef inputRef) {
      return register(inputRef);
    }

    public RexNode visitLiteral(RexLiteral literal) {
      return register(literal);
    }

    public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
      return register(correlVariable);
    }

    public RexNode visitCall(RexCall call) {
      List<RexNode> normalizedOperands = new ArrayList<>();
      int diffCount = 0;
      for (RexNode operand : call.getOperands()) {
        operand.accept(this);
        final RexNode normalizedOperand = lookup(operand);
        normalizedOperands.add(normalizedOperand);
        if (normalizedOperand != operand) {
          ++diffCount;
        }
      }
      if (diffCount > 0) {
        call =
            call.clone(
                call.getType(),
                normalizedOperands);
      }
      return register(call);
    }

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      return register(dynamicParam);
    }

    public RexNode visitRangeRef(RexRangeRef rangeRef) {
      return register(rangeRef);
    }

    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      final RexNode expr = fieldAccess.getReferenceExpr();
      expr.accept(this);
      final RexNode normalizedExpr = lookup(expr);
      if (normalizedExpr != expr) {
        fieldAccess =
            new RexFieldAccess(
                normalizedExpr,
                fieldAccess.getField());
      }
      return register(fieldAccess);
    }

    /**
     * Thrown if there is a sub-expression.
     */
    private static class SubExprExistsException extends ControlFlowException {
      SubExprExistsException(RexNode expr) {
        Util.discard(expr);
      }
    }
  }

  /**
   * Walks over an expression and throws an exception if it finds an
   * {@link RexInputRef} with an ordinal beyond the number of fields in the
   * input row type, or a {@link RexLocalRef} with ordinal greater than that set
   * using {@link #setLimit(int)}.
   */
  private static class ForwardRefFinder extends RexVisitorImpl<Void> {
    private int limit = -1;
    private final RelDataType inputRowType;

    ForwardRefFinder(RelDataType inputRowType) {
      super(true);
      this.inputRowType = inputRowType;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      super.visitInputRef(inputRef);
      if (inputRef.getIndex() >= inputRowType.getFieldCount()) {
        throw new IllegalForwardRefException();
      }
      return null;
    }

    public Void visitLocalRef(RexLocalRef inputRef) {
      super.visitLocalRef(inputRef);
      if (inputRef.getIndex() >= limit) {
        throw new IllegalForwardRefException();
      }
      return null;
    }

    public void setLimit(int limit) {
      this.limit = limit;
    }

    /** Thrown to abort a visit when we find an illegal forward reference.
     * It changes control flow but is not considered an error. */
    static class IllegalForwardRefException extends ControlFlowException {
    }
  }

  /**
   * Visitor which builds a bitmap of the inputs used by an expression.
   */
  public static class FieldAccessFinder extends RexVisitorImpl<Void> {
    private final List<RexFieldAccess> fieldAccessList;

    public FieldAccessFinder() {
      super(true);
      fieldAccessList = new ArrayList<>();
    }

    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      fieldAccessList.add(fieldAccess);
      return null;
    }

    public Void visitCall(RexCall call) {
      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return null;
    }

    public List<RexFieldAccess> getFieldAccessList() {
      return fieldAccessList;
    }
  }

  /** Helps {@link org.apache.calcite.rex.RexUtil#toCnf}. */
  private static class CnfHelper {
    final RexBuilder rexBuilder;
    int currentCount;
    final int maxNodeCount; // negative means no limit

    private CnfHelper(RexBuilder rexBuilder, int maxNodeCount) {
      this.rexBuilder = rexBuilder;
      this.maxNodeCount = maxNodeCount;
    }

    public RexNode toCnf(RexNode rex) {
      try {
        this.currentCount = 0;
        return toCnf2(rex);
      } catch (OverflowError e) {
        Util.swallow(e, null);
        return rex;
      }
    }

    private RexNode toCnf2(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        incrementAndCheck();
        operands = flattenAnd(((RexCall) rex).getOperands());
        final List<RexNode> cnfOperands = new ArrayList<>();
        for (RexNode node : operands) {
          RexNode cnf = toCnf2(node);
          switch (cnf.getKind()) {
          case AND:
            incrementAndCheck();
            cnfOperands.addAll(((RexCall) cnf).getOperands());
            break;
          default:
            incrementAndCheck();
            cnfOperands.add(cnf);
          }
        }
        return and(cnfOperands);
      case OR:
        incrementAndCheck();
        operands = flattenOr(((RexCall) rex).getOperands());
        final RexNode head = operands.get(0);
        final RexNode headCnf = toCnf2(head);
        final List<RexNode> headCnfs = RelOptUtil.conjunctions(headCnf);
        final RexNode tail = or(Util.skip(operands));
        final RexNode tailCnf = toCnf2(tail);
        final List<RexNode> tailCnfs = RelOptUtil.conjunctions(tailCnf);
        final List<RexNode> list = new ArrayList<>();
        for (RexNode h : headCnfs) {
          for (RexNode t : tailCnfs) {
            list.add(or(ImmutableList.of(h, t)));
          }
        }
        return and(list);
      case NOT:
        final RexNode arg = ((RexCall) rex).getOperands().get(0);
        switch (arg.getKind()) {
        case NOT:
          return toCnf2(((RexCall) arg).getOperands().get(0));
        case OR:
          operands = ((RexCall) arg).getOperands();
          return toCnf2(
              and(Lists.transform(flattenOr(operands), RexUtil::addNot)));
        case AND:
          operands = ((RexCall) arg).getOperands();
          return toCnf2(
              or(Lists.transform(flattenAnd(operands), RexUtil::addNot)));
        default:
          incrementAndCheck();
          return rex;
        }
      default:
        incrementAndCheck();
        return rex;
      }
    }

    private void incrementAndCheck() {
      if (maxNodeCount >= 0 && ++currentCount > maxNodeCount) {
        throw OverflowError.INSTANCE;
      }
    }

    /** Exception to catch when we pass the limit. */
    @SuppressWarnings("serial")
    private static class OverflowError extends ControlFlowException {
      @SuppressWarnings("ThrowableInstanceNeverThrown")
      protected static final OverflowError INSTANCE = new OverflowError();

      private OverflowError() {}
    }

    private RexNode pull(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        operands = flattenAnd(((RexCall) rex).getOperands());
        return and(pullList(operands));
      case OR:
        operands = flattenOr(((RexCall) rex).getOperands());
        final Map<RexNode, RexNode> factors = commonFactors(operands);
        if (factors.isEmpty()) {
          return or(operands);
        }
        final List<RexNode> list = new ArrayList<>();
        for (RexNode operand : operands) {
          list.add(removeFactor(factors, operand));
        }
        return and(Iterables.concat(factors.values(), ImmutableList.of(or(list))));
      default:
        return rex;
      }
    }

    private List<RexNode> pullList(List<RexNode> nodes) {
      final List<RexNode> list = new ArrayList<>();
      for (RexNode node : nodes) {
        RexNode pulled = pull(node);
        switch (pulled.getKind()) {
        case AND:
          list.addAll(((RexCall) pulled).getOperands());
          break;
        default:
          list.add(pulled);
        }
      }
      return list;
    }

    private Map<RexNode, RexNode> commonFactors(List<RexNode> nodes) {
      final Map<RexNode, RexNode> map = new HashMap<>();
      int i = 0;
      for (RexNode node : nodes) {
        if (i++ == 0) {
          for (RexNode conjunction : RelOptUtil.conjunctions(node)) {
            map.put(conjunction, conjunction);
          }
        } else {
          map.keySet().retainAll(RelOptUtil.conjunctions(node));
        }
      }
      return map;
    }

    private RexNode removeFactor(Map<RexNode, RexNode> factors, RexNode node) {
      List<RexNode> list = new ArrayList<>();
      for (RexNode operand : RelOptUtil.conjunctions(node)) {
        if (!factors.containsKey(operand)) {
          list.add(operand);
        }
      }
      return and(list);
    }

    private RexNode and(Iterable<? extends RexNode> nodes) {
      return composeConjunction(rexBuilder, nodes);
    }

    private RexNode or(Iterable<? extends RexNode> nodes) {
      return composeDisjunction(rexBuilder, nodes);
    }
  }

  /** Transforms a list of expressions to the list of digests. */
  public static List<String> strings(List<RexNode> list) {
    return Lists.transform(list, Object::toString);
  }

  /** Detects if the given RexNode has an interval data type. */
  public static boolean isInterval(RexNode node) {
    return INTERVAL_TYPES.contains(node.getType().getSqlTypeName());
  }

  /** Detects if the given RexNode is an interval literal. */
  public static boolean isIntervalLiteral(RexNode node) {
    return node instanceof RexLiteral && isInterval(node);
  }

  /**
   * Finds all {@link CorrelationId} used in {@link RexSubQuery}.
   *
   * @param rexNode node to traverse
   * @return A set of all {@link CorrelationId} used
   */
  public static Set<CorrelationId> findSubQueryCorrelationIds(RexNode rexNode) {
    Set<CorrelationId> correlationIds = new HashSet<>();
    findSubQueryCorrelationIds(correlationIds, rexNode);
    return correlationIds;
  }

  /**
   * Finds all {@link CorrelationId} used in {@link RexSubQuery}.
   *
   * @param rexNodes nodes to traverse
   * @return A set of all {@link CorrelationId} used
   */
  public static Set<CorrelationId> findSubQueryCorrelationIds(Iterable<RexNode> rexNodes) {
    Set<CorrelationId> correlationIds = new HashSet<>();
    for (RexNode rexNode : rexNodes) {
      findSubQueryCorrelationIds(correlationIds, rexNode);
    }
    return correlationIds;
  }

  private static void findSubQueryCorrelationIds(Set<CorrelationId> correlationIds,
      RexNode rexNode) {
    rexNode.accept(new RexVisitorImpl<Void>(true) {
      @Override public Void visitSubQuery(RexSubQuery subQuery) {
        if (subQuery.correlationId != null) {
          correlationIds.add(subQuery.correlationId);
        }
        return super.visitSubQuery(subQuery);
      }
    });
  }

  /** Detects if the given RexNode has an interval data type. */
  public static boolean isInterval(RexNode node) {
    return INTERVAL_TYPES.contains(node.getType().getSqlTypeName());
  }

  /** Detects if the given RexNode is an interval literal. */
  public static boolean isIntervalLiteral(RexNode node) {
    return node instanceof RexLiteral && isInterval(node);
  }

  /** Helps {@link org.apache.calcite.rex.RexUtil#toDnf}. */
  private static class DnfHelper {
    final RexBuilder rexBuilder;
    int currentCount;
    final int maxNodeCount; // negative means no limit

    private DnfHelper(RexBuilder rexBuilder, int maxNodeCount) {
      this.rexBuilder = rexBuilder;
      this.maxNodeCount = maxNodeCount;
    }

    public RexNode toDnf(RexNode rex) {
      try {
        this.currentCount = 0;
        return toDnf2(rex);
      } catch (DnfHelper.OverflowError e) {
        Util.swallow(e, null);
        return rex;
      }
    }

    public RexNode toDnf2(RexNode rex) {
      final List<RexNode> operands;
      switch (rex.getKind()) {
      case AND:
        incrementAndCheck();
        operands = flattenAnd(((RexCall) rex).getOperands());
        final RexNode head = operands.get(0);
        final RexNode headDnf = toDnf2(head);
        final List<RexNode> headDnfs = RelOptUtil.disjunctions(headDnf);
        final RexNode tail = and(Util.skip(operands));
        final RexNode tailDnf = toDnf2(tail);
        final List<RexNode> tailDnfs = RelOptUtil.disjunctions(tailDnf);
        final List<RexNode> list = new ArrayList<>();
        for (RexNode h : headDnfs) {
          for (RexNode t : tailDnfs) {
            list.add(and(ImmutableList.of(h, t)));
          }
        }
        return or(list);
      case OR:
        incrementAndCheck();
        operands = flattenOr(((RexCall) rex).getOperands());
<<<<<<< HEAD
        final List<RexNode> dnfOperands = new ArrayList<>();
=======
        final List<RexNode> dnfOperands = Lists.newArrayList();
        for (RexNode node : operands) {
          RexNode dnf = toDnf2(node);
          switch (dnf.getKind()) {
          case OR:
            incrementAndCheck();
            dnfOperands.addAll(((RexCall) dnf).getOperands());
            break;
          default:
            incrementAndCheck();
            dnfOperands.add(dnf);
          }
        }
        return or(dnfOperands);
      case NOT:
        final RexNode arg = ((RexCall) rex).getOperands().get(0);
        switch (arg.getKind()) {
        case NOT:
          return toDnf2(((RexCall) arg).getOperands().get(0));
        case OR:
          operands = ((RexCall) arg).getOperands();
          return toDnf2(and(Lists.transform(flattenOr(operands), ADD_NOT)));
        case AND:
          operands = ((RexCall) arg).getOperands();
          return toDnf2(or(Lists.transform(flattenAnd(operands), ADD_NOT)));
        default:
          incrementAndCheck();
          return rex;
        }
      default:
        incrementAndCheck();
        return rex;
      }
    }

    private RexNode and(Iterable<? extends RexNode> nodes) {
      return composeConjunction(rexBuilder, nodes);
    }

    private RexNode or(Iterable<? extends RexNode> nodes) {
      return composeDisjunction(rexBuilder, nodes);
    }
<<<<<<< HEAD
=======

    private void incrementAndCheck() {
      if (maxNodeCount >= 0 && ++currentCount > maxNodeCount) {
        throw DnfHelper.OverflowError.INSTANCE;
      }
    }

    /** Exception to catch when we pass the limit. */
    @SuppressWarnings("serial")
    private static class OverflowError extends ControlFlowException {
      @SuppressWarnings("ThrowableInstanceNeverThrown")
      protected static final DnfHelper.OverflowError INSTANCE = new DnfHelper.OverflowError();

      private OverflowError() {}
    }
  }
>>>>>>> 1a728e568 ([CALCITE-5238] Add a maxNodeCount parameter to RexUtil.toDnf)

    private void incrementAndCheck() {
      if (maxNodeCount >= 0 && ++currentCount > maxNodeCount) {
        throw DnfHelper.OverflowError.INSTANCE;
      }
    }

    /** Exception to catch when we pass the limit. */
    @SuppressWarnings("serial")
    private static class OverflowError extends ControlFlowException {
      @SuppressWarnings("ThrowableInstanceNeverThrown")
      protected static final DnfHelper.OverflowError INSTANCE = new DnfHelper.OverflowError();
      private OverflowError() {}
    }
  }

  /** Shuttle that adds {@code offset} to each {@link RexInputRef} in an
   * expression. */
  private static class RexShiftShuttle extends RexShuttle {
    private final int offset;

    RexShiftShuttle(int offset) {
      this.offset = offset;
    }

    @Override public RexNode visitInputRef(RexInputRef input) {
      return new RexInputRef(input.getIndex() + offset, input.getType());
    }
  }

  /** Visitor that throws {@link org.apache.calcite.util.Util.FoundOne} if
   * applied to an expression that contains a {@link RexCorrelVariable}. */
  private static class CorrelationFinder extends RexVisitorImpl<Void> {
    static final CorrelationFinder INSTANCE = new CorrelationFinder();

    private CorrelationFinder() {
      super(true);
    }

    @Override public Void visitCorrelVariable(RexCorrelVariable var) {
      throw Util.FoundOne.NULL;
    }
  }

  /** Shuttle that fixes up an expression to match changes in nullability of
   * input fields. */
  public static class FixNullabilityShuttle extends RexShuttle {
    private final List<RelDataType> typeList;
    private final RexBuilder rexBuilder;

    public FixNullabilityShuttle(RexBuilder rexBuilder,
        List<RelDataType> typeList) {
      this.typeList = typeList;
      this.rexBuilder = rexBuilder;
    }

    @Override public RexNode visitInputRef(RexInputRef ref) {
      final RelDataType rightType = typeList.get(ref.getIndex());
      final RelDataType refType = ref.getType();
      if (refType.equals(rightType)) {
        return ref;
      }
      final RelDataType refType2 =
          rexBuilder.getTypeFactory().createTypeWithNullability(refType,
              rightType.isNullable());
      if (Objects.equals(refType2, rightType)) {
        return new RexInputRef(ref.getIndex(), refType2);
      }
      throw new AssertionError("mismatched type " + ref + " " + rightType);
    }
  }

  /** Visitor that throws {@link org.apache.calcite.util.Util.FoundOne} if
   * applied to an expression that contains a {@link RexSubQuery}. */
  public static class SubQueryFinder extends RexVisitorImpl<Void> {
    public static final SubQueryFinder INSTANCE = new SubQueryFinder();

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<Project> PROJECT_PREDICATE =
        SubQueryFinder::containsSubQuery;

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final Predicate<Filter> FILTER_PREDICATE =
        SubQueryFinder::containsSubQuery;

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<Join> JOIN_PREDICATE =
        SubQueryFinder::containsSubQuery;

    private SubQueryFinder() {
      super(true);
    }

    /** Returns whether a {@link Project} contains a sub-query. */
    public static boolean containsSubQuery(Project project) {
      for (RexNode node : project.getProjects()) {
        try {
          node.accept(INSTANCE);
        } catch (Util.FoundOne e) {
          return true;
        }
      }
      return false;
    }

    /** Returns whether a {@link Filter} contains a sub-query. */
    public static boolean containsSubQuery(Filter filter) {
      try {
        filter.getCondition().accept(INSTANCE);
        return false;
      } catch (Util.FoundOne e) {
        return true;
      }
    }

    /** Returns whether a {@link Join} contains a sub-query. */
    public static boolean containsSubQuery(Join join) {
      try {
        join.getCondition().accept(INSTANCE);
        return false;
      } catch (Util.FoundOne e) {
        return true;
      }
    }

    @Override public Void visitSubQuery(RexSubQuery subQuery) {
      throw new Util.FoundOne(subQuery);
    }

    public static RexSubQuery find(Iterable<RexNode> nodes) {
      for (RexNode node : nodes) {
        try {
          node.accept(INSTANCE);
        } catch (Util.FoundOne e) {
          return (RexSubQuery) e.getNode();
        }
      }
      return null;
    }

    public static RexSubQuery find(RexNode node) {
      try {
        node.accept(INSTANCE);
        return null;
      } catch (Util.FoundOne e) {
        return (RexSubQuery) e.getNode();
      }
    }
  }

  /** Deep expressions simplifier.
   *
   * <p>This class is broken because it does not change the value of
   * {@link RexUnknownAs} as it recurses into an expression. Do not use. */
  @Deprecated // to be removed before 2.0
  public static class ExprSimplifier extends RexShuttle {
    private final RexSimplify simplify;
    private final Map<RexNode, RexUnknownAs> unknownAsMap =
        new HashMap<>();
    private final RexUnknownAs unknownAs;
    private final boolean matchNullability;

    public ExprSimplifier(RexSimplify simplify) {
      this(simplify, RexUnknownAs.UNKNOWN, true);
    }

    public ExprSimplifier(RexSimplify simplify, boolean matchNullability) {
      this(simplify, RexUnknownAs.UNKNOWN, matchNullability);
    }

    public ExprSimplifier(RexSimplify simplify, RexUnknownAs unknownAs,
        boolean matchNullability) {
      this.simplify = simplify;
      this.unknownAs = unknownAs;
      this.matchNullability = matchNullability;
    }

    @Override public RexNode visitCall(RexCall call) {
      RexUnknownAs unknownAs = this.unknownAs;
      switch (unknownAs)  {
      case FALSE:
        switch (call.getKind()) {
        case AND:
        case CASE:
          // Default value is used for top operator
          unknownAs = unknownAsMap.getOrDefault(call, RexUnknownAs.FALSE);
          break;
        default:
          unknownAs = RexUnknownAs.UNKNOWN;
        }
        for (RexNode operand : call.operands) {
          this.unknownAsMap.put(operand, unknownAs);
        }
      }
      RexNode node = super.visitCall(call);
      RexNode simplifiedNode = simplify.simplify(node, unknownAs);
      if (node == simplifiedNode) {
        return node;
      }
      if (simplifiedNode.getType().equals(call.getType())) {
        return simplifiedNode;
      }
      return simplify.rexBuilder.makeCast(call.getType(), simplifiedNode, matchNullability);
    }
  }
<<<<<<< HEAD
=======

  /**
   * Visitor class which transforms datetime_plus operators to timestamp_add operators.
   */
  private static class DateTimePlusToTsAddVisitor extends RexShuttle {
    private final RexBuilder builder;

    DateTimePlusToTsAddVisitor(RexBuilder builder) {
      this.builder = builder;
    }

    private static TimeUnit getTimeUnitFromIntervalLiteral(RexNode intervalLiteral) {
      final IntervalSqlType intervalType = (IntervalSqlType) intervalLiteral.getType();
      return intervalType.getIntervalQualifier().getUnit();
    }

    private RexNode convertIntervalLiteralToInteger(TimeUnit unit,
        RexNode intervalLiteral, RexNode coefficient, boolean negate) {
      final RexLiteral intervalAsInt = builder.makeExactLiteral(((RexLiteral) intervalLiteral)
          .getValueAs(BigDecimal.class).divide(unit.multiplier, 0, RoundingMode.HALF_DOWN));

      // No co-efficient. Just return the original literal.
      if (null == coefficient) {
        if (!negate)  {
          return intervalAsInt;
        }
        // Make the negation part of the integer literal.
        return builder.makeExactLiteral(((RexLiteral) intervalLiteral)
            .getValueAs(BigDecimal.class)
            .divide(unit.multiplier, 0, RoundingMode.HALF_DOWN)
            .negate());
      }

      RexNode result;
      // Unit literal. Omit.
      if (intervalAsInt.getValueAs(BigDecimal.class).equals(BigDecimal.ONE)) {
        result = coefficient;
      } else {
        // Multiply the interval-as-int by the coefficient.
        result = builder.makeCall(SqlStdOperatorTable.MULTIPLY, intervalAsInt, coefficient);
      }

      if (negate) {
        result = builder.makeCall(SqlStdOperatorTable.UNARY_MINUS, result);
      }

      return result;
    }

    @Override public RexNode visitCall(RexCall call) {
      // Only transform DATETIME_PLUS calls that are applied to a
      // datetime <op> single-field interval.
      if (call.getOperator() != SqlStdOperatorTable.DATETIME_PLUS
          && ((call.getOperator() != SqlStdOperatorTable.MINUS_DATE)
              || (call.getOperands().size() != 2)
              || !(INTERVAL_TYPES.contains(
                      call.getOperands().get(1).getType().getSqlTypeName())))) {
        return super.visitCall(call);
      }

      RexNode datetime = call.getOperands().get(0);
      final SqlTypeName datetimeType = datetime.getType().getSqlTypeName();
      if (datetimeType != SqlTypeName.DATE
          && datetimeType != SqlTypeName.TIME
          && datetimeType != SqlTypeName.TIMESTAMP) {
        return super.visitCall(call);
      }

      // The datetime argument might be another call to DATETIME_PLUS.
      // Recursively transform this as well.
      datetime = datetime.accept(this);

      // Check the type of the interval parameter.
      if (!isInterval(call.getOperands().get(1))) {
        throw new Util.FoundOne(call);
      }

      final boolean negate = call.getOperator() == SqlStdOperatorTable.MINUS_DATE;

      // TODO DX-13313: Support more types of interval expressions.

      // Note that while we can CAST the interval argument to an integer, we wouldn't be able
      // to represent this in a SQL dialect that doesn't support intervals (since there's no way
      // to cast an interval). We will only try to do this conversion if the interval is either
      // a literal, or a literal multiplied by an expression.
      final RexNode interval = call.getOperands().get(1);

      final RexNode intervalAsInt;
      final TimeUnit unit;
      if (interval instanceof RexLiteral) {
        unit = getTimeUnitFromIntervalLiteral(interval);
        intervalAsInt = convertIntervalLiteralToInteger(unit, interval, null, negate);
      } else if (interval instanceof RexCall
          && ((RexCall) interval).getOperator() == SqlStdOperatorTable.MULTIPLY) {
        // Identify which, if any operand is an interval literal.
        final RexCall multiplyOp = (RexCall) interval;
        final RexNode leftOp = multiplyOp.getOperands().get(0);
        final RexNode rightOp = multiplyOp.getOperands().get(1);

        if (isIntervalLiteral(leftOp)) {
          unit = getTimeUnitFromIntervalLiteral(leftOp);
          intervalAsInt = convertIntervalLiteralToInteger(unit, leftOp, rightOp, negate);
        } else if (isIntervalLiteral(rightOp)) {
          unit = getTimeUnitFromIntervalLiteral(rightOp);
          intervalAsInt = convertIntervalLiteralToInteger(unit, rightOp, leftOp, negate);
        } else {
          // Neither are single field interval literals. Don't convert.
          // We could do a deeper search if these are multiply functions but this wouldn't
          // be very common.
          throw new Util.FoundOne(call);
        }
      } else {
        throw new Util.FoundOne(call);
      }

      return builder.makeCall(SqlStdOperatorTable.TIMESTAMP_ADD,
          builder.makeFlag(unit), intervalAsInt, datetime);
    }
  }
}

  /**
   * Visitor class which transforms datetime_plus operators to timestamp_add operators.
   */
  private static class DateTimePlusToTsAddVisitor extends RexShuttle {
    private final RexBuilder builder;

    DateTimePlusToTsAddVisitor(RexBuilder builder) {
      this.builder = builder;
    }

    private static TimeUnit getTimeUnitFromIntervalLiteral(RexNode intervalLiteral) {
      final IntervalSqlType intervalType = (IntervalSqlType) intervalLiteral.getType();
      return intervalType.getIntervalQualifier().getUnit();
    }

    private RexNode convertIntervalLiteralToInteger(TimeUnit unit,
        RexNode intervalLiteral, RexNode coefficient, boolean negate) {
      final RexLiteral intervalAsInt = builder.makeExactLiteral(((RexLiteral) intervalLiteral)
          .getValueAs(BigDecimal.class).divide(unit.multiplier, 0, RoundingMode.HALF_DOWN));

      // No co-efficient. Just return the original literal.
      if (null == coefficient) {
        if (!negate)  {
          return intervalAsInt;
        }
        // Make the negation part of the integer literal.
        return builder.makeExactLiteral(((RexLiteral) intervalLiteral)
            .getValueAs(BigDecimal.class)
            .divide(unit.multiplier, 0, RoundingMode.HALF_DOWN)
            .negate());
      }

      RexNode result;
      // Unit literal. Omit.
      if (intervalAsInt.getValueAs(BigDecimal.class).equals(BigDecimal.ONE)) {
        result = coefficient;
      } else {
        // Multiply the interval-as-int by the coefficient.
        result = builder.makeCall(SqlStdOperatorTable.MULTIPLY, intervalAsInt, coefficient);
      }

      if (negate) {
        result = builder.makeCall(SqlStdOperatorTable.UNARY_MINUS, result);
      }

      return result;
    }

    @Override public RexNode visitCall(RexCall call) {
      // Only transform DATETIME_PLUS calls that are applied to a
      // datetime <op> single-field interval.
      if (call.getOperator() != SqlStdOperatorTable.DATETIME_PLUS
          && ((call.getOperator() != SqlStdOperatorTable.MINUS_DATE)
              || (call.getOperands().size() != 2)
              || !(
                  INTERVAL_TYPES.contains(
                      call.getOperands().get(1).getType().getSqlTypeName())))) {
        return super.visitCall(call);
      }

      RexNode datetime = call.getOperands().get(0);
      final SqlTypeName datetimeType = datetime.getType().getSqlTypeName();
      if (datetimeType != SqlTypeName.DATE
          && datetimeType != SqlTypeName.TIME
          && datetimeType != SqlTypeName.TIMESTAMP) {
        return super.visitCall(call);
      }

      // The datetime argument might be another call to DATETIME_PLUS.
      // Recursively transform this as well.
      datetime = datetime.accept(this);

      // Check the type of the interval parameter.
      if (!isInterval(call.getOperands().get(1))) {
        throw new Util.FoundOne(call);
      }

      final boolean negate = call.getOperator() == SqlStdOperatorTable.MINUS_DATE;

      // TODO DX-13313: Support more types of interval expressions.

      // Note that while we can CAST the interval argument to an integer, we wouldn't be able
      // to represent this in a SQL dialect that doesn't support intervals (since there's no way
      // to cast an interval). We will only try to do this conversion if the interval is either
      // a literal, or a literal multiplied by an expression.
      final RexNode interval = call.getOperands().get(1);
      final RexNode intervalAsInt;
      final TimeUnit unit;
      if (interval instanceof RexLiteral) {
        unit = getTimeUnitFromIntervalLiteral(interval);
        intervalAsInt = convertIntervalLiteralToInteger(unit, interval, null, negate);
      } else if (interval instanceof RexCall
          && ((RexCall) interval).getOperator() == SqlStdOperatorTable.MULTIPLY) {
        // Identify which, if any operand is an interval literal.
        final RexCall multiplyOp = (RexCall) interval;
        final RexNode leftOp = multiplyOp.getOperands().get(0);
        final RexNode rightOp = multiplyOp.getOperands().get(1);

        if (isIntervalLiteral(leftOp)) {
          unit = getTimeUnitFromIntervalLiteral(leftOp);
          intervalAsInt = convertIntervalLiteralToInteger(unit, leftOp, rightOp, negate);
        } else if (isIntervalLiteral(rightOp)) {
          unit = getTimeUnitFromIntervalLiteral(rightOp);
          intervalAsInt = convertIntervalLiteralToInteger(unit, rightOp, leftOp, negate);
        } else {
          // Neither are single field interval literals. Don't convert.
          // We could do a deeper search if these are multiply functions but this wouldn't
          // be very common.
          throw new Util.FoundOne(call);
        }
      } else {
        throw new Util.FoundOne(call);
      }

      return builder.makeCall(SqlStdOperatorTable.TIMESTAMP_ADD,
          builder.makeFlag(unit), intervalAsInt, datetime);
    }
  }
}
