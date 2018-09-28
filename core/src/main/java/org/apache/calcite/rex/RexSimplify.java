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
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.apache.calcite.rex.RexUnknownAs.FALSE;
>>>>>>> e84a51205 ([CALCITE-2604] When simplifying an expression, say whether an UNKNOWN value will be interpreted as is, or as TRUE or FALSE)
import static org.apache.calcite.rex.RexUnknownAs.UNKNOWN;

/**
 * Context required to simplify a row-expression.
 */
public class RexSimplify {
  private final boolean paranoid;
  public final RexBuilder rexBuilder;
  private final RelOptPredicateList predicates;
  /** How to treat UNKNOWN values, if one of the deprecated {@code
   * simplify} methods without an {@code unknownAs} argument is called. */
  final RexUnknownAs defaultUnknownAs;
  private final RexExecutor executor;
  private final Strong strong;

  /**
   * Creates a RexSimplify.
   *
   * @param rexBuilder Rex builder
   * @param predicates Predicates known to hold on input fields
   * @param executor Executor for constant reduction, not null
   */
  public RexSimplify(RexBuilder rexBuilder, RelOptPredicateList predicates,
                     RexExecutor executor) {
    this(rexBuilder, predicates, UNKNOWN, false, executor);
  }

  /** Internal constructor. */
  private RexSimplify(RexBuilder rexBuilder, RelOptPredicateList predicates,
      RexUnknownAs defaultUnknownAs, boolean paranoid, RexExecutor executor) {
    this.rexBuilder = Objects.requireNonNull(rexBuilder);
    this.predicates = Objects.requireNonNull(predicates);
    this.defaultUnknownAs = Objects.requireNonNull(defaultUnknownAs);
    this.paranoid = paranoid;
    this.executor = Objects.requireNonNull(executor);
    this.strong = new Strong();
  }

  @Deprecated // to be removed before 2.0
  public RexSimplify(RexBuilder rexBuilder, boolean unknownAsFalse,
      RexExecutor executor) {
    this(rexBuilder, RelOptPredicateList.EMPTY,
        RexUnknownAs.falseIf(unknownAsFalse), false, executor);
  }

  @Deprecated // to be removed before 2.0
  public RexSimplify(RexBuilder rexBuilder, RelOptPredicateList predicates,
      boolean unknownAsFalse, RexExecutor executor) {
    this(rexBuilder, predicates, RexUnknownAs.falseIf(unknownAsFalse),
        false, executor);
  }

  //~ Methods ----------------------------------------------------------------

  /** Returns a RexSimplify the same as this but with a specified
   * {@link #defaultUnknownAs} value.
   *
   * @deprecated Use methods with a {@link RexUnknownAs} argument, such as
   * {@link #simplify(RexNode, RexUnknownAs)}. */
  @Deprecated // to be removed before 2.0
  public RexSimplify withUnknownAsFalse(boolean unknownAsFalse) {
    final RexUnknownAs defaultUnknownAs = RexUnknownAs.falseIf(unknownAsFalse);
    return defaultUnknownAs == this.defaultUnknownAs
        ? this
        : new RexSimplify(rexBuilder, predicates, defaultUnknownAs, paranoid, executor);
  }

  /** Returns a RexSimplify the same as this but with a specified
   * {@link #predicates} value. */
  public RexSimplify withPredicates(RelOptPredicateList predicates) {
    return predicates == this.predicates
        ? this
        : new RexSimplify(rexBuilder, predicates, defaultUnknownAs, paranoid, executor);
  }

  /** Returns a RexSimplify the same as this but which verifies that
   * the expression before and after simplification are equivalent.
   *
   * @see #verify
   */
  public RexSimplify withParanoid(boolean paranoid) {
    return paranoid == this.paranoid
        ? this
        : new RexSimplify(rexBuilder, predicates, defaultUnknownAs, paranoid,
            executor);
  }

  /** Simplifies a boolean expression, always preserving its type and its
   * nullability.
   *
   * <p>This is useful if you are simplifying expressions in a
   * {@link Project}. */
  public RexNode simplifyPreservingType(RexNode e) {
    return simplifyPreservingType(e, defaultUnknownAs);
  }

  private RexNode simplifyPreservingType(RexNode e, RexUnknownAs unknownAs) {
    try {
      final RexNode e2 = simplifyUnknownAs(e, unknownAs);
      if (e2.getType() == e.getType()) {
        return e2;
      }
      final RexNode e3 = rexBuilder.makeCast(e.getType(), e2, true);
      if (e3.equals(e)) {
        return e;
      }
      return e3;
    } catch (ClassCastException ex) {
      return e;
    }
  }

  /**
   * Simplifies a boolean expression.
   *
   * <p>In particular:</p>
   * <ul>
   * <li>{@code simplify(x = 1 AND y = 2 AND NOT x = 1)}
   * returns {@code y = 2}</li>
   * <li>{@code simplify(x = 1 AND FALSE)}
   * returns {@code FALSE}</li>
   * </ul>
   *
   * <p>Handles UNKNOWN values using the policy specified when you created this
   * {@code RexSimplify}. Unless you used a deprecated constructor, that policy
   * is {@link RexUnknownAs#UNKNOWN}.
   *
   * <p>If the expression is a predicate in a WHERE clause, consider instead
   * using {@link #simplifyUnknownAsFalse(RexNode)}.
   *
   * @param e Expression to simplify
   */
  public RexNode simplify(RexNode e) {
    return simplifyUnknownAs(e, defaultUnknownAs);
  }

  /** As {@link #simplify(RexNode)}, but for a boolean expression
   * for which a result of UNKNOWN will be treated as FALSE.
   *
   * <p>Use this form for expressions on a WHERE, ON, HAVING or FILTER(WHERE)
   * clause.
   *
   * <p>This may allow certain additional simplifications. A result of UNKNOWN
   * may yield FALSE, however it may still yield UNKNOWN. (If the simplified
   * expression has type BOOLEAN NOT NULL, then of course it can only return
   * FALSE.) */
  public final RexNode simplifyUnknownAsFalse(RexNode e) {
    return simplifyUnknownAs(e, FALSE);
  }

  /** As {@link #simplify(RexNode)}, but specifying how UNKNOWN values are to be
   * treated.
   *
   * <p>If UNKNOWN is treated as FALSE, this may allow certain additional
   * simplifications. A result of UNKNOWN may yield FALSE, however it may still
   * yield UNKNOWN. (If the simplified expression has type BOOLEAN NOT NULL,
   * then of course it can only return FALSE.) */
  public RexNode simplifyUnknownAs(RexNode e, RexUnknownAs unknownAs) {
    return verify(e, unknownAs,
        simplifier -> simplifier.simplify(e, unknownAs));
  }

  /** Internal method to simplify an expression.
   *
   * <p>Unlike the public {@link #simplify(RexNode)}
   * and {@link #simplifyUnknownAsFalse(RexNode)} methods,
   * never calls {@link #verify(RexNode, RexUnknownAs, Function)}.
   * Verify adds an overhead that is only acceptable for a top-level call.
   */
  RexNode simplify(RexNode e, RexUnknownAs unknownAs) {
    try {
      if (strong.isNull(e)) {
        // Only boolean NULL (aka UNKNOWN) can be converted to FALSE. Even in
        // unknownAs=FALSE mode, we must not convert a NULL integer (say) to FALSE
        if (e.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
          switch (unknownAs) {
          case FALSE:
          case TRUE:
            return rexBuilder.makeLiteral(unknownAs.toBoolean());
          }
        }
        return rexBuilder.makeNullLiteral(e.getType());
      }
      switch (e.getKind()) {
      case AND:
        return simplifyAnd((RexCall) e, unknownAs);
      case OR:
        return simplifyOr((RexCall) e, unknownAs);
      case NOT:
        return simplifyNot((RexCall) e, unknownAs);
      case CASE:
        return simplifyCase((RexCall) e, unknownAs);
      case COALESCE:
        return simplifyCoalesce((RexCall) e);
      case CAST:
        return simplifyCast((RexCall) e);
      case CEIL:
      case FLOOR:
        return simplifyCeilFloor((RexCall) e);
      case IS_NULL:
      case IS_NOT_NULL:
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
        assert e instanceof RexCall;
        return simplifyIs((RexCall) e);
      case EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case NOT_EQUALS:
        return simplifyComparison((RexCall) e, unknownAs);
      default:
        return e;
      }
    } catch (ClassCastException ex) {
      return e;
    }

    if (e.getType().getSqlTypeName() == SqlTypeName.VARIANT) {
      // Upstream has many fixes that makes this code path "work".
      // For now we can just return the expression, since there is no RexLiteral for a Variant.
      // When we cherry-pick [CALCITE-4918] Add a VARIANT data type we can remove this
      return e;
    }

    try {
      if (strong.isNull(e)) {
        // Only boolean NULL (aka UNKNOWN) can be converted to FALSE. Even in
        // unknownAs=FALSE mode, we must not convert a NULL integer (say) to FALSE
        if (e.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
          switch (unknownAs) {
          case FALSE:
          case TRUE:
            return rexBuilder.makeLiteral(unknownAs.toBoolean());
          }
        }
        return rexBuilder.makeNullLiteral(e.getType());
      }
      switch (e.getKind()) {
      case AND:
        return simplifyAnd((RexCall) e, unknownAs);
      case OR:
        return simplifyOr((RexCall) e, unknownAs);
      case NOT:
        return simplifyNot((RexCall) e, unknownAs);
      case CASE:
        return simplifyCase((RexCall) e, unknownAs);
      case COALESCE:
        return simplifyCoalesce((RexCall) e);
      case CAST:
        return simplifyCast((RexCall) e);
      case CEIL:
      case FLOOR:
        return simplifyCeilFloor((RexCall) e);
      case IS_NULL:
      case IS_NOT_NULL:
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
        assert e instanceof RexCall;
        return simplifyIs((RexCall) e, unknownAs);
      case EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case NOT_EQUALS:
        return simplifyComparison((RexCall) e, unknownAs);
      case MINUS_PREFIX:
        return simplifyUnaryMinus((RexCall) e, unknownAs);
      case PLUS_PREFIX:
        return simplifyUnaryPlus((RexCall) e, unknownAs);
      default:
        if (e.getClass() == RexCall.class) {
          return simplifyGenericNode((RexCall) e);
        } else {
          return e;
        }
      }
    } catch (ClassCastException ex) {
      return e;
    } finally {
      currentDepth--;
    }
  }

  /**
   * Runs simplification inside a non-specialized node.
   */
  private RexNode simplifyGenericNode(RexCall e) {
    final List<RexNode> operands = new ArrayList<>(e.operands);
    simplifyList(operands, UNKNOWN);
    if (e.operands.equals(operands)) {
      return e;
    }
    return rexBuilder.makeCall(e.getType(), e.getOperator(), operands);
  }

  // e must be a comparison (=, >, >=, <, <=, !=)
  private RexNode simplifyComparison(RexCall e, RexUnknownAs unknownAs) {
    //noinspection unchecked
    return simplifyComparison(e, unknownAs, Comparable.class);
  }

  // e must be a comparison (=, >, >=, <, <=, !=)
  private <C extends Comparable<C>> RexNode simplifyComparison(RexCall e,
      RexUnknownAs unknownAs, Class<C> clazz) {
    final List<RexNode> operands = new ArrayList<>(e.operands);
>>>>>>> e84a51205 ([CALCITE-2604] When simplifying an expression, say whether an UNKNOWN value will be interpreted as is, or as TRUE or FALSE)
    simplifyList(operands, UNKNOWN);

    // Simplify "x <op> x"
    final RexNode o0 = operands.get(0);
    final RexNode o1 = operands.get(1);
    if (o0.equals(o1)
            && RexUtil.isDeterministic(o1)
        && (unknownAs == FALSE
            || (!o0.getType().isNullable()
                && !o1.getType().isNullable()))) {
      switch (e.getKind()) {
      case EQUALS:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
        // "x = x" simplifies to "x is not null" (similarly <= and >=)
        return simplify(
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, o0), unknownAs);
      default:
        // "x != x" simplifies to "false" (similarly < and >)
        return rexBuilder.makeLiteral(false);
      }
    }

    if (o0.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      Comparison cmp = Comparison.of(rexBuilder.makeCall(e.getOperator(), o0, o1), node -> true);
      if (cmp != null) {
        if (cmp.literal.isAlwaysTrue()) {
          switch (cmp.kind) {
          case GREATER_THAN_OR_EQUAL:
          case EQUALS: // x=true
            return cmp.ref;
          case LESS_THAN:
          case NOT_EQUALS: // x!=true
            return simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, cmp.ref), unknownAs);
          case GREATER_THAN:
            /* this is false, but could be null if x is null */
            if (!cmp.ref.getType().isNullable()) {
              return rexBuilder.makeLiteral(false);
            }
            break;
          case LESS_THAN_OR_EQUAL:
            /* this is true, but could be null if x is null */
            if (!cmp.ref.getType().isNullable()) {
              return rexBuilder.makeLiteral(true);
            }
            break;
          }
        }
        if (cmp.literal.isAlwaysFalse()) {
          switch (cmp.kind) {
          case EQUALS:
          case LESS_THAN_OR_EQUAL:
            return simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, cmp.ref), unknownAs);
          case NOT_EQUALS:
          case GREATER_THAN:
            return cmp.ref;
          case GREATER_THAN_OR_EQUAL:
            /* this is true, but could be null if x is null */
            if (!cmp.ref.getType().isNullable()) {
              return rexBuilder.makeLiteral(true);
            }
            break;
          case LESS_THAN:
            /* this is false, but could be null if x is null */
            if (!cmp.ref.getType().isNullable()) {
              return rexBuilder.makeLiteral(false);
            }
            break;
          }
        }
      }
    }


    // Simplify "<literal1> <op> <literal2>"
    // For example, "1 = 2" becomes FALSE;
    // "1 != 1" becomes FALSE;
    // "1 != NULL" becomes UNKNOWN (or FALSE if unknownAsFalse);
    // "1 != '1'" is unchanged because the types are not the same.
    if (o0.isA(SqlKind.LITERAL)
        && o1.isA(SqlKind.LITERAL)
        && SqlTypeUtil.equalSansNullability(rexBuilder.getTypeFactory(),
              o0.getType(), o1.getType())) {
      final C v0 = ((RexLiteral) o0).getValueAs(clazz);
      final C v1 = ((RexLiteral) o1).getValueAs(clazz);
      if (v0 == null || v1 == null) {
        return unknownAs == FALSE
            ? rexBuilder.makeLiteral(false)
            : rexBuilder.makeNullLiteral(e.getType());
      }
      final int comparisonResult = v0.compareTo(v1);
      switch (e.getKind()) {
      case EQUALS:
        return rexBuilder.makeLiteral(comparisonResult == 0);
      case GREATER_THAN:
        return rexBuilder.makeLiteral(comparisonResult > 0);
      case GREATER_THAN_OR_EQUAL:
        return rexBuilder.makeLiteral(comparisonResult >= 0);
      case LESS_THAN:
        return rexBuilder.makeLiteral(comparisonResult < 0);
      case LESS_THAN_OR_EQUAL:
        return rexBuilder.makeLiteral(comparisonResult <= 0);
      case NOT_EQUALS:
        return rexBuilder.makeLiteral(comparisonResult != 0);
      default:
        throw new AssertionError();
      }
    }

    // If none of the arguments were simplified, return the call unchanged.
    final RexNode e2;
    if (operands.equals(e.operands)) {
      e2 = e;
    } else {
      e2 = rexBuilder.makeCall(e.op, operands);
    }
    return simplifyUsingPredicates(e2, clazz);
  }

  /**
   * Simplifies a conjunction of boolean expressions.
   */
  @Deprecated // to be removed before 2.0
  public RexNode simplifyAnds(Iterable<? extends RexNode> nodes) {
    return simplifyAnds(nodes, defaultUnknownAs);
  }

  // package-protected only for a deprecated method; treat as private
  RexNode simplifyAnds(Iterable<? extends RexNode> nodes,
      RexUnknownAs unknownAs) {
    try {
      final List<RexNode> terms = new ArrayList<>();
      final List<RexNode> notTerms = new ArrayList<>();
      for (RexNode e : nodes) {
        RelOptUtil.decomposeConjunction(e, terms, notTerms);
      }
      simplifyList(terms, UNKNOWN);
      simplifyList(notTerms, UNKNOWN);
      if (unknownAs == FALSE) {
        return simplifyAnd2ForUnknownAsFalse(terms, notTerms);
      }
      return simplifyAnd2(terms, notTerms);
    } catch (ClassCastException e) {
      return RexUtil.composeConjunction(rexBuilder, nodes);
    }
  }

  private void simplifyList(List<RexNode> terms, RexUnknownAs unknownAs) {
    unknownAs = UNKNOWN; // TODO
    for (int i = 0; i < terms.size(); i++) {
      terms.set(i, simplify(terms.get(i), unknownAs));
    }
  }

  private RexNode simplifyNot(RexCall call, RexUnknownAs unknownAs) {
    final RexNode a = call.getOperands().get(0);
    switch (a.getKind()) {
    case NOT:
      // NOT NOT x ==> x
      return simplify(((RexCall) a).getOperands().get(0), unknownAs);
    case LITERAL:
      if (a.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
          && !RexLiteral.isNullLiteral(a)) {
        return rexBuilder.makeLiteral(!RexLiteral.booleanValue(a));
      }
    }
    final SqlKind negateKind = a.getKind().negate();
    if (a.getKind() != negateKind) {
      return simplify(
          rexBuilder.makeCall(RexUtil.op(negateKind),
              ((RexCall) a).getOperands()), unknownAs);
    }
    final SqlKind negateKind2 = a.getKind().negateNullSafe();
    if (a.getKind() != negateKind2) {
      return simplify(
          rexBuilder.makeCall(RexUtil.op(negateKind2),
              ((RexCall) a).getOperands()), unknownAs);
    }
    if (a.getKind() == SqlKind.AND) {
      // NOT distributivity for AND
      final List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        newOperands.add(
            simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand),
                unknownAs));
      }
      return simplify(
          rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands), unknownAs);
    }
    if (a.getKind() == SqlKind.OR) {
      // NOT distributivity for OR
      final List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        newOperands.add(
            simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand),
                unknownAs));
      }
      return simplify(
          rexBuilder.makeCall(SqlStdOperatorTable.AND, newOperands), unknownAs);
    }
    if (a.getKind() == SqlKind.CASE) {
      final List<RexNode> newOperands = new ArrayList<>();
      List<RexNode> operands = ((RexCall) a).getOperands();
      for (int i = 0; i < operands.size(); i += 2) {
        if (i + 1 == operands.size()) {
          newOperands.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operands.get(i)));
        } else {
          newOperands.add(operands.get(i));
          newOperands.add(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operands.get(i + 1)));
        }
      }
      return simplify(
          rexBuilder.makeCall(SqlStdOperatorTable.CASE, newOperands), unknownAs);
    }
    RexNode a2 = simplify(a, unknownAs.negate());
    if (a == a2) {
      return call;
    }
    if (a2.isAlwaysTrue()) {
      return rexBuilder.makeLiteral(false);
    }
    if (a2.isAlwaysFalse()) {
      return rexBuilder.makeLiteral(true);
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, a2);
  }

  private RexNode simplifyUnaryMinus(RexCall call, RexUnknownAs unknownAs) {
    final RexNode a = call.getOperands().get(0);
    if (a.getKind() == SqlKind.MINUS_PREFIX) {
      // -(-(x)) ==> x
      return simplify(((RexCall) a).getOperands().get(0), unknownAs);
    }
    return simplifyGenericNode(call);
  }

  private RexNode simplifyUnaryPlus(RexCall call, RexUnknownAs unknownAs) {
    return simplify(call.getOperands().get(0), unknownAs);
  }

  private RexNode simplifyIs(RexCall call, RexUnknownAs unknownAs) {
    final SqlKind kind = call.getKind();
    final RexNode a = call.getOperands().get(0);
    final RexNode simplified = simplifyIs2(kind, a);
    if (simplified != null) {
      return simplified;
    }
    return call;
  }

  private RexNode simplifyIs2(SqlKind kind, RexNode a) {
    final RexNode simplified;
    switch (kind) {
    case IS_NULL:
      // x IS NULL ==> FALSE (if x is not nullable)
      simplified = simplifyIsNull(a);
      if (simplified != null) {
        return simplified;
      }
      break;
    case IS_NOT_NULL:
      // x IS NOT NULL ==> TRUE (if x is not nullable)
      simplified = simplifyIsNotNull(a);
      if (simplified != null) {
        return simplified;
      }
      break;
    case IS_TRUE:
    case IS_NOT_FALSE:
      // x IS TRUE ==> x (if x is not nullable)
      // x IS NOT FALSE ==> x (if x is not nullable)
      if (!a.getType().isNullable()) {
        return simplify(a, UNKNOWN);
      }
    case IS_FALSE:
    case IS_NOT_TRUE:
      // x IS NOT TRUE ==> NOT x (if x is not nullable)
      // x IS FALSE ==> NOT x (if x is not nullable)
      if (!a.getType().isNullable()) {
        return simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, a),
            UNKNOWN);
      }
      break;
    }
    switch (a.getKind()) {
    case NOT:
      // (NOT x) IS TRUE ==> x IS FALSE
      // Similarly for IS NOT TRUE, IS FALSE, etc.
      //
      // Note that
      //   (NOT x) IS TRUE !=> x IS FALSE
      // because of null values.
      final SqlOperator notKind = RexUtil.op(kind.negateNullSafe());
      final RexNode arg = ((RexCall) a).operands.get(0);
      return simplify(rexBuilder.makeCall(notKind, arg), UNKNOWN);
    }
    RexNode a2 = simplify(a, UNKNOWN);
    if (a != a2) {
      return rexBuilder.makeCall(RexUtil.op(kind), ImmutableList.of(a2));
    }
    return null; // cannot be simplified
  }

  private RexNode simplifyIsNotNull(RexNode a) {
    // Simplify the argument first,
    // call ourselves recursively to see whether we can make more progress.
    // For example, given
    // "(CASE WHEN FALSE THEN 1 ELSE 2) IS NOT NULL" we first simplify the
    // argument to "2", and only then we can simplify "2 IS NOT NULL" to "TRUE".
    a = simplify(a, UNKNOWN);
    if (!a.getType().isNullable() && isSafeExpression(a)) {
      return rexBuilder.makeLiteral(true);
    }
    switch (Strong.policy(a.getKind())) {
    case NOT_NULL:
      return rexBuilder.makeLiteral(true);
    case ANY:
      // "f" is a strong operator, so "f(operand0, operand1) IS NOT NULL"
      // simplifies to "operand0 IS NOT NULL AND operand1 IS NOT NULL"
      final List<RexNode> operands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        final RexNode simplified = simplifyIsNotNull(operand);
        if (simplified == null) {
          operands.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
        } else if (simplified.isAlwaysFalse()) {
          return rexBuilder.makeLiteral(false);
        } else {
          operands.add(simplified);
        }
      }
      return RexUtil.composeConjunction(rexBuilder, operands);
    case CUSTOM:
      switch (a.getKind()) {
      case LITERAL:
        return rexBuilder.makeLiteral(!((RexLiteral) a).isNull());
      default:
        throw new AssertionError("every CUSTOM policy needs a handler, "
            + a.getKind());
      }
    case AS_IS:
    default:
      return null;
    }
  }

  private RexNode simplifyIsNull(RexNode a) {
    if (!a.getType().isNullable()) {
      return rexBuilder.makeLiteral(false);
    }
    if (RexUtil.isNull(a)) {
      return rexBuilder.makeLiteral(true);
    }
    switch (Strong.policy(a.getKind())) {
    case NOT_NULL:
      return rexBuilder.makeLiteral(false);
    case ANY:
      // "f" is a strong operator, so "f(operand0, operand1) IS NULL" simplifies
      // to "operand0 IS NULL OR operand1 IS NULL"
      final List<RexNode> operands = new ArrayList<>();
      for (RexNode operand : ((RexCall) a).getOperands()) {
        final RexNode simplified = simplifyIsNull(operand);
        if (simplified == null) {
          operands.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand));
        } else {
          operands.add(simplified);
        }
      }
      return RexUtil.composeDisjunction(rexBuilder, operands, false);
    case AS_IS:
    default:
      return null;
    }
  }

  private RexNode simplifyCoalesce(RexCall call) {
    final Set<String> digests = new HashSet<>();
    final List<RexNode> operands = new ArrayList<>();
    for (RexNode operand : call.getOperands()) {
      operand = simplify(operand, UNKNOWN);
      if (!RexUtil.isNull(operand)
              && digests.add(operand.toString())) {
        operands.add(operand);
      }
      if (!operand.getType().isNullable()) {
        break;
      }
    }
    switch (operands.size()) {
    case 0:
      return rexBuilder.makeNullLiteral(call.type);
    case 1:
      return operands.get(0);
    default:
      if (operands.equals(call.operands)) {
        return call;
      }
      return call.clone(call.type, operands);
    }
  }

  private RexNode simplifyCase(RexCall call, RexUnknownAs unknownAs) {
    final List<RexNode> operands = call.getOperands();
    final List<RexNode> newOperands = new ArrayList<>();
    final Set<String> values = new HashSet<>();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (RexUtil.isCasePredicate(call, i)) {
        if (operand.isAlwaysTrue()) {
          // Predicate is always TRUE. Make value the ELSE and quit.
          newOperands.add(operands.get(++i));
          if (unknownAs == FALSE && RexUtil.isNull(operands.get(i))) {
            values.add(rexBuilder.makeLiteral(false).toString());
          } else {
            values.add(operands.get(i).toString());
          }
          break;
        } else if (operand.isAlwaysFalse() || RexUtil.isNull(operand)) {
          // Predicate is always FALSE or NULL. Skip predicate and value.
          ++i;
          continue;
        }
      } else {
        if (unknownAs == FALSE && RexUtil.isNull(operand)) {
          values.add(rexBuilder.makeLiteral(false).toString());
        } else {
          operands.add(simplified);
        }
      }
      return RexUtil.composeDisjunction(rexBuilder, operands, false);
    case AS_IS:
    default:
      return null;
    }
>>>>>>> e84a51205 ([CALCITE-2604] When simplifying an expression, say whether an UNKNOWN value will be interpreted as is, or as TRUE or FALSE)
  }

  private <C extends Comparable<C>> RexNode simplifyUsingPredicates(RexNode e,
      Class<C> clazz) {
    final Comparison comparison = Comparison.of(e);
    // Check for comparison with null values
    if (comparison == null
        || comparison.literal.getValue() == null) {
      return e;
    }

    final C v0 = comparison.literal.getValueAs(clazz);
    final Range<C> range = range(comparison.kind, v0);
    final Range<C> range2 =
        residue(comparison.ref, range, predicates.pulledUpPredicates,
            clazz);
    if (range2 == null) {
      // Term is impossible to satisfy given these predicates
      return rexBuilder.makeLiteral(false);
    } else if (range2.equals(range)) {
      // no change
      return e;
    } else if (range2.equals(Range.all())) {
      // Term is always satisfied given these predicates
      return rexBuilder.makeLiteral(true);
    } else if (range2.lowerEndpoint().equals(range2.upperEndpoint())) {
      if (range2.lowerBoundType() == BoundType.OPEN
          || range2.upperBoundType() == BoundType.OPEN) {
        // range is a point, but does not include its endpoint, therefore is
        // effectively empty
        return rexBuilder.makeLiteral(false);
      }
      // range is now a point; it's worth simplifying
      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, comparison.ref,
          rexBuilder.makeLiteral(range2.lowerEndpoint(),
              comparison.literal.getType(), comparison.literal.getTypeName()));
    } else {
      // range has been reduced but it's not worth simplifying
      return e;
    }
  }

  /** Weakens a term so that it checks only what is not implied by predicates.
   *
   * <p>The term is broken into "ref comparison constant",
   * for example "$0 &lt; 5".
   *
   * <p>Examples:
   * <ul>
   *
   * <li>{@code residue($0 < 10, [$0 < 5])} returns {@code true}
   *
   * <li>{@code residue($0 < 10, [$0 < 20, $0 > 0])} returns {@code $0 < 10}
   * </ul>
   */
  private <C extends Comparable<C>> Range<C> residue(RexNode ref, Range<C> r0,
      List<RexNode> predicates, Class<C> clazz) {
    Range<C> result = r0;
    for (RexNode predicate : predicates) {
      switch (predicate.getKind()) {
      case EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        final RexCall call = (RexCall) predicate;
        if (call.operands.get(0).equals(ref)
            && call.operands.get(1) instanceof RexLiteral) {
          final RexLiteral literal = (RexLiteral) call.operands.get(1);
          final C c1 = literal.getValueAs(clazz);
          final Range<C> r1 = range(predicate.getKind(), c1);
          if (result.encloses(r1)) {
            // Given these predicates, term is always satisfied.
            // e.g. r0 is "$0 < 10", r1 is "$0 < 5"
            result = Range.all();
            continue;
          }
          if (result.isConnected(r1)) {
            result = result.intersection(r1);
            continue;
          }
          // Ranges do not intersect. Return null meaning the empty range.
          return null;
        }
      }
    }
    return result;
  }

  /** Simplifies OR(x, x) into x, and similar.
   * The simplified expression returns UNKNOWN values as is (not as FALSE). */
<<<<<<< HEAD
  @Deprecated // to be removed before 2.0
=======
  public RexNode simplifyOr(RexCall call) {
    return simplifyOr(call, UNKNOWN);
  }

  private RexNode simplifyOr(RexCall call, RexUnknownAs unknownAs) {
    try {
      assert call.getKind() == SqlKind.OR;
      final List<RexNode> terms = RelOptUtil.disjunctions(call);
      return simplifyOrs(terms, unknownAs);
    } catch (ClassCastException ex) {
      return call;
    }
  }

  /** Simplifies a list of terms and combines them into an OR.
   * Modifies the list in place.
   * The simplified expression returns UNKNOWN values as is (not as FALSE). */
  public RexNode simplifyOrs(List<RexNode> terms) {
    return simplifyOrs(terms, UNKNOWN);
  }

  /** Simplifies a list of terms and combines them into an OR.
   * Modifies the list in place. */
  public RexNode simplifyOrs(List<RexNode> terms, RexUnknownAs unknownAs) {
    final List<RexNode> originalTerms = new ArrayList<>(terms);
    try {
      if (paranoid) {
        final RexNode before = RexUtil.composeDisjunction(rexBuilder, terms);
        return verify(before, unknownAs,
          simplifier -> simplifier.simplifyOrs(terms, unknownAs));
      }
      for (int i = 0; i < terms.size(); i++) {
        final RexNode term = simplify(terms.get(i), unknownAs);
        switch (term.getKind()) {
        case LITERAL:
          if (RexLiteral.isNullLiteral(term)) {
            if (unknownAs == FALSE) {
              terms.remove(i);
              --i;
              continue;
            }
          } else {
            if (RexLiteral.booleanValue(term)) {
              return term; // true
            } else {
              terms.remove(i);
              --i;
              continue;
            }
          }
        }
        terms.set(i, term);
      }
      return RexUtil.composeDisjunction(rexBuilder, terms);
    } catch (ClassCastException ex) {
      return RexUtil.composeDisjunction(rexBuilder, originalTerms);
    }
  }

  private RexNode verify(RexNode before, RexUnknownAs unknownAs,
      Function<RexSimplify, RexNode> simplifier) {
    final RexNode simplified = simplifier.apply(withParanoid(false));
    if (!paranoid) {
      return simplified;
    }
    final RexAnalyzer foo0 = new RexAnalyzer(before, predicates);
    final RexAnalyzer foo1 = new RexAnalyzer(simplified, predicates);
    if (foo0.unsupportedCount > 0 || foo1.unsupportedCount > 0) {
      // Analyzer cannot handle this expression currently
      return simplified;
    }
    if (!foo0.variables.containsAll(foo1.variables)) {
      throw new AssertionError("variable mismatch: "
          + before + " has " + foo0.variables + ", "
          + simplified + " has " + foo1.variables);
    }
    assignment_loop:
    for (Map<RexNode, Comparable> map : foo0.assignments()) {
      for (RexNode predicate : predicates.pulledUpPredicates) {
        final Comparable v = RexInterpreter.evaluate(predicate, map);
        if (!v.equals(true)) {
          continue assignment_loop;
        }
      }
      Comparable v0 = RexInterpreter.evaluate(foo0.e, map);
      if (v0 == null) {
        throw new AssertionError("interpreter returned null for " + foo0.e);
      }
      Comparable v1 = RexInterpreter.evaluate(foo1.e, map);
      if (v1 == null) {
        throw new AssertionError("interpreter returned null for " + foo1.e);
      }
      if (before.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
        switch (unknownAs) {
        case FALSE:
        case TRUE:
          if (v0 == NullSentinel.INSTANCE) {
            v0 = unknownAs.toBoolean();
          }
          if (v1 == NullSentinel.INSTANCE) {
            v1 = unknownAs.toBoolean();
          }
        }
      }
      if (!v0.equals(v1)) {
        throw new AssertionError("result mismatch: when applied to " + map
            + ", " + before + " yielded " + v0
            + ", and " + simplified + " yielded " + v1);
      }
    }
    return simplified;
  }

  private RexNode simplifyCast(RexCall e) {
    RexNode operand = e.getOperands().get(0);
    operand = simplify(operand, UNKNOWN);
    if (sameTypeOrNarrowsNullability(e.getType(), operand.getType())) {
      return operand;
    }
    if (RexUtil.isLosslessCast(operand)) {
      // x :: y below means cast(x as y) (which is PostgreSQL-specifiic cast by the way)
      // A) Remove lossless casts:
      // A.1) intExpr :: bigint :: int => intExpr
      // A.2) char2Expr :: char(5) :: char(2) => char2Expr
      // B) There are cases when we can't remove two casts, but we could probably remove inner one
      // B.1) char2expression :: char(4) :: char(5) -> char2expression :: char(5)
      // B.2) char2expression :: char(10) :: char(5) -> char2expression :: char(5)
      // B.3) char2expression :: varchar(10) :: char(5) -> char2expression :: char(5)
      // B.4) char6expression :: varchar(10) :: char(5) -> char6expression :: char(5)
      // C) Simplification is not possible:
      // C.1) char6expression :: char(3) :: char(5) -> must not be changed
      //      the input is truncated to 3 chars, so we can't use char6expression :: char(5)
      // C.2) varchar2Expr :: char(5) :: varchar(2) -> must not be changed
      //      the input have to be padded with spaces (up to 2 chars)
      // C.3) char2expression :: char(4) :: varchar(5) -> must not be changed
      //      would not have the padding

      // The approach seems to be:
      // 1) Ensure inner cast is lossless (see if above)
      // 2) If operand of the inner cast has the same type as the outer cast,
      //    remove two casts except C.2 or C.3-like pattern (== inner cast is CHAR)
      // 3) If outer cast is lossless, remove inner cast (B-like cases)

      // Here we try to remove two casts in one go (A-like cases)
      RexNode intExpr = ((RexCall) operand).operands.get(0);
      // intExpr == CHAR detects A.1
      // operand != CHAR detects C.2
      if ((intExpr.getType().getSqlTypeName() == SqlTypeName.CHAR
          || operand.getType().getSqlTypeName() != SqlTypeName.CHAR)
          && sameTypeOrNarrowsNullability(e.getType(), intExpr.getType())) {
        return intExpr;
      }
      // Here we try to remove inner cast (B-like cases)
      if (RexUtil.isLosslessCast(intExpr.getType(), operand.getType())
          && (e.getType().getSqlTypeName() == operand.getType().getSqlTypeName()
          || e.getType().getSqlTypeName() == SqlTypeName.CHAR
          || operand.getType().getSqlTypeName() != SqlTypeName.CHAR)) {
        return rexBuilder.makeCast(e.getType(), intExpr);
      }
    }
    switch (operand.getKind()) {
    case LITERAL:
      final RexLiteral literal = (RexLiteral) operand;
      final Comparable value = literal.getValueAs(Comparable.class);
      final SqlTypeName typeName = literal.getTypeName();

      // First, try to remove the cast without changing the value.
      // makeCast and canRemoveCastFromLiteral have the same logic, so we are
      // sure to be able to remove the cast.
      if (rexBuilder.canRemoveCastFromLiteral(e.getType(), value, typeName)) {
        return rexBuilder.makeCast(e.getType(), operand);
      }

      // Next, try to convert the value to a different type,
      // e.g. CAST('123' as integer)
      switch (literal.getTypeName()) {
      case TIME:
        switch (e.getType().getSqlTypeName()) {
        case TIMESTAMP:
          return e;
        }
        break;
      }
      final List<RexNode> reducedValues = new ArrayList<>();
      executor.reduce(rexBuilder, ImmutableList.of(e), reducedValues);
      return Objects.requireNonNull(
          Iterables.getOnlyElement(reducedValues));
    default:
<<<<<<< HEAD
      if (operand == e.getOperands().get(0)) {
        return e;
      } else {
        return rexBuilder.makeCast(e.getType(), operand);
      }
=======
      if (operand.getType().equals(e.getType())) {
        return simplify(operand, UNKNOWN);
      }
      return e;
    }
  }

  /** Tries to simplify CEIL/FLOOR function on top of CEIL/FLOOR.
   *
   * <p>Examples:
   * <ul>
   *
   * <li>{@code ceil(floor($0, flag(hour)), flag(day))} returns {@code ceil($0, flag(day))}
   *
   * <li>{@code ceil(ceil($0, flag(second)), flag(day))} returns {@code ceil($0, flag(day))}
   *
   * <li>{@code floor(ceil($0, flag(day)), flag(second))} does not change
   *
   * </ul>
   */
  private RexNode simplifyCeilFloor(RexCall e) {
    if (e.getOperands().size() != 2) {
      // Bail out since we only simplify ceil/floor <date>
      return e;
    }
    final RexNode operand = simplify(e.getOperands().get(0), UNKNOWN);
    if (e.getKind() == operand.getKind()) {
      assert e.getKind() == SqlKind.CEIL || e.getKind() == SqlKind.FLOOR;
      // CEIL/FLOOR on top of CEIL/FLOOR
      final RexCall child = (RexCall) operand;
      if (child.getOperands().size() != 2) {
        // Bail out since we only simplify ceil/floor <date>
        return e;
      }
      final RexLiteral parentFlag = (RexLiteral) e.operands.get(1);
      final TimeUnitRange parentFlagValue = (TimeUnitRange) parentFlag.getValue();
      final RexLiteral childFlag = (RexLiteral) child.operands.get(1);
      final TimeUnitRange childFlagValue = (TimeUnitRange) childFlag.getValue();
      if (parentFlagValue != null && childFlagValue != null) {
        if (canRollUp(parentFlagValue.startUnit, childFlagValue.startUnit)) {
          return e.clone(e.getType(),
              ImmutableList.of(child.getOperands().get(0), parentFlag));
        }
      }
    }
    return e.clone(e.getType(),
        ImmutableList.of(operand, e.getOperands().get(1)));
  }

  /** Method that returns whether we can rollup from inner time unit
   * to outer time unit. */
  private static boolean canRollUp(TimeUnit outer, TimeUnit inner) {
    // Special handling for QUARTER as it is not in the expected
    // order in TimeUnit
    switch (outer) {
    case YEAR:
    case MONTH:
    case DAY:
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      switch (inner) {
      case YEAR:
      case QUARTER:
      case MONTH:
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
      case MILLISECOND:
      case MICROSECOND:
        if (inner == TimeUnit.QUARTER) {
          return outer == TimeUnit.YEAR;
        }
        return outer.ordinal() <= inner.ordinal();
      }
      break;
    case QUARTER:
      switch (inner) {
      case QUARTER:
      case MONTH:
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
      case MILLISECOND:
      case MICROSECOND:
        return true;
      }
    }
    return false;
  }

  /** Removes any casts that change nullability but not type.
   *
   * <p>For example, {@code CAST(1 = 0 AS BOOLEAN)} becomes {@code 1 = 0}. */
  public RexNode removeNullabilityCast(RexNode e) {
    return RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), e);
  }

  private static <C extends Comparable<C>> RexNode processRange(
      RexBuilder rexBuilder, List<RexNode> terms,
      RangeTerms rangeTerms, RexNode term,
      Comparison comparison, RexSimplify simplify, Class<C> clazz) {
    Pair<Range<C>, List<RexNode>> p = rangeTerms.get(comparison.ref);
    SqlKind sqlKind = comparison.kind;
    final C v0 = comparison.literal.getValueAs(clazz);
    if (p == null) {
      rangeTerms.put(comparison.ref,
          (List<RexNode>) ImmutableList.of(term),
              comparison.kind, comparison, simplify, clazz);
    } else {
      // Exists
      boolean removeUpperBound = false;
      boolean removeLowerBound = false;
      Range<C> r = p.left;
      final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
      switch (sqlKind) {
      case EQUALS:
        if (!r.contains(v0)) {
          // Range is empty, not satisfiable
          return rexBuilder.makeLiteral(false);
        }
        rangeTerms.put(comparison.ref.toString(),
=======
        rangeTerms.put(comparison.ref,
            Range.singleton(v0), (List<RexNode>) ImmutableList.of(term));
        // remove
        for (RexNode e : p.right) {
          replaceLast(terms, e, trueLiteral);
        }
        break;
      case LESS_THAN: {
        int comparisonResult = 0;
        if (r.hasUpperBound()) {
          comparisonResult = v0.compareTo(r.upperEndpoint());
        }
        if (comparisonResult <= 0) {
          // 1) No upper bound, or
          // 2) We need to open the upper bound, or
          // 3) New upper bound is lower than old upper bound
          if (r.hasLowerBound()) {
            if (v0.compareTo(r.lowerEndpoint()) <= 0) {
              // Range is empty, not satisfiable
              return rexBuilder.makeLiteral(false);
            }
            // a <= x < b OR a < x < b
            r = Range.range(r.lowerEndpoint(), r.lowerBoundType(),
                    v0, BoundType.OPEN);
          } else {
            // x < b
            r = Range.lessThan(v0);
          }

          if (r.isEmpty()) {
            // Range is empty, not satisfiable
            return rexBuilder.makeLiteral(false);
          }

          // remove prev upper bound
          removeUpperBound = true;
        } else {
          // Remove this term as it is contained in current upper bound
          replaceLast(terms, term, trueLiteral);
        }
        break;
      }
      case LESS_THAN_OR_EQUAL: {
        int comparisonResult = -1;
        if (r.hasUpperBound()) {
          comparisonResult = v0.compareTo(r.upperEndpoint());
        }
        if (comparisonResult < 0) {
          // 1) No upper bound, or
          // 2) New upper bound is lower than old upper bound
          if (r.hasLowerBound()) {
            if (v0.compareTo(r.lowerEndpoint()) < 0) {
              // Range is empty, not satisfiable
              return rexBuilder.makeLiteral(false);
            }
            // a <= x <= b OR a < x <= b
            r = Range.range(r.lowerEndpoint(), r.lowerBoundType(),
                    v0, BoundType.CLOSED);
          } else {
            // x <= b
            r = Range.atMost(v0);
          }

          if (r.isEmpty()) {
            // Range is empty, not satisfiable
            return rexBuilder.makeLiteral(false);
          }

          // remove prev upper bound
          removeUpperBound = true;
        } else {
          // Remove this term as it is contained in current upper bound
          replaceLast(terms, term, trueLiteral);
        }
        break;
      }
      case GREATER_THAN: {
        int comparisonResult = 0;
        if (r.hasLowerBound()) {
          comparisonResult = v0.compareTo(r.lowerEndpoint());
        }
        if (comparisonResult >= 0) {
          // 1) No lower bound, or
          // 2) We need to open the lower bound, or
          // 3) New lower bound is greater than old lower bound
          if (r.hasUpperBound()) {
            if (v0.compareTo(r.upperEndpoint()) >= 0) {
              // Range is empty, not satisfiable
              return rexBuilder.makeLiteral(false);
            }
            // a < x <= b OR a < x < b
            r = Range.range(v0, BoundType.OPEN,
                    r.upperEndpoint(), r.upperBoundType());
          } else {
            // x > a
            r = Range.greaterThan(v0);
          }

          if (r.isEmpty()) {
            // Range is empty, not satisfiable
            return rexBuilder.makeLiteral(false);
          }

          // remove prev lower bound
          removeLowerBound = true;
        } else {
          // Remove this term as it is contained in current lower bound
          replaceLast(terms, term, trueLiteral);
        }
        break;
      }
      case GREATER_THAN_OR_EQUAL: {
        int comparisonResult = 1;
        if (r.hasLowerBound()) {
          comparisonResult = v0.compareTo(r.lowerEndpoint());
        }
        if (comparisonResult > 0) {
          // 1) No lower bound, or
          // 2) New lower bound is greater than old lower bound
          if (r.hasUpperBound()) {
            if (v0.compareTo(r.upperEndpoint()) > 0) {
              // Range is empty, not satisfiable
              return rexBuilder.makeLiteral(false);
            }
            // a <= x <= b OR a <= x < b
            r = Range.range(v0, BoundType.CLOSED,
                    r.upperEndpoint(), r.upperBoundType());
          } else {
            // x >= a
            r = Range.atLeast(v0);
          }

          if (r.isEmpty()) {
            // Range is empty, not satisfiable
            return rexBuilder.makeLiteral(false);
          }

          // remove prev lower bound
          removeLowerBound = true;
        } else {
          // Remove this term as it is contained in current lower bound
          replaceLast(terms, term, trueLiteral);
        }
        break;
      }
      default:
        throw new AssertionError();
      }
      if (removeUpperBound) {
        ImmutableList.Builder<RexNode> newBounds = ImmutableList.builder();
        for (RexNode e : p.right) {
          if (isUpperBound(e)) {
            replaceLast(terms, e, trueLiteral);
          } else {
            newBounds.add(e);
          }
        }
        newBounds.add(term);
        rangeTerms.put(comparison.ref, r, (List<RexNode>) newBounds.build());
      } else if (removeLowerBound) {
        ImmutableList.Builder<RexNode> newBounds = ImmutableList.builder();
        for (RexNode e : p.right) {
          if (isLowerBound(e)) {
            replaceLast(terms, e, trueLiteral);
          } else {
            newBounds.add(e);
          }
        }
        newBounds.add(term);
        rangeTerms.put(comparison.ref, r, (List<RexNode>) newBounds.build());
      }
    }
    // Default
    return null;
  }

  private static <C extends Comparable<C>> Range<C> range(SqlKind comparison,
      C c) {
    switch (comparison) {
    case EQUALS:
      return Range.singleton(c);
    case LESS_THAN:
      return Range.lessThan(c);
    case LESS_THAN_OR_EQUAL:
      return Range.atMost(c);
    case GREATER_THAN:
      return Range.greaterThan(c);
    case GREATER_THAN_OR_EQUAL:
      return Range.atLeast(c);
    default:
      throw new AssertionError();
    }
  }

  /** Comparison between a {@link RexInputRef} or {@link RexFieldAccess} and a
   * literal. Literal may be on left or right side, and may be null. */
  public static class Comparison {
    final RexNode ref;
    final SqlKind kind;
    final RexLiteral literal;

    private Comparison(RexNode ref, SqlKind kind, RexLiteral literal) {
      this.ref = Objects.requireNonNull(ref);
      this.kind = Objects.requireNonNull(kind);
      this.literal = Objects.requireNonNull(literal);
    }

    /** Creates a comparison, or returns null. */
    public static Comparison of(RexNode e) {
      switch (e.getKind()) {
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        final RexCall call = (RexCall) e;
        final List<RexNode> operands = call.getOperands();
        final RexNode left = operands.get(0);
        final RexNode right = operands.get(1);

        switch (right.getKind()) {
        case LITERAL:
          if (nodePredicate.test(left)) {
            return new Comparison(left, e.getKind(), (RexLiteral) right);
          }
        }
        switch (left.getKind()) {
        case LITERAL:
          if (nodePredicate.test(right)) {
            return new Comparison(right, e.getKind().reverse(), (RexLiteral) left);
          }
        }
      }
      return null;
    }

    public RexLiteral getConsistentTypeLiteral(RexSimplify simplify) {
      if (literal != null) {
        final RexBuilder rexBuilder = simplify.rexBuilder;
        List<RelDataType> types = RexUtil.types(Arrays.asList(ref, literal));
        RelDataType type = SqlTypeUtil.consistentType(rexBuilder.getTypeFactory(),
            SqlOperandTypeChecker.Consistency.COMPARE, types);
        if ((type != null) && !(types.stream().allMatch(type::equals))) {
          return (RexLiteral) simplify.simplify(
              rexBuilder.ensureType(type, (RexLiteral) literal, true));
        }
      }
      return literal;
    }
  }

  /** Holds a map between an input and a RangeTerm.
   * @param <C> */
  private static class RangeTerms<C extends Comparable<C>> {
    Map<RexNode, RangeTerm> rangeTerms;

    /** Gives a range that ensures consistent type,
     * which is done in a lazy manner.
     * @param <C> */
    private static class RangeTerm<C extends Comparable<C>> {
      List<RexNode> rexNodes;
      Range<C> range;
      SqlKind sqlKind;
      Comparison comparison;
      RexSimplify simplify;
      Class<C> clazz;

      private RangeTerm(List<RexNode> rexNodes, SqlKind sqlKind,
          Comparison comparison, RexSimplify simplify, Class<C> clazz) {
        this.rexNodes = rexNodes;
        this.comparison = comparison;
        this.simplify = simplify;
        this.clazz = clazz;
        this.sqlKind = sqlKind;
        this.range = null;
      }

      private RangeTerm(Range<C> range, List<RexNode> rexNodes) {
        this.range = range;
        this.rexNodes = rexNodes;
      }

      public Pair<Range<C>, List<RexNode>> getPair() {
        if (range == null) {
          range = range(sqlKind, comparison.getConsistentTypeLiteral(simplify).getValueAs(clazz));
        }
        return Pair.of(range, rexNodes);
      }
    }

    RangeTerms() {
      rangeTerms = new HashMap<>();
    }

    public void put(RexNode key, List<RexNode> rexNodes, SqlKind sqlKind,
        Comparison comparison, RexSimplify simplify, Class<C> clazz) {
      rangeTerms.put(key, new RangeTerm(rexNodes, sqlKind, comparison, simplify, clazz));
    }

    public void put(RexNode key, Range<C> range, List<RexNode> rexNodes) {
      rangeTerms.put(key, new RangeTerm(range, rexNodes));
    }
    public Pair<Range<C>, List<RexNode>> get(RexNode key) {
      RangeTerm term = rangeTerms.get(key);
      return term == null ? null : rangeTerms.get(key).getPair();
    }
  }

  private static boolean isUpperBound(final RexNode e) {
    final List<RexNode> operands;
    switch (e.getKind()) {
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      operands = ((RexCall) e).getOperands();
      return RexUtil.isReferenceOrAccess(operands.get(0), true)
          && operands.get(1).isA(SqlKind.LITERAL);
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      operands = ((RexCall) e).getOperands();
      return RexUtil.isReferenceOrAccess(operands.get(1), true)
          && operands.get(0).isA(SqlKind.LITERAL);
    default:
      return false;
    }
  }

  private static boolean isLowerBound(final RexNode e) {
    final List<RexNode> operands;
    switch (e.getKind()) {
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      operands = ((RexCall) e).getOperands();
      return RexUtil.isReferenceOrAccess(operands.get(1), true)
          && operands.get(0).isA(SqlKind.LITERAL);
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      operands = ((RexCall) e).getOperands();
      return RexUtil.isReferenceOrAccess(operands.get(0), true)
          && operands.get(1).isA(SqlKind.LITERAL);
    default:
      return false;
    }
  }
<<<<<<< HEAD
=======

  /**
   * Replaces the last occurrence of one specified value in a list with
   * another.
   *
   * <p>Does not change the size of the list.
   *
   * <p>Returns whether the value was found.
   */
  private static <E> boolean replaceLast(List<E> list, E oldVal, E newVal) {
    final int index = list.lastIndexOf(oldVal);
    if (index < 0) {
      return false;
    }
    list.set(index, newVal);
    return true;
  }

  /**
   * Combines predicates AND, optimizes, and returns null if the result is
   * always false.
   *
   * <p>The expression is simplified on the assumption that an UNKNOWN value
   * is always treated as FALSE. Therefore the simplified expression may
   * sometimes evaluate to FALSE where the original expression would evaluate to
   * UNKNOWN.
   *
   * @param predicates Filter condition predicates
   * @return simplified conjunction of predicates for the filter, null if always false
   */
  public RexNode simplifyFilterPredicates(Iterable<? extends RexNode> predicates) {
    final RexNode simplifiedAnds = simplifyAnds(predicates, FALSE);
    if (simplifiedAnds.isAlwaysFalse()) {
      return null;
    }

    // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
    // nullable and not-nullable conditions, but a CAST might get in the way of
    // other rewrites.
    return removeNullabilityCast(simplifiedAnds);
  }

}

  /**
   * Replaces the last occurrence of one specified value in a list with
   * another.
   *
   * <p>Does not change the size of the list.
   *
   * <p>Returns whether the value was found.
   */
  private static <E> boolean replaceLast(List<E> list, E oldVal, E newVal) {
    final int index = list.lastIndexOf(oldVal);
    if (index < 0) {
      return false;
    }
    list.set(index, newVal);
    return true;
  }

  /**
   * Combines predicates AND, optimizes, and returns null if the result is
   * always false.
   *
   * <p>The expression is simplified on the assumption that an UNKNOWN value
   * is always treated as FALSE. Therefore the simplified expression may
   * sometimes evaluate to FALSE where the original expression would evaluate to
   * UNKNOWN.
   *
   * @param predicates Filter condition predicates
   * @return simplified conjunction of predicates for the filter, null if always false
   */
  public RexNode simplifyFilterPredicates(Iterable<? extends RexNode> predicates) {
    final RexNode simplifiedAnds =
        withPredicateElimination(Bug.CALCITE_2401_FIXED)
            .simplifyUnknownAsFalse(
                RexUtil.composeConjunction(rexBuilder, predicates));
    if (simplifiedAnds.isAlwaysFalse()) {
      return null;
    }

    // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
    // nullable and not-nullable conditions, but a CAST might get in the way of
    // other rewrites.
    return removeNullabilityCast(simplifiedAnds);
  }

}
