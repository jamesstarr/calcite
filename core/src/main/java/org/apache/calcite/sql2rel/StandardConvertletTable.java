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
package org.apache.calcite.sql2rel;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.fun.SqlJsonValueFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlLiteralChainOperator;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlMultisetQueryConstructor;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.fun.SqlOverlapsOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlSequenceValueOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Standard implementation of {@link SqlRexConvertletTable}.
 */
public class StandardConvertletTable extends ReflectiveConvertletTable {

  /** Singleton instance. */
  public static final StandardConvertletTable INSTANCE =
      new StandardConvertletTable();

  //~ Constructors -----------------------------------------------------------

  private StandardConvertletTable() {
    super();

    // Register aliases (operators which have a different name but
    // identical behavior to other operators).
    addAlias(SqlStdOperatorTable.CHARACTER_LENGTH,
        SqlStdOperatorTable.CHAR_LENGTH);
    addAlias(SqlStdOperatorTable.IS_UNKNOWN,
        SqlStdOperatorTable.IS_NULL);
    addAlias(SqlStdOperatorTable.IS_NOT_UNKNOWN,
        SqlStdOperatorTable.IS_NOT_NULL);
    addAlias(SqlStdOperatorTable.PERCENT_REMAINDER, SqlStdOperatorTable.MOD);

    // Register convertlets for specific objects.
    registerOp(SqlStdOperatorTable.CAST, this::convertCast);
    registerOp(SqlLibraryOperators.INFIX_CAST, this::convertCast);
    registerOp(SqlStdOperatorTable.IS_DISTINCT_FROM,
        (cx, call) -> convertIsDistinctFrom(cx, call, false));
    registerOp(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        (cx, call) -> convertIsDistinctFrom(cx, call, true));

    registerOp(SqlStdOperatorTable.PLUS, this::convertPlus);
    registerOp(
        SqlStdOperatorTable.EQUALS,

    final RexCall convertedCall = (RexCall) convertCall(cx, call);

    final RexNode op0 = convertedCall.getOperands().get(0);
    final RexNode op1 = convertedCall.getOperands().get(1);
    final RexNode booleanOp;
    final RexNode integerOp;

    if (op0.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
        && SqlTypeName.INT_TYPES.contains(op1.getType().getSqlTypeName())) {
      booleanOp = op0;
      integerOp = op1;
    } else if (SqlTypeName.INT_TYPES.contains(op0.getType().getSqlTypeName())
        && op1.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      booleanOp = op1;
      integerOp = op0;
    } else {
      return convertedCall;
    }

    final RexBuilder rexBuilder = cx.getRexBuilder();
    return rexBuilder.makeCall(call.getOperator(),
        booleanOp,
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                integerOp,
                rexBuilder.makeZeroLiteral(integerOp.getType())),
            rexBuilder.makeLiteral(false),
            rexBuilder.makeLiteral(true)));
  }

  private RexNode convertPlus(SqlRexContext cx, SqlCall call) {
    final RexNode rex = convertCall(cx, call);
    switch (rex.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIMESTAMP:
      // Use special "+" operator for datetime + interval.
      // Re-order operands, if necessary, so that interval is second.
      final RexBuilder rexBuilder = cx.getRexBuilder();
      List<RexNode> operands = ((RexCall) rex).getOperands();
      if (operands.size() == 2) {
        final SqlTypeName sqlTypeName = operands.get(0).getType().getSqlTypeName();
        switch (sqlTypeName) {
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
          operands = ImmutableList.of(operands.get(1), operands.get(0));
        }
      }
      return rexBuilder.makeCall(rex.getType(),
          SqlStdOperatorTable.DATETIME_PLUS, operands);
    default:
      return rex;
    }
  }

  private RexNode convertIsDistinctFrom(
      SqlRexContext cx,
      SqlCall call,
      boolean neg) {
    RexNode op0 = cx.convertExpression(call.operand(0));
    RexNode op1 = cx.convertExpression(call.operand(1));
    return RelOptUtil.isDistinctFrom(
        cx.getRexBuilder(), op0, op1, neg);
  }

  /**
   * Converts a BETWEEN expression.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertBetween(
      SqlRexContext cx,
      SqlBetweenOperator op,
      SqlCall call) {
    final List<RexNode> list =
        convertExpressionList(cx, call.getOperandList(),
            op.getOperandTypeChecker().getConsistency());
    final RexNode x = list.get(SqlBetweenOperator.VALUE_OPERAND);
    final RexNode y = list.get(SqlBetweenOperator.LOWER_OPERAND);
    final RexNode z = list.get(SqlBetweenOperator.UPPER_OPERAND);

    final RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode ge1 = ge(rexBuilder, x, y);
    RexNode le1 = le(rexBuilder, x, z);
    RexNode and1 = and(rexBuilder, ge1, le1);

    RexNode res;
    final SqlBetweenOperator.Flag symmetric = op.flag;
    switch (symmetric) {
    case ASYMMETRIC:
      res = and1;
      break;
    case SYMMETRIC:
      RexNode ge2 = ge(rexBuilder, x, z);
      RexNode le2 = le(rexBuilder, x, y);
      RexNode and2 = and(rexBuilder, ge2, le2);
      res = or(rexBuilder, and1, and2);
      break;
    default:
      throw Util.unexpected(symmetric);
    }
    final SqlBetweenOperator betweenOp =
        (SqlBetweenOperator) call.getOperator();
    if (betweenOp.isNegated()) {
      res = rexBuilder.makeCall(SqlStdOperatorTable.NOT, res);
    }
    return res;
  }

  /**
   * Converts a LiteralChain expression: that is, concatenates the operands
   * immediately, to produce a single literal string.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertLiteralChain(
      SqlRexContext cx,
      SqlLiteralChainOperator op,
      SqlCall call) {
    Util.discard(cx);

    SqlLiteral sum = SqlLiteralChainOperator.concatenateOperands(call);
    return cx.convertLiteral(sum);
  }

  /**
   * Converts a ROW.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertRow(
      SqlRexContext cx,
      SqlRowOperator op,
      SqlCall call) {
    if (cx.getValidator().getValidatedNodeType(call).getSqlTypeName()
        != SqlTypeName.COLUMN_LIST) {
      return convertCall(cx, call);
    }
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<RexNode> columns = new ArrayList<>();
    for (SqlNode operand : call.getOperandList()) {
      columns.add(
          rexBuilder.makeLiteral(
              ((SqlIdentifier) operand).getSimple()));
    }
    final RelDataType type =
        rexBuilder.deriveReturnType(SqlStdOperatorTable.COLUMN_LIST, columns);
    return rexBuilder.makeCall(type, SqlStdOperatorTable.COLUMN_LIST, columns);
  }

  /**
   * Converts a call to OVERLAPS.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertOverlaps(
      SqlRexContext cx,
      SqlOverlapsOperator op,
      SqlCall call) {
    // for intervals [t0, t1] overlaps [t2, t3], we can find if the
    // intervals overlaps by: ~(t1 < t2 or t3 < t0)
    assert call.getOperandList().size() == 2;

    final Pair<RexNode, RexNode> left =
        convertOverlapsOperand(cx, call.getParserPosition(), call.operand(0));
    final RexNode r0 = left.left;
    final RexNode r1 = left.right;
    final Pair<RexNode, RexNode> right =
        convertOverlapsOperand(cx, call.getParserPosition(), call.operand(1));
    final RexNode r2 = right.left;
    final RexNode r3 = right.right;

    // Sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
    final RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode leftSwap = le(rexBuilder, r0, r1);
    final RexNode s0 = case_(rexBuilder, leftSwap, r0, r1);
    final RexNode e0 = case_(rexBuilder, leftSwap, r1, r0);
    RexNode rightSwap = le(rexBuilder, r2, r3);
    final RexNode s1 = case_(rexBuilder, rightSwap, r2, r3);
    final RexNode e1 = case_(rexBuilder, rightSwap, r3, r2);
    // (e0 >= s1) AND (e1 >= s0)
    switch (op.kind) {
    case OVERLAPS:
      return and(rexBuilder,
          ge(rexBuilder, e0, s1),
          ge(rexBuilder, e1, s0));
    case CONTAINS:
      return and(rexBuilder,
          le(rexBuilder, s0, s1),
          ge(rexBuilder, e0, e1));
    case PERIOD_EQUALS:
      return and(rexBuilder,
          eq(rexBuilder, s0, s1),
          eq(rexBuilder, e0, e1));
    case PRECEDES:
      return le(rexBuilder, e0, s1);
    case IMMEDIATELY_PRECEDES:
      return eq(rexBuilder, e0, s1);
    case SUCCEEDS:
      return ge(rexBuilder, s0, e1);
    case IMMEDIATELY_SUCCEEDS:
      return eq(rexBuilder, s0, e1);
    default:
      throw new AssertionError(op);
    }
  }

  private Pair<RexNode, RexNode> convertOverlapsOperand(SqlRexContext cx,
      SqlParserPos pos, SqlNode operand) {
    final SqlNode a0;
    final SqlNode a1;
    switch (operand.getKind()) {
    case ROW:
      a0 = ((SqlCall) operand).operand(0);
      final SqlNode a10 = ((SqlCall) operand).operand(1);
      final RelDataType t1 = cx.getValidator().getValidatedNodeType(a10);
      if (SqlTypeUtil.isInterval(t1)) {
        // make t1 = t0 + t1 when t1 is an interval.
        a1 = plus(pos, a0, a10);
      } else {
        a1 = a10;
      }
      break;
    default:
      a0 = operand;
      a1 = operand;
    }

    final RexNode r0 = cx.convertExpression(a0);
    final RexNode r1 = cx.convertExpression(a1);
    return Pair.of(r0, r1);
  }

  /**
   * Casts a RexNode value to the validated type of a SqlCall. If the value
   * was already of the validated type, then the value is returned without an
   * additional cast.
   */
  public RexNode castToValidatedType(
      SqlRexContext cx,
      SqlCall call,
      RexNode value) {
    return castToValidatedType(call, value, cx.getValidator(),
        cx.getRexBuilder());
  }

  /**
   * Casts a RexNode value to the validated type of a SqlCall. If the value
   * was already of the validated type, then the value is returned without an
   * additional cast.
   */
  public static RexNode castToValidatedType(SqlNode node, RexNode e,
      SqlValidator validator, RexBuilder rexBuilder) {
    final RelDataType type = validator.getValidatedNodeType(node);
    if (e.getType() == type) {
      return e;
    }
    return rexBuilder.makeCast(type, e);
  }

  /** Convertlet that handles {@code AVG} and {@code VARIANCE}
   * windowed aggregate functions. */
  private static class AvgVarianceConvertlet implements SqlRexConvertlet {
    private final SqlKind kind;

    AvgVarianceConvertlet(SqlKind kind) {
      this.kind = kind;
    }

    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      assert call.operandCount() == 1;
      final SqlNode arg = call.operand(0);
      final SqlNode expr;
      final RelDataType type =
          cx.getValidator().getValidatedNodeType(call);
      switch (kind) {
      case AVG:
        expr = expandAvg(arg, type, cx);
        break;
      case STDDEV_POP:
        expr = expandVariance(arg, type, cx, true, true);
        break;
      case STDDEV_SAMP:
        expr = expandVariance(arg, type, cx, false, true);
        break;
      case VAR_POP:
        expr = expandVariance(arg, type, cx, true, false);
        break;
      case VAR_SAMP:
        expr = expandVariance(arg, type, cx, false, false);
        break;
      default:
        throw Util.unexpected(kind);
      }
      RexNode rex = cx.convertExpression(expr);
      return cx.getRexBuilder().ensureType(type, rex, true);
    }

    private SqlNode expandAvg(
        final SqlNode arg, final RelDataType avgType, final SqlRexContext cx) {
      final SqlParserPos pos = SqlParserPos.ZERO;
      final SqlNode sum =
          SqlStdOperatorTable.SUM.createCall(pos, arg);
      final RexNode sumRex = cx.convertExpression(sum);
      final SqlNode sumCast;
      if (!sumRex.getType().equals(avgType)) {
        sumCast = SqlStdOperatorTable.CAST.createCall(pos,
            sum,
            SqlTypeUtil.convertTypeToSpec(avgType));
      } else {
        sumCast = sum;
      }
      final SqlNode count =
          SqlStdOperatorTable.COUNT.createCall(pos, arg);
      return SqlStdOperatorTable.DIVIDE.createCall(
          pos, sumCast, count);
    }

    private SqlNode expandVariance(
        final SqlNode argInput,
        final RelDataType varType,
        final SqlRexContext cx,
        boolean biased,
        boolean sqrt) {
      // stddev_pop(x) ==>
      //   power(
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / count(x),
      //     .5)
      //
      // stddev_samp(x) ==>
      //   power(
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / (count(x) - 1),
      //     .5)
      //
      // var_pop(x) ==>
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / count(x)
      //
      // var_samp(x) ==>
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / (count(x) - 1)
      final SqlParserPos pos = SqlParserPos.ZERO;

      final RexNode argRex = cx.convertExpression(argInput);
      final SqlNode arg;
      if (!argRex.getType().equals(varType)) {
        arg = SqlStdOperatorTable.CAST.createCall(pos,
            argInput,
            SqlTypeUtil.convertTypeToSpec(varType));
      } else {
        arg = argInput;
      }

      final SqlNode argSquared =
          SqlStdOperatorTable.MULTIPLY.createCall(pos, arg, arg);
      final SqlNode sumArgSquared =
          SqlStdOperatorTable.SUM.createCall(pos, argSquared);
      final SqlNode sum =
          SqlStdOperatorTable.SUM.createCall(pos, arg);
      final SqlNode sumSquared =
          SqlStdOperatorTable.MULTIPLY.createCall(pos, sum, sum);
      final SqlNode count =
          SqlStdOperatorTable.COUNT.createCall(pos, arg);
      final SqlNode avgSumSquared =
          SqlStdOperatorTable.DIVIDE.createCall(
              pos, sumSquared, count);
      final SqlNode diff =
          SqlStdOperatorTable.MINUS.createCall(
              pos, sumArgSquared, avgSumSquared);
      final SqlNode denominator;
      if (biased) {
        denominator = count;
      } else {
        final SqlNumericLiteral one =
            SqlLiteral.createExactNumeric("1", pos);
        denominator =
            SqlStdOperatorTable.MINUS.createCall(
                pos, count, one);
      }
      final SqlNode div =
          SqlStdOperatorTable.DIVIDE.createCall(
              pos, diff, denominator);
      SqlNode result = div;
      if (sqrt) {
        final SqlNumericLiteral half =
            SqlLiteral.createExactNumeric("0.5", pos);
        result =
            SqlStdOperatorTable.POWER.createCall(pos, div, half);
      }
      return result;
    }
  }

  /** Convertlet that converts {@code LTRIM} and {@code RTRIM} to
   * {@code TRIM}. */
  private static class TrimConvertlet implements SqlRexConvertlet {
    private final SqlTrimFunction.Flag flag;

    TrimConvertlet(SqlTrimFunction.Flag flag) {
      this.flag = flag;
    }

    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final RexNode operand =
          cx.convertExpression(call.getOperandList().get(0));
      return rexBuilder.makeCall(SqlStdOperatorTable.TRIM,
          rexBuilder.makeFlag(flag), rexBuilder.makeLiteral(" "), operand);
    }
  }

  /** Convertlet that converts {@code GREATEST} and {@code LEAST}. */
  private static class GreatestConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      // Translate
      //   GREATEST(a, b, c, d)
      // to
      //   CASE
      //   WHEN a IS NULL OR b IS NULL OR c IS NULL OR d IS NULL
      //   THEN NULL
      //   WHEN a > b AND a > c AND a > d
      //   THEN a
      //   WHEN b > c AND b > d
      //   THEN b
      //   WHEN c > d
      //   THEN c
      //   ELSE d
      //   END
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final RelDataType type =
          cx.getValidator().getValidatedNodeType(call);
      final SqlBinaryOperator op;
      switch (call.getKind()) {
      case GREATEST:
        op = SqlStdOperatorTable.GREATER_THAN;
        break;
      case LEAST:
        op = SqlStdOperatorTable.LESS_THAN;
        break;
      default:
        throw new AssertionError();
      }
      final List<RexNode> exprs = convertExpressionList(cx,
          call.getOperandList(), SqlOperandTypeChecker.Consistency.NONE);
      final List<RexNode> list = new ArrayList<>();
      final List<RexNode> orList = new ArrayList<>();
      for (RexNode expr : exprs) {
        orList.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, expr));
      }
      list.add(RexUtil.composeDisjunction(rexBuilder, orList));
      list.add(rexBuilder.makeNullLiteral(type));
      for (int i = 0; i < exprs.size() - 1; i++) {
        RexNode expr = exprs.get(i);
        final List<RexNode> andList = new ArrayList<>();
        for (int j = i + 1; j < exprs.size(); j++) {
          final RexNode expr2 = exprs.get(j);
          andList.add(rexBuilder.makeCall(op, expr, expr2));
        }
        list.add(RexUtil.composeConjunction(rexBuilder, andList));
        list.add(expr);
      }
      list.add(exprs.get(exprs.size() - 1));
      return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, list);
    }
  }

  /** Convertlet that handles {@code FLOOR} and {@code CEIL} functions. */
  private class FloorCeilConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      return convertFloorCeil(cx, call);
    }
  }

  /** Convertlet that handles the {@code TIMESTAMPADD} function. */
  private static class TimestampAddConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      // TIMESTAMPADD(unit, count, timestamp)
      //  => timestamp + count * INTERVAL '1' UNIT
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final SqlLiteral unitLiteral = call.operand(0);
      final TimeUnit unit = unitLiteral.symbolValue(TimeUnit.class);
      RexNode interval2Add;
      SqlIntervalQualifier qualifier =
          new SqlIntervalQualifier(unit, null, unitLiteral.getParserPosition());
      RexNode op1 = cx.convertExpression(call.operand(1));
      switch (unit) {
      case MICROSECOND:
      case NANOSECOND:
        interval2Add =
            divide(rexBuilder,
                multiply(rexBuilder,
                    rexBuilder.makeIntervalLiteral(BigDecimal.ONE, qualifier), op1),
                BigDecimal.ONE.divide(unit.multiplier,
                    RoundingMode.UNNECESSARY));
        break;
      default:
        interval2Add = multiply(rexBuilder,
            rexBuilder.makeIntervalLiteral(unit.multiplier, qualifier), op1);
      }

      return rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS,
          cx.convertExpression(call.operand(2)), interval2Add);
    }
  }

  /** Convertlet that handles the {@code TIMESTAMPDIFF} function. */
  private static class TimestampDiffConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      // TIMESTAMPDIFF(unit, t1, t2)
      //    => (t2 - t1) UNIT
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final SqlLiteral unitLiteral = call.operand(0);
      TimeUnit unit = unitLiteral.symbolValue(TimeUnit.class);
      BigDecimal multiplier = BigDecimal.ONE;
      BigDecimal divider = BigDecimal.ONE;
      SqlTypeName sqlTypeName = unit == TimeUnit.NANOSECOND
          ? SqlTypeName.BIGINT
          : SqlTypeName.INTEGER;
      switch (unit) {
      case MICROSECOND:
      case MILLISECOND:
      case NANOSECOND:
      case WEEK:
        multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
        divider = unit.multiplier;
        unit = TimeUnit.SECOND;
        break;
      case QUARTER:
        divider = unit.multiplier;
        unit = TimeUnit.MONTH;
        break;
      }
      final SqlIntervalQualifier qualifier =
          new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
      final RexNode op2 = cx.convertExpression(call.operand(2));
      final RexNode op1 = cx.convertExpression(call.operand(1));
      final RelDataType intervalType =
          cx.getTypeFactory().createTypeWithNullability(
              cx.getTypeFactory().createSqlIntervalType(qualifier),
              op1.getType().isNullable() || op2.getType().isNullable());
      final RexCall rexCall = (RexCall) rexBuilder.makeCall(
          intervalType, SqlStdOperatorTable.MINUS_DATE,
          ImmutableList.of(op2, op1));
      final RelDataType intType =
          cx.getTypeFactory().createTypeWithNullability(
              cx.getTypeFactory().createSqlType(sqlTypeName),
              SqlTypeUtil.containsNullable(rexCall.getType()));
      RexNode e = rexBuilder.makeCast(intType, rexCall);
      return rexBuilder.multiplyDivide(e, multiplier, divider);
    }
  }
}
