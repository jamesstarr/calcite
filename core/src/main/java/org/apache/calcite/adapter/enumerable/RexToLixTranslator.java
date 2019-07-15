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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
    }
    default:
      if (expr instanceof RexCall) {
        return translateCall((RexCall) expr, nullAs);
      }
      throw new RuntimeException(
          "cannot translate expression " + expr);
    }
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = program.getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Translates a call to an operator or function. */
  private Expression translateCall(RexCall call, RexImpTable.NullAs nullAs) {
    final SqlOperator operator = call.getOperator();
    CallImplementor implementor =
        RexImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    return implementor.implement(this, call, nullAs);
  }

  /** Translates a parameter. */
  private Expression translateParameter(RexDynamicParam expr,
      RexImpTable.NullAs nullAs, Type storageType) {
    if (storageType == null) {
      storageType = typeFactory.getJavaClass(expr.getType());
    }
    return nullAs.handle(
        EnumUtils.convert(
            Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant("?" + expr.getIndex())),
            storageType));
  }

  /** Translates a literal.
   *
   * @throws AlwaysNull if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
      case TRUE:
      case IS_NULL:
        return RexImpTable.TRUE_EXPR;
      case FALSE:
      case IS_NOT_NULL:
        return RexImpTable.FALSE_EXPR;
      case NOT_POSSIBLE:
        throw AlwaysNull.INSTANCE;
      case NULL:
      default:
        return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
      case IS_NOT_NULL:
        return RexImpTable.TRUE_EXPR;
      case IS_NULL:
        return RexImpTable.FALSE_EXPR;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
    final Object value2;
    switch (literal.getType().getSqlTypeName()) {
    case DECIMAL:
      final BigDecimal bd = literal.getValueAs(BigDecimal.class);
      if (javaClass == float.class) {
        return Expressions.constant(bd, javaClass);
      } else if (javaClass == double.class) {
        return Expressions.constant(bd, javaClass);
      }
      assert javaClass == BigDecimal.class;
      return Expressions.new_(BigDecimal.class,
          Expressions.constant(bd.toString()));
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      value2 = literal.getValueAs(Integer.class);
      javaClass = int.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
      value2 = literal.getValueAs(Long.class);
      javaClass = long.class;
      break;
    case CHAR:
    case UUID:
    case VARCHAR:
      value2 = literal.getValueAs(String.class);
      break;
    case BINARY:
    case VARBINARY:
      return Expressions.new_(
          ByteString.class,
          Expressions.constant(
              literal.getValueAs(byte[].class),
              byte[].class));
    case SYMBOL:
      value2 = literal.getValueAs(Enum.class);
      javaClass = value2.getClass();
      break;
    default:
      final Primitive primitive = Primitive.ofBoxOr(javaClass);
      final Comparable value = literal.getValueAs(Comparable.class);
      if (primitive != null && value instanceof Number) {
        value2 = primitive.number((Number) value);
      } else {
        value2 = value;
      }
    }
    return Expressions.constant(value2, javaClass);
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs) {
    return translateList(operandList, nullAs,
        EnumUtils.internalTypes(operandList));
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>();
    for (Pair<RexNode, ? extends Type> e : Pair.zip(operandList, storageTypes)) {
      list.add(translate(e.left, nullAs, e.right));
    }
    return list;
  }

  /**
   * Translates the list of {@code RexNode}, using the default output types.
   * This might be suboptimal in terms of additional box-unbox when you use
   * the translation later.
   * If you know the java class that will be used to store the results, use
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator#translateList(java.util.List, java.util.List)}
   * version.
   *
   * @param operandList list of RexNodes to translate
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList) {
    return translateList(operandList, EnumUtils.internalTypes(operandList));
  }

  /**
   * Translates the list of {@code RexNode}, while optimizing for output
   * storage.
   * For instance, if the result of translation is going to be stored in
   * {@code Object[]}, and the input is {@code Object[]} as well,
   * then translator will avoid casting, boxing, etc.
   *
   * @param operandList list of RexNodes to translate
   * @param storageTypes hints of the java classes that will be used
   *                     to store translation results. Use null to use
   *                     default storage type
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());

    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;
  }

  public static Expression translateCondition(RexProgram program,
      JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter,
      Function1<String, InputGetter> correlates, SqlConformance conformance) {
    if (program.getCondition() == null) {
      return RexImpTable.TRUE_EXPR;
    }
    final ParameterExpression root = DataContext.ROOT;
    RexToLixTranslator translator =
        new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
            null, null);
    translator = translator.setCorrelates(correlates);
    return translator.translate(
        program.getCondition(),
        RexImpTable.NullAs.FALSE);
  }
<<<<<<< HEAD
        block, ImmutableMap.of(), builder, this, correlates);
=======

  public static Expression convert(Expression operand, Type toType) {
    final Type fromType = operand.getType();
    return convert(operand, fromType, toType);
  }

  public static Expression convert(Expression operand, Type fromType,
      Type toType) {
    if (fromType.equals(toType)) {
      return operand;
    }
    // E.g. from "Short" to "int".
    // Generate "x.intValue()".
    final Primitive toPrimitive = Primitive.of(toType);
    final Primitive toBox = Primitive.ofBox(toType);
    final Primitive fromBox = Primitive.ofBox(fromType);
    final Primitive fromPrimitive = Primitive.of(fromType);
    final boolean fromNumber = fromType instanceof Class
        && Number.class.isAssignableFrom((Class) fromType);
    if (fromType == String.class) {
      if (toPrimitive != null) {
        switch (toPrimitive) {
        case CHAR:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          // Generate "SqlFunctions.toShort(x)".
          return Expressions.call(
              SqlFunctions.class,
              "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
              operand);
        default:
          // Generate "Short.parseShort(x)".
          return Expressions.call(
              toPrimitive.boxClass,
              "parse" + SqlFunctions.initcap(toPrimitive.primitiveName),
              operand);
        }
      }
      if (toBox != null) {
        switch (toBox) {
        case CHAR:
          // Generate "SqlFunctions.toCharBoxed(x)".
          return Expressions.call(
              SqlFunctions.class,
              "to" + SqlFunctions.initcap(toBox.primitiveName) + "Boxed",
              operand);
        default:
          // Generate "Short.valueOf(x)".
          return Expressions.call(
              toBox.boxClass,
              "valueOf",
              operand);
        }
      }
    }
    if (toPrimitive != null) {
      if (fromPrimitive != null) {
        // E.g. from "float" to "double"
        return Expressions.convert_(
            operand, toPrimitive.primitiveClass);
      }
      if (fromNumber || fromBox == Primitive.CHAR) {
        // Generate "x.shortValue()".
        return Expressions.unbox(operand, toPrimitive);
      } else {
        // E.g. from "Object" to "short".
        // Generate "SqlFunctions.toShort(x)"
        return Expressions.call(
            SqlFunctions.class,
            "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
            operand);
      }
    } else if (fromNumber && toBox != null) {
      // E.g. from "Short" to "Integer"
      // Generate "x == null ? null : Integer.valueOf(x.intValue())"
      return Expressions.condition(
          Expressions.equal(operand, RexImpTable.NULL_EXPR),
          RexImpTable.NULL_EXPR,
          Expressions.box(
              Expressions.unbox(operand, toBox),
              toBox));
    } else if (fromPrimitive != null && toBox != null) {
      // E.g. from "int" to "Long".
      // Generate Long.valueOf(x)
      // Eliminate primitive casts like Long.valueOf((long) x)
      if (operand instanceof UnaryExpression) {
        UnaryExpression una = (UnaryExpression) operand;
        if (una.nodeType == ExpressionType.Convert
            || Primitive.of(una.getType()) == toBox) {
          return Expressions.box(una.expression, toBox);
        }
      }
      return Expressions.box(operand, toBox);
    } else if (fromType == java.sql.Date.class) {
      if (toBox == Primitive.INT) {
        return Expressions.call(BuiltInMethod.DATE_TO_INT.method, operand);
      } else {
        return Expressions.convert_(operand, toType);
      }
    } else if (toType == java.sql.Date.class) {
      // E.g. from "int" or "Integer" to "java.sql.Date",
      // generate "SqlFunctions.internalToDate".
      if (isA(fromType, Primitive.INT)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, operand);
      } else {
        return Expressions.convert_(operand, java.sql.Date.class);
      }
    } else if (toType == java.sql.Time.class) {
      // E.g. from "int" or "Integer" to "java.sql.Time",
      // generate "SqlFunctions.internalToTime".
      if (isA(fromType, Primitive.INT)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, operand);
      } else {
        return Expressions.convert_(operand, java.sql.Time.class);
      }
    } else if (toType == java.sql.Timestamp.class) {
      // E.g. from "long" or "Long" to "java.sql.Timestamp",
      // generate "SqlFunctions.internalToTimestamp".
      if (isA(fromType, Primitive.LONG)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method,
            operand);
      } else {
        return Expressions.convert_(operand, java.sql.Timestamp.class);
      }
    } else if (toType == BigDecimal.class) {
      if (fromBox != null) {
        // E.g. from "Integer" to "BigDecimal".
        // Generate "x == null ? null : new BigDecimal(x.intValue())"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.new_(
                BigDecimal.class,
                Expressions.unbox(operand, fromBox)));
      }
      if (fromPrimitive != null) {
        // E.g. from "int" to "BigDecimal".
        // Generate "new BigDecimal(x)"
        // Fix CALCITE-2325, we should decide null here for int type.
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.new_(
                BigDecimal.class,
                operand));
      }
      // E.g. from "Object" to "BigDecimal".
      // Generate "x == null ? null : SqlFunctions.toBigDecimal(x)"
      return Expressions.condition(
          Expressions.equal(operand, RexImpTable.NULL_EXPR),
          RexImpTable.NULL_EXPR,
          Expressions.call(
              SqlFunctions.class,
              "toBigDecimal",
              operand));
    } else if (toType == String.class) {
      if (fromPrimitive != null) {
        switch (fromPrimitive) {
        case DOUBLE:
        case FLOAT:
          // E.g. from "double" to "String"
          // Generate "SqlFunctions.toString(x)"
          return Expressions.call(
              SqlFunctions.class,
              "toString",
              operand);
        default:
          // E.g. from "int" to "String"
          // Generate "Integer.toString(x)"
          return Expressions.call(
              fromPrimitive.boxClass,
              "toString",
              operand);
        }
      } else if (fromType == BigDecimal.class) {
        // E.g. from "BigDecimal" to "String"
        // Generate "x.toString()"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.call(
                SqlFunctions.class,
                "toString",
                operand));
      } else {
        // E.g. from "BigDecimal" to "String"
        // Generate "x == null ? null : x.toString()"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.call(
                operand,
                "toString"));
      }
    }
    return Expressions.convert_(operand, toType);
  }

  static boolean isA(Type fromType, Primitive primitive) {
    return Primitive.of(fromType) == primitive
        || Primitive.ofBox(fromType) == primitive;
  }

  public Expression translateConstructor(
      List<RexNode> operandList, SqlKind kind) {
    switch (kind) {
    case MAP_VALUE_CONSTRUCTOR:
      Expression map =
          list.append(
              "map",
              Expressions.new_(LinkedHashMap.class),
              false);
      for (int i = 0; i < operandList.size(); i++) {
        RexNode key = operandList.get(i++);
        RexNode value = operandList.get(i);
        list.add(
            Expressions.statement(
                Expressions.call(
                    map,
                    BuiltInMethod.MAP_PUT.method,
                    Expressions.box(translate(key)),
                    Expressions.box(translate(value)))));
      }
      return map;
    case ARRAY_VALUE_CONSTRUCTOR:
      Expression lyst =
          list.append(
              "list",
              Expressions.new_(ArrayList.class),
              false);
      for (RexNode value : operandList) {
        list.add(
            Expressions.statement(
                Expressions.call(
                    lyst,
                    BuiltInMethod.COLLECTION_ADD.method,
                    Expressions.box(translate(value)))));
      }
      return lyst;
    default:
      throw new AssertionError("unexpected: " + kind);
    }
  }

  /** Returns whether an expression is nullable. Even if its type says it is
   * nullable, if we have previously generated a check to make sure that it is
   * not null, we will say so.
   *
   * <p>For example, {@code WHERE a == b} translates to
   * {@code a != null && b != null && a.equals(b)}. When translating the
   * 3rd part of the disjunction, we already know a and b are not null.</p>
   *
   * @param e Expression
   * @return Whether expression is nullable in the current translation context
   */
  public boolean isNullable(RexNode e) {
    if (!e.getType().isNullable()) {
      return false;
    }
    final Boolean b = isKnownNullable(e);
    return b == null || b;
  }

  /**
   * Walks parent translator chain and verifies if the expression is nullable.
   *
   * @param node RexNode to check if it is nullable or not
   * @return null when nullability is not known, true or false otherwise
   */
  protected Boolean isKnownNullable(RexNode node) {
    if (!exprNullableMap.isEmpty()) {
      Boolean nullable = exprNullableMap.get(node);
      if (nullable != null) {
        return nullable;
      }
    }
    return parent == null ? null : parent.isKnownNullable(node);
  }

  /** Creates a read-only copy of this translator that records that a given
   * expression is nullable. */
  public RexToLixTranslator setNullable(RexNode e, boolean nullable) {
    return setNullable(Collections.singletonMap(e, nullable));
  }

  /** Creates a read-only copy of this translator that records that a given
   * expression is nullable. */
  public RexToLixTranslator setNullable(
      Map<? extends RexNode, Boolean> nullable) {
    if (nullable == null || nullable.isEmpty()) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        nullable, builder, conformance, this, correlates);
  }

  public RexToLixTranslator setBlock(BlockBuilder block) {
    if (block == list) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        block, ImmutableMap.of(), builder, conformance, this, correlates);
  }

  public RexToLixTranslator setCorrelates(
      Function1<String, InputGetter> correlates) {
    if (this.correlates == correlates) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), builder, conformance, this, correlates);
  }

  private RexToLixTranslator withConformance(SqlConformance conformance) {
    if (conformance == this.conformance) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        Collections.emptyMap(), builder, conformance, this, correlates);
  }

  public RelDataType nullifyType(RelDataType type, boolean nullable) {
    if (!nullable) {
      final Primitive primitive = javaPrimitive(type);
      if (primitive != null) {
        return typeFactory.createJavaType(primitive.primitiveClass);
      }
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  private Primitive javaPrimitive(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      return Primitive.ofBox(
          ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
    }
    return null;
  }

  public Expression getRoot() {
    return root;
  }

  private static Expression scaleIntervalToNumber(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    switch (targetType.getSqlTypeName().getFamily()) {
    case NUMERIC:
      switch (sourceType.getSqlTypeName()) {
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
        // Scale to the given field.
        final BigDecimal multiplier = BigDecimal.ONE;
        final BigDecimal divider =
            sourceType.getSqlTypeName().getEndUnit().multiplier;
        return RexImpTable.multiplyDivide(operand, multiplier, divider);
      }
    }
    return operand;
  }

  /** Translates a field of an input to an expression. */
  public interface InputGetter {
    Expression field(BlockBuilder list, int index, Type storageType);
  }

  /** Implementation of {@link InputGetter} that calls
   * {@link PhysType#fieldReference}. */
  public static class InputGetterImpl implements InputGetter {
    private List<Pair<Expression, PhysType>> inputs;

    public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
      this.inputs = inputs;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      int offset = 0;
      for (Pair<Expression, PhysType> input : inputs) {
        final PhysType physType = input.right;
        int fieldCount = physType.getRowType().getFieldCount();
        if (index >= offset + fieldCount) {
          offset += fieldCount;
          continue;
        }
        final Expression left = list.append("current", input.left);
        return physType.fieldReference(left, index - offset, storageType);
      }
      throw new IllegalArgumentException("Unable to find field #" + index);
    }
  }

  /** Thrown in the unusual (but not erroneous) situation where the expression
   * we are translating is the null literal but we have already checked that
   * it is not null. It is easier to throw (and caller will always handle)
   * than to check exhaustively beforehand. */
  static class AlwaysNull extends ControlFlowException {
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final AlwaysNull INSTANCE = new AlwaysNull();

    private AlwaysNull() {}
  }
}
