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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
    final RexUtil.ExprSimplifier defaultSimplifier =
        new RexUtil.ExprSimplifier(f.simplify, RexUnknownAs.UNKNOWN, true);
    final RexUtil.ExprSimplifier nonMatchingNullabilitySimplifier =
        new RexUtil.ExprSimplifier(f.simplify, RexUnknownAs.UNKNOWN, false);

    // The cast is nullable, while the literal is not nullable. When we simplify
    // it, we end up with the literal. If defaultSimplifier is used, a CAST is
    // introduced on top of the expression, as nullability of the new expression
    // does not match the nullability of the original one. If
    // nonMatchingNullabilitySimplifier is used, the CAST is not added and the
    // simplified expression only consists of the literal.
    final RexNode e = f.cast(f.intRelDataType, f.literal(2014));
    assertThat(
        f.simplify.simplifyPreservingType(e, RexUnknownAs.UNKNOWN, true)
            .toString(),
        is("CAST(2014):JavaType(class java.lang.Integer)"));
    assertThat(
        f.simplify.simplifyPreservingType(e, RexUnknownAs.UNKNOWN, false)
            .toString(),
        is("2014"));

    // In this case, the cast is not nullable. Thus, in both cases, the
    // simplified expression only consists of the literal.
    RelDataType notNullIntRelDataType = f.typeFactory.createJavaType(int.class);
    final RexNode e2 = f.cast(notNullIntRelDataType,
        f.cast(notNullIntRelDataType, f.literal(2014)));
    assertThat(
        f.simplify.simplifyPreservingType(e2, RexUnknownAs.UNKNOWN, true)
            .toString(),
        is("2014"));
    assertThat(
        f.simplify.simplifyPreservingType(e2, RexUnknownAs.UNKNOWN, false)
            .toString(),
        is("2014"));
  }

  /** Test case for simplifier of ceil/floor. */
  @Test public void testSimplifyCeilFloor() {
    // We can add more time units here once they are supported in
    // RexInterpreter, e.g., TimeUnitRange.HOUR, TimeUnitRange.MINUTE,
    // TimeUnitRange.SECOND.
    final ImmutableList<TimeUnitRange> timeUnitRanges =
        ImmutableList.of(TimeUnitRange.YEAR, TimeUnitRange.MONTH);
    final Fixture f = new Fixture();
<<<<<<< HEAD
=======
    final RexUtil.ExprSimplifier defaultSimplifier =
        new RexUtil.ExprSimplifier(f.simplify, RexUnknownAs.UNKNOWN, true);
>>>>>>> e84a51205 ([CALCITE-2604] When simplifying an expression, say whether an UNKNOWN value will be interpreted as is, or as TRUE or FALSE)

    final RexNode literalTs =
        f.timestampLiteral(new TimestampString("2010-10-10 00:00:00"));
    for (int i = 0; i < timeUnitRanges.size(); i++) {
      final RexNode innerFloorCall = f.rexBuilder.makeCall(
          SqlStdOperatorTable.FLOOR, literalTs,
          f.rexBuilder.makeFlag(timeUnitRanges.get(i)));
      final RexNode innerCeilCall = f.rexBuilder.makeCall(
          SqlStdOperatorTable.CEIL, literalTs,
          f.rexBuilder.makeFlag(timeUnitRanges.get(i)));
      for (int j = 0; j <= i; j++) {
        final RexNode outerFloorCall = f.rexBuilder.makeCall(
            SqlStdOperatorTable.FLOOR, innerFloorCall,
            f.rexBuilder.makeFlag(timeUnitRanges.get(j)));
        final RexNode outerCeilCall = f.rexBuilder.makeCall(
            SqlStdOperatorTable.CEIL, innerCeilCall,
            f.rexBuilder.makeFlag(timeUnitRanges.get(j)));
        final RexCall floorSimplifiedExpr = (RexCall) defaultSimplifier.apply(outerFloorCall);
        assertThat(floorSimplifiedExpr.getKind(), is(SqlKind.FLOOR));
        assertThat(((RexLiteral) floorSimplifiedExpr.getOperands().get(1)).getValue().toString(),
            is(timeUnitRanges.get(j).toString()));
        assertThat(floorSimplifiedExpr.getOperands().get(0).toString(), is(literalTs.toString()));
        final RexCall ceilSimplifiedExpr = (RexCall) defaultSimplifier.apply(outerCeilCall);
        assertThat(ceilSimplifiedExpr.getKind(), is(SqlKind.CEIL));
        assertThat(((RexLiteral) ceilSimplifiedExpr.getOperands().get(1)).getValue().toString(),
            is(timeUnitRanges.get(j).toString()));
        assertThat(ceilSimplifiedExpr.getOperands().get(0).toString(), is(literalTs.toString()));
      }
    }

    // Negative test
    for (int i = timeUnitRanges.size() - 1; i >= 0; i--) {
      final RexNode innerFloorCall = f.rexBuilder.makeCall(
          SqlStdOperatorTable.FLOOR, literalTs,
          f.rexBuilder.makeFlag(timeUnitRanges.get(i)));
      final RexNode innerCeilCall = f.rexBuilder.makeCall(
          SqlStdOperatorTable.CEIL, literalTs,
          f.rexBuilder.makeFlag(timeUnitRanges.get(i)));
      for (int j = timeUnitRanges.size() - 1; j > i; j--) {
        final RexNode outerFloorCall = f.rexBuilder.makeCall(
            SqlStdOperatorTable.CEIL, innerFloorCall,
            f.rexBuilder.makeFlag(timeUnitRanges.get(j)));
        final RexNode outerCeilCall = f.rexBuilder.makeCall(
            SqlStdOperatorTable.CEIL, innerCeilCall,
            f.rexBuilder.makeFlag(timeUnitRanges.get(j)));
        final RexCall floorSimplifiedExpr = (RexCall) defaultSimplifier.apply(outerFloorCall);
        assertThat(floorSimplifiedExpr.toString(), is(outerFloorCall.toString()));
        final RexCall ceilSimplifiedExpr = (RexCall) defaultSimplifier.apply(outerCeilCall);
        assertThat(ceilSimplifiedExpr.toString(), is(outerCeilCall.toString()));
      }
    }
  }

  @Test public void testRexSimplifyDateCharImplicitCast() {
    Fixture f = new Fixture();
    RexNode dateLiteral = f.dateLiteral(new DateString(2020, 12, 31));
    RexNode stringLiteral = f.charLiteral("2020-12-31");
    RexNode dateRef = f.d;
    RexNode condition = f.and(f.eq(dateRef, dateLiteral), f.eq(dateRef, stringLiteral));
    RexNode simp = f.simplifyUnknownFalse.simplify(condition);
    assertThat(simp.isAlwaysFalse(), is(false));
  }

  @Test public void testRexSimplifyLongCharImplicitCast() {
    Fixture f = new Fixture();
    RexNode longLiteral = f.longLiteral(2020);
    RexNode stringLiteral = f.charLiteral("2020");
    RexNode longRef = f.lg;
    RexNode condition = f.and(f.eq(longRef, longLiteral), f.eq(longRef, stringLiteral));
    RexNode simp = f.simplifyUnknownFalse.simplify(condition);
    assertThat(simp.isAlwaysFalse(), is(false));
  }

  @Test public void testRexSimplifyLongFloatImplicitCast() {
    Fixture f = new Fixture();
    RexNode longLiteral = f.longLiteral(2020);
    RexNode floatLiteral = f.floatLiteral(2020D);
    RexNode longRef = f.lg;
    RexNode condition = f.and(f.eq(longRef, longLiteral), f.eq(longRef, floatLiteral));
    RexNode simp = f.simplifyUnknownFalse.simplify(condition);
    assertThat(simp.isAlwaysFalse(), is(false));
  }

  /** Contains all the nourishment a test case could possibly need.
   *
   * <p>We put the data in here, rather than as fields in the test case, so that
   * the data can be garbage-collected as soon as the test has executed.
   */
  @SuppressWarnings("WeakerAccess")
  public static class Fixture {
    public final RelDataTypeFactory typeFactory;
    public final RexBuilder rexBuilder;
    public final RelDataType boolRelDataType;
    public final RelDataType intRelDataType;
    public final RelDataType decRelDataType;
    public final RelDataType longRelDataType;
    public final RelDataType shortDataType;
    public final RelDataType byteDataType;
    public final RelDataType floatDataType;
    public final RelDataType charDataType;
    public final RelDataType dateDataType;
    public final RelDataType timestampDataType;
    public final RelDataType timeDataType;
    public final RelDataType stringDataType;

    public final RexNode bl; // a field of Java type "Boolean"
    public final RexNode i; // a field of Java type "Integer"
    public final RexNode dec; // a field of Java type "Double"
    public final RexNode lg; // a field of Java type "Long"
    public final RexNode sh; // a  field of Java type "Short"
    public final RexNode by; // a field of Java type "Byte"
    public final RexNode fl; // a field of Java type "Float" (not a SQL FLOAT)
    public final RexNode d; // a field of Java type "Date"
    public final RexNode ch; // a field of Java type "Character"
    public final RexNode ts; // a field of Java type "Timestamp"
    public final RexNode t; // a field of Java type "Time"
    public final RexNode str; // a field of Java type "String"

    public final RexImplicationChecker checker;
    public final RelDataType rowType;
    public final RexExecutorImpl executor;
    public final RexSimplify simplify;
    public final RexSimplify simplifyUnknownFalse;

    public Fixture() {
      typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      rexBuilder = new RexBuilder(typeFactory);
      boolRelDataType = typeFactory.createJavaType(Boolean.class);
      intRelDataType = typeFactory.createJavaType(Integer.class);
      decRelDataType = typeFactory.createJavaType(Double.class);
      longRelDataType = typeFactory.createJavaType(Long.class);
      shortDataType = typeFactory.createJavaType(Short.class);
      byteDataType = typeFactory.createJavaType(Byte.class);
      floatDataType = typeFactory.createJavaType(Float.class);
      charDataType = typeFactory.createJavaType(Character.class);
      dateDataType = typeFactory.createJavaType(Date.class);
      timestampDataType = typeFactory.createJavaType(Timestamp.class);
      timeDataType = typeFactory.createJavaType(Time.class);
      stringDataType = typeFactory.createJavaType(String.class);

      bl = ref(0, this.boolRelDataType);
      i = ref(1, intRelDataType);
      dec = ref(2, decRelDataType);
      lg = ref(3, longRelDataType);
      sh = ref(4, shortDataType);
      by = ref(5, byteDataType);
      fl = ref(6, floatDataType);
      ch = ref(7, charDataType);
      d = ref(8, dateDataType);
      ts = ref(9, timestampDataType);
      t = ref(10, timeDataType);
      str = ref(11, stringDataType);

      rowType = typeFactory.builder()
          .add("bool", this.boolRelDataType)
          .add("int", intRelDataType)
          .add("dec", decRelDataType)
          .add("long", longRelDataType)
          .add("short", shortDataType)
          .add("byte", byteDataType)
          .add("float", floatDataType)
          .add("char", charDataType)
          .add("date", dateDataType)
          .add("timestamp", timestampDataType)
          .add("time", timeDataType)
          .add("string", stringDataType)
          .build();

      executor = Frameworks.withPrepare(
          (cluster, relOptSchema, rootSchema, statement) ->
              new RexExecutorImpl(
                  Schemas.createDataContext(statement.getConnection(),
                      rootSchema)));
      simplify =
          new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, false,
              executor).withParanoid(true);
      simplifyUnknownFalse =
              new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true,
                      executor);
      checker = new RexImplicationChecker(rexBuilder, executor, rowType);
    }

    public RexInputRef ref(int i, RelDataType type) {
      return new RexInputRef(i,
          typeFactory.createTypeWithNullability(type, true));
    }

    public RexLiteral literal(int i) {
      return rexBuilder.makeExactLiteral(new BigDecimal(i));
    }

    public RexNode gt(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, node1, node2);
    }

    public RexNode ge(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, node1, node2);
    }

    public RexNode eq(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, node1, node2);
    }

    public RexNode ne(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, node1, node2);
    }

    public RexNode lt(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, node1, node2);
    }

    public RexNode le(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, node1,
          node2);
    }

    public RexNode notNull(RexNode node1) {
      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, node1);
    }

    public RexNode isNull(RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, node2);
    }

    public RexNode and(RexNode... nodes) {
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, nodes);
    }

    public RexNode or(RexNode... nodes) {
      return rexBuilder.makeCall(SqlStdOperatorTable.OR, nodes);
    }

    public RexNode longLiteral(long value) {
      return rexBuilder.makeLiteral(value, longRelDataType, true);
    }

    public RexNode shortLiteral(short value) {
      return rexBuilder.makeLiteral(value, shortDataType, true);
    }

    public RexLiteral floatLiteral(double value) {
      return rexBuilder.makeApproxLiteral(new BigDecimal(value));
    }

    public RexLiteral charLiteral(String z) {
      return rexBuilder.makeCharLiteral(
          new NlsString(z, null, SqlCollation.COERCIBLE));
    }

    public RexNode dateLiteral(DateString d) {
      return rexBuilder.makeDateLiteral(d);
    }

    public RexNode timestampLiteral(TimestampString ts) {
      return rexBuilder.makeTimestampLiteral(ts,
          timestampDataType.getPrecision());
    }

    public RexNode timestampLocalTzLiteral(TimestampString ts) {
      return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(ts,
          timestampDataType.getPrecision());
    }

    public RexNode timeLiteral(TimeString t) {
      return rexBuilder.makeTimeLiteral(t, timeDataType.getPrecision());
    }

    public RexNode cast(RelDataType type, RexNode exp) {
      return rexBuilder.makeCast(type, exp, true);
    }

    void checkImplies(RexNode node1, RexNode node2) {
      assertTrue(checker.implies(node1, node2),
          () -> node1 + " does not imply " + node2 + " when it should");
    }

    void checkNotImplies(RexNode node1, RexNode node2) {
      assertFalse(checker.implies(node1, node2),
          () -> node1 + " does implies " + node2 + " when it should not");
    }
  }
}
