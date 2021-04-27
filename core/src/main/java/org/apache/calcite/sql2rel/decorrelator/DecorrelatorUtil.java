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

import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/** Common functions for decorrelation.*/
class DecorrelatorUtil {

  private DecorrelatorUtil() {
  }

  static RexNode removeCorrelationExpr(
      RelDecorrelator relDecorrelator,
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator,
      RexInputRef nullIndicator) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(relDecorrelator,
            projectPulledAboveLeftCorrelator, nullIndicator,
            ImmutableSet.of());
    return exp.accept(shuttle);
  }


  static RexNode removeCorrelationExpr(
      RelDecorrelator relDecorrelator,
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(relDecorrelator,
            projectPulledAboveLeftCorrelator, null, ImmutableSet.of());
    return exp.accept(shuttle);
  }

  static RexNode removeCorrelationExpr(
      RelDecorrelator relDecorrelator,
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator,
      Set<Integer> isCount) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(relDecorrelator,
            projectPulledAboveLeftCorrelator, null, isCount);
    return exp.accept(shuttle);
  }


  /** Shuttle that removes correlations. */
  private static class RemoveCorrelationRexShuttle extends RexShuttle {
    final RelDecorrelator relDecorrelator;
    final RexBuilder rexBuilder;
    final RelDataTypeFactory typeFactory;
    final boolean projectPulledAboveLeftCorrelator;
    final @Nullable RexInputRef nullIndicator;
    final ImmutableSet<Integer> isCount;

    RemoveCorrelationRexShuttle(
        RelDecorrelator relDecorrelator,
        boolean projectPulledAboveLeftCorrelator,
        @Nullable RexInputRef nullIndicator,
        Set<Integer> isCount) {
      this.relDecorrelator = relDecorrelator;
      this.projectPulledAboveLeftCorrelator = projectPulledAboveLeftCorrelator;
      this.nullIndicator = nullIndicator; // may be null
      this.isCount = ImmutableSet.copyOf(isCount);
      this.rexBuilder = relDecorrelator.relBuilder.getRexBuilder();
      this.typeFactory = rexBuilder.getTypeFactory();
    }

    private RexNode createCaseExpression(
        RexInputRef nullInputRef,
        @Nullable RexLiteral lit,
        RexNode rexNode) {
      RexNode[] caseOperands = new RexNode[3];

      // Construct a CASE expression to handle the null indicator.
      //
      // This also covers the case where a left correlated sub-query
      // projects fields from outer relation. Since LOJ cannot produce
      // nulls on the LHS, the projection now need to make a nullable LHS
      // reference using a nullability indicator. If this this indicator
      // is null, it means the sub-query does not produce any value. As a
      // result, any RHS ref by this sub-query needs to produce null value.

      // WHEN indicator IS NULL
      caseOperands[0] =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_NULL,
              new RexInputRef(
                  nullInputRef.getIndex(),
                  typeFactory.createTypeWithNullability(
                      nullInputRef.getType(),
                      true)));

      // THEN CAST(NULL AS newInputTypeNullable)
      caseOperands[1] =
          lit == null
              ? rexBuilder.makeNullLiteral(rexNode.getType())
              : rexBuilder.makeCast(rexNode.getType(), lit);

      // ELSE cast (newInput AS newInputTypeNullable) END
      caseOperands[2] =
          rexBuilder.makeCast(
              typeFactory.createTypeWithNullability(
                  rexNode.getType(),
                  true),
              rexNode);

      return rexBuilder.makeCall(
          SqlStdOperatorTable.CASE,
          caseOperands);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (relDecorrelator.cm.mapFieldAccessToCorRef.containsKey(fieldAccess)) {
        // if it is a corVar, change it to be input ref.
        RelDecorrelator.CorRef corVar = relDecorrelator.cm.mapFieldAccessToCorRef.get(fieldAccess);

        // corVar offset should point to the leftInput of currentRel,
        // which is the Correlate.
        RexNode newRexNode =
            new RexInputRef(corVar.field, fieldAccess.getType());

        if (projectPulledAboveLeftCorrelator
            && (nullIndicator != null)) {
          // need to enforce nullability by applying an additional
          // cast operator over the transformed expression.
          newRexNode =
              createCaseExpression(nullIndicator, null, newRexNode);
        }
        return newRexNode;
      }
      return fieldAccess;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (relDecorrelator.currentRel instanceof Correlate) {
        // if this rel references corVar
        // and now it needs to be rewritten
        // it must have been pulled above the Correlate
        // replace the input ref to account for the LHS of the
        // Correlate
        final int leftInputFieldCount =
            ((Correlate) relDecorrelator.currentRel).getLeft().getRowType()
                .getFieldCount();
        RelDataType newType = inputRef.getType();

        if (projectPulledAboveLeftCorrelator) {
          newType =
              typeFactory.createTypeWithNullability(newType, true);
        }

        int pos = inputRef.getIndex();
        RexInputRef newInputRef =
            new RexInputRef(leftInputFieldCount + pos, newType);

        if (isCount.contains(pos)) {
          return createCaseExpression(
              newInputRef,
              rexBuilder.makeExactLiteral(BigDecimal.ZERO),
              newInputRef);
        } else {
          return newInputRef;
        }
      }
      return inputRef;
    }

    @Override public RexNode visitLiteral(RexLiteral literal) {
      // Use nullIndicator to decide whether to project null.
      // Do nothing if the literal is null.
      if (!RexUtil.isNull(literal)
          && projectPulledAboveLeftCorrelator
          && (nullIndicator != null)) {
        return createCaseExpression(nullIndicator, null, literal);
      }
      return literal;
    }

    @Override public RexNode visitCall(final RexCall call) {
      RexNode newCall;

      boolean[] update = {false};
      List<RexNode> clonedOperands = visitList(call.operands, update);
      if (update[0]) {
        SqlOperator operator = call.getOperator();

        boolean isSpecialCast = false;
        if (operator instanceof SqlFunction) {
          SqlFunction function = (SqlFunction) operator;
          if (function.getKind() == SqlKind.CAST) {
            if (call.operands.size() < 2) {
              isSpecialCast = true;
            }
          }
        }

        final RelDataType newType;
        if (!isSpecialCast) {
          // TODO: ideally this only needs to be called if the result
          // type will also change. However, since that requires
          // support from type inference rules to tell whether a rule
          // decides return type based on input types, for now all
          // operators will be recreated with new type if any operand
          // changed, unless the operator has "built-in" type.
          newType = rexBuilder.deriveReturnType(operator, clonedOperands);
        } else {
          // Use the current return type when creating a new call, for
          // operators with return type built into the operator
          // definition, and with no type inference rules, such as
          // cast function with less than 2 operands.

          // TODO: Comments in RexShuttle.visitCall() mention other
          // types in this category. Need to resolve those together
          // and preferably in the base class RexShuttle.
          newType = call.getType();
        }
        newCall =
            rexBuilder.makeCall(
                newType,
                operator,
                clonedOperands);
      } else {
        newCall = call;
      }

      if (projectPulledAboveLeftCorrelator && (nullIndicator != null)) {
        return createCaseExpression(nullIndicator, null, newCall);
      }
      return newCall;
    }
  }

}
