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
package org.apache.calcite.rel.type;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Glossary;

/**
 * Type system.
 *
 * <p>Provides behaviors concerning type limits and behaviors. For example,
 * in the default system, a DECIMAL can have maximum precision 19, but Hive
 * overrides to 38.
 *
 * <p>The default implementation is {@link #DEFAULT}.
 */
public interface RelDataTypeSystem {
  /** Default type system. */
  RelDataTypeSystem DEFAULT = new RelDataTypeSystemImpl() { };

  /** Returns the maximum scale of a given type. */
  int getMaxScale(SqlTypeName typeName);

  /**
   * Returns default precision for this type if supported, otherwise -1 if
   * precision is either unsupported or must be specified explicitly.
   *
   * @return Default precision
   */
  int getDefaultPrecision(SqlTypeName typeName);

  /**
   * Returns the maximum precision (or length) allowed for this type, or -1 if
   * precision/length are not applicable for this type.
   *
   * @return Maximum allowed precision
   */
  int getMaxPrecision(SqlTypeName typeName);

  /** Returns the maximum scale of a NUMERIC or DECIMAL type. */
  int getMaxNumericScale();

  /** Returns the maximum precision of a NUMERIC or DECIMAL type. */
  int getMaxNumericPrecision();

  /** Returns the LITERAL string for the type, either PREFIX/SUFFIX. */
  String getLiteral(SqlTypeName typeName, boolean isPrefix);

  /** Returns whether the type is case sensitive. */
  boolean isCaseSensitive(SqlTypeName typeName);

  /** Returns whether the type can be auto increment. */
  boolean isAutoincrement(SqlTypeName typeName);

  /** Returns the numeric type radix, typically 2 or 10.
   * 0 means "not applicable". */
  int getNumTypeRadix(SqlTypeName typeName);

  /** Returns the return type of a call to the {@code SUM} aggregate function,
   * inferred from its argument type. */
  RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType);

  /** Returns the return type of a call to the {@code AVG}, {@code STDDEV} or
   * {@code VAR} aggregate functions, inferred from its argument type.
   */
  RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType);

  /** Returns the return type of a call to the {@code COVAR} aggregate function,
   * inferred from its argument types. */
  RelDataType deriveCovarType(RelDataTypeFactory typeFactory,
      RelDataType arg0Type, RelDataType arg1Type);

  /** Returns the return type of the {@code CUME_DIST} and {@code PERCENT_RANK}
   * aggregate functions. */
  RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory);

  /** Returns the return type of the {@code NTILE}, {@code RANK},
   * {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate functions. */
  RelDataType deriveRankType(RelDataTypeFactory typeFactory);

  /** Whether two record types are considered distinct if their field names
   * are the same but in different cases. */
  boolean isSchemaCaseSensitive();

  /**
   * Infers the return type of a decimal addition. Decimal addition involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   *
   * Type-inference strategy whereby the result type of a call is the decimal
   * sum of two exact numeric operands where at least one of the operands is a
   * decimal. Let p1, s1 be the precision and scale of the first operand Let
   * p2, s2 be the precision and scale of the second operand Let p, s be the
   * precision and scale of the result, Then the result type is a decimal
   * with:
   *
   * <ul>
   * <li>s = max(s1, s2)</li>
   * <li>p = max(p1 - s1, p2 - s2) + s + 1</li>
   * </ul>
   *
   * <p>p and s are capped at their maximum values
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal addition.
   */
  RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2);

  /**
   * Infers the return type of a decimal multiplication. Decimal
   * multiplication involves at least one decimal operand and requires both
   * operands to have exact numeric types.
   *
   * Implemented with SQL 2003 compliant behavior. Let p1,
   * s1 be the precision and scale of the first operand Let p2, s2 be the
   * precision and scale of the second operand Let p, s be the precision and
   * scale of the result, Then the result type is a decimal with:
   *
   * <ul>
   * <li>p = p1 + p2</li>
   * <li>s = s1 + s2</li>
   * </ul>
   *
   * <p>p and s are capped at their maximum values
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal multiplication, or null if decimal
   * multiplication should not be applied to the operands.
   */
  RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2);

  /**
   * Infers the return type of a decimal division. Decimal division involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   * Rules:
   *
   * <ul>
   * <li>Let p1, s1 be the precision and scale of the first operand
   * <li>Let p2, s2 be the precision and scale of the second operand
   * <li>Let p, s be the precision and scale of the result
   * <li>Let d be the number of whole digits in the result
   * <li>Then the result type is a decimal with:
   *   <ul>
   *   <li>d = p1 - s1 + s2</li>
   *   <li>s &lt; max(6, s1 + p2 + 1)</li>
   *   <li>p = d + s</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * @see Glossary#SQL2003 SQL:2003 Part 2 Section 6.26
   *
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal division, or null if decimal
   * division should not be applied to the operands.
   */
  RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
       RelDataType type1, RelDataType type2);

  /**
   * Infers the return type of a decimal mod operation. Decimal mod involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   *
   * Always returns the type of the second argument as the return type
   * for the operation.
   * @param typeFactory typeFactory used to create output type
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal mod, or null if decimal
   * mod should not be applied to the operands.
   */
  RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
       RelDataType type1, RelDataType type2);

  // TODO both deriveDecimalTruncateType and deriveDecimalRoundType would require refactoring
  //  post calcite 1.16 (to use operator table instead)
  /**
   * Infers the return type of a decimal truncate operation. Decimal truncate
   * involves at least one decimal operand.
   *
   * The result type is a decimal with precision as that of the first argument
   * and scale as scale2
   *
   * @param typeFactory TypeFactory used to create output type
   * @param type1       Type of the first operand
   * @param scale2      Scale value to truncate to
   * @return Result type for a decimal truncate
   */
  RelDataType deriveDecimalTruncateType(RelDataTypeFactory typeFactory,
      RelDataType type1, Integer scale2);

  /**
   * Infers the return type of a decimal round operation. Decimal round involves at least one
   * decimal operand.
   *
   * The result type is a decimal with precision as that of the first argument
   * and scale as scale2
   *
   * @param typeFactory TypeFactory used to create output type
   * @param type1       Type of the first operand
   * @param scale2      Scale value to round to
   * @return Result type for a decimal round
   */
  RelDataType deriveDecimalRoundType(
      RelDataTypeFactory typeFactory, RelDataType type1, Integer scale2);
}

// End RelDataTypeSystem.java
