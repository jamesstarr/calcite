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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
>>>>>>> 42ca94873 (Apply small refactorings to Calcite codebase (Java 5, Java 7, Java 8))
=======
    Map<String, Long> fieldCnt = fieldList.stream()
        .map(RelDataTypeField::getName)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    int i = -1;
    for (RelDataTypeField field : fieldList) {
      ++i;
      types.add(field.getType());
      String oriFieldName = field.getName();
      // Patch up the field name with index if there are duplicates.
      // There is still possibility that the patched name conflicts with existing ones,
      // but that should be rare case.
      String fieldName = fieldCnt.get(oriFieldName) > 1
          ? oriFieldName + "_" + i
          : oriFieldName;
      fieldNames.add(fieldName);
    }
    return typeFactory.createStructType(types, fieldNames);
  }

  public static boolean needsNullIndicator(RelDataType recordType) {
    // NOTE jvs 9-Mar-2005: It would be more storage-efficient to say that
    // no null indicator is required for structured type columns declared
    // as NOT NULL.  However, the uniformity of always having a null
    // indicator makes things cleaner in many places.
    return recordType.getSqlTypeName() == SqlTypeName.STRUCTURED;
  }

  private static boolean flattenFields(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      List<RelDataTypeField> list,
      int[] flatteningMap) {
    boolean nested = false;
    for (RelDataTypeField field : type.getFieldList()) {
      if (flatteningMap != null) {
        flatteningMap[field.getIndex()] = list.size();
      }
      if (field.getType().isStruct()) {
        nested = true;
        flattenFields(
            typeFactory,
            field.getType(),
            list,
            null);
      } else if (field.getType().getComponentType() != null) {
        nested = true;

        // TODO jvs 14-Feb-2005:  generalize to any kind of
        // collection type
        RelDataType flattenedCollectionType =
            typeFactory.createMultisetType(
                flattenRecordType(
                    typeFactory,
                    field.getType().getComponentType(),
                    null),
                -1);
        if (field.getType() instanceof ArraySqlType) {
          flattenedCollectionType =
              typeFactory.createArrayType(
                  flattenRecordType(
                      typeFactory,
                      field.getType().getComponentType(),
                      null),
                  -1);
        }
        field =
            new RelDataTypeFieldImpl(
                field.getName(),
                field.getIndex(),
                flattenedCollectionType);
        list.add(field);
      } else {
        list.add(field);
      }
    }
    return nested;
  }

  /**
   * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
   *
   * @param type         type descriptor
   * @param charSetName  charSet name
   * @param maxPrecision The max allowed precision.
   * @return corresponding parse representation
   */
  public static SqlDataTypeSpec convertTypeToSpec(RelDataType type,
      String charSetName, int maxPrecision) {
    SqlTypeName typeName = type.getSqlTypeName();

    // TODO jvs 28-Dec-2004:  support row types, user-defined types,
    // interval types, multiset types, etc
    assert typeName != null;

    int precision = typeName.allowsPrec() ? type.getPrecision() : -1;
    // fix up the precision.
    if (maxPrecision > 0 && precision > maxPrecision) {
      precision = maxPrecision;
    }
    int scale = typeName.allowsScale() ? type.getScale() : -1;

    final SqlBasicTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(
        typeName,
        precision,
        scale,
        charSetName,
        SqlParserPos.ZERO);

    // REVIEW jvs 28-Dec-2004:  discriminate between precision/scale
    // zero and unspecified?

    // REVIEW angel 11-Jan-2006:
    // Use neg numbers to indicate unspecified precision/scale

    return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
  }

  /**
   * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
   *
   * @param type type descriptor
   * @return corresponding parse representation
   */
  public static SqlDataTypeSpec convertTypeToSpec(RelDataType type) {
    // TODO jvs 28-Dec-2004:  collation
    String charSetName = inCharFamily(type) ? type.getCharset().name() : null;
    return convertTypeToSpec(type, charSetName, -1);
  }

  public static RelDataType createMultisetType(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      boolean nullable) {
    RelDataType ret = typeFactory.createMultisetType(type, -1);
    return typeFactory.createTypeWithNullability(ret, nullable);
  }

  public static RelDataType createArrayType(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      boolean nullable) {
    RelDataType ret = typeFactory.createArrayType(type, -1);
    return typeFactory.createTypeWithNullability(ret, nullable);
  }

  public static RelDataType createMapType(
      RelDataTypeFactory typeFactory,
      RelDataType keyType,
      RelDataType valueType,
      boolean nullable) {
    RelDataType ret = typeFactory.createMapType(keyType, valueType);
    return typeFactory.createTypeWithNullability(ret, nullable);
  }

  /** Creates a MAP type from a record type. The record type must have exactly
   * two fields. */
  public static RelDataType createMapTypeFromRecord(
      RelDataTypeFactory typeFactory, RelDataType type) {
    Preconditions.checkArgument(type.getFieldCount() == 2,
        "MAP requires exactly two fields, got %s; row type %s",
        type.getFieldCount(), type);
    return createMapType(typeFactory, type.getFieldList().get(0).getType(),
        type.getFieldList().get(1).getType(), false);
  }

  /**
   * Adds collation and charset to a character type, returns other types
   * unchanged.
   *
   * @param type        Type
   * @param typeFactory Type factory
   * @return Type with added charset and collation, or unchanged type if it is
   * not a char type.
   */
  public static RelDataType addCharsetAndCollation(
      RelDataType type,
      RelDataTypeFactory typeFactory) {
    if (!inCharFamily(type)) {
      return type;
    }
    Charset charset = type.getCharset();
    if (charset == null) {
      charset = typeFactory.getDefaultCharset();
    }
    SqlCollation collation = type.getCollation();
    if (collation == null) {
      collation = SqlCollation.IMPLICIT;
    }

    // todo: should get the implicit collation from repository
    //   instead of null
    type =
        typeFactory.createTypeWithCharsetAndCollation(
            type,
            charset,
            collation);
    SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
    return type;
  }

  /**
   * Returns whether two types are equal, ignoring nullability.
   *
   * <p>They need not come from the same factory.
   *
   * @param factory Type factory
   * @param type1   First type
   * @param type2   Second type
   * @return whether types are equal, ignoring nullability
   */
  public static boolean equalSansNullability(
      RelDataTypeFactory factory,
      RelDataType type1,
      RelDataType type2) {
    if (type1.equals(type2)) {
      return true;
    }

    if (type1.isNullable() == type2.isNullable()) {
      // If types have the same nullability and they weren't equal above,
      // they must be different.
      return false;
    }
    return type1.equals(
        factory.createTypeWithNullability(type2, type1.isNullable()));
  }

  /**
   * This is a poorman's
   * {@link #equalSansNullability(RelDataTypeFactory, RelDataType, RelDataType)}.
   *
   * <p>We assume that "not null" is represented in the type's digest
   * as a trailing "NOT NULL" (case sensitive).
   *
   * <p>If you got a type factory,
   * {@link #equalSansNullability(RelDataTypeFactory, RelDataType, RelDataType)}
   * is preferred.
   *
   * @param type1 First type
   * @param type2 Second type
   * @return true if the types are equal or the only difference is nullability
   */
  public static boolean equalSansNullability(RelDataType type1, RelDataType type2) {
    if (type1 == type2) {
      return true;
    }
    String x = type1.getFullTypeString();
    String y = type2.getFullTypeString();
    if (x.length() < y.length()) {
      String c = x;
      x = y;
      y = c;
    }

    return (x.length() == y.length()
            || x.length() == y.length() + NON_NULLABLE_SUFFIX.length()
            && x.endsWith(NON_NULLABLE_SUFFIX)) && x.startsWith(y);
  }

  /**
   * Returns whether two struct types are equal, ignoring nullability.
   *
   * <p>They do not need to come from the same factory.
   *
   * @param factory       Type factory
   * @param type1         First type
   * @param type2         Second type
   * @param nameMatcher   Name matcher used to compare the field names
   * @return Whether types are equal, ignoring nullability
   */
  public static boolean equalAsStructSansNullability(
      RelDataTypeFactory factory,
      RelDataType type1,
      RelDataType type2,
      SqlNameMatcher nameMatcher) {
    assert type1.isStruct();
    assert type2.isStruct();

    if (type1.getFieldCount() != type2.getFieldCount()) {
      return false;
    }

    for (Pair<RelDataTypeField, RelDataTypeField> pair
        : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
      if (!nameMatcher.matches(pair.left.getName(), pair.right.getName())) {
        return false;
      }
      if (!equalSansNullability(factory, pair.left.getType(), pair.right.getType())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns the ordinal of a given field in a record type, or -1 if the field
   * is not found.
   *
   * <p>The {@code fieldName} is always simple, if the field is nested within a record field,
   * returns index of the outer field instead. i.g. for row type
   * (a int, b (b1 bigint, b2 varchar(20) not null)), returns 1 for both simple name "b1" and "b2".
   *
   * @param type      Record type
   * @param fieldName Name of field
   * @return Ordinal of field
   */
  public static int findField(RelDataType type, String fieldName) {
    List<RelDataTypeField> fields = type.getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      if (field.getName().equals(fieldName)) {
        return i;
      }
      final RelDataType fieldType = field.getType();
      if (fieldType.isStruct() && findField(fieldType, fieldName) != -1) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Selects data types of the specified fields from an input row type.
   * This is useful when identifying data types of a function that is going
   * to operate on inputs that are specified as field ordinals (e.g.
   * aggregate calls).
   *
   * @param rowType input row type
   * @param requiredFields ordinals of the projected fields
   * @return list of data types that are requested by requiredFields
   */
  public static List<RelDataType> projectTypes(final RelDataType rowType,
      final List<? extends Number> requiredFields) {
    final List<RelDataTypeField> fields = rowType.getFieldList();

    return new AbstractList<RelDataType>() {
      @Override public RelDataType get(int index) {
        return fields.get(requiredFields.get(index).intValue()).getType();
      }

      @Override public int size() {
        return requiredFields.size();
      }
    };
  }

  /**
   * Records a struct type with no fields.
   *
   * @param typeFactory Type factory
   * @return Struct type with no fields
   */
  public static RelDataType createEmptyStructType(
      RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(
        ImmutableList.of(),
        ImmutableList.of());
  }

  /** Returns whether a type is flat. It is not flat if it is a record type that
   * has one or more fields that are themselves record types. */
  public static boolean isFlat(RelDataType type) {
    if (type.isStruct()) {
      for (RelDataTypeField field : type.getFieldList()) {
        if (field.getType().isStruct()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks that there is no expression inside list which may
   * return struct type.
   *
   * @param exprList list of expressions to check
   * @return true if all expressions don't return struct type
   */
  public static boolean isFlat(List<RexNode> exprList) {
    return exprList.stream()
        .map(RexNode::getType)
        .noneMatch(RelDataType::isStruct);
  }

  /**
   * Returns whether two types are comparable. They need to be scalar types of
   * the same family, or struct types whose fields are pairwise comparable.
   *
   * @param type1 First type
   * @param type2 Second type
   * @return Whether types are comparable
   */
  public static boolean isComparable(RelDataType type1, RelDataType type2) {
    if (type1.isStruct() != type2.isStruct()) {
      return false;
    }

    if (type1.isStruct()) {
      int n = type1.getFieldCount();
      if (n != type2.getFieldCount()) {
        return false;
      }
      for (Pair<RelDataTypeField, RelDataTypeField> pair
          : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
        if (!isComparable(pair.left.getType(), pair.right.getType())) {
          return false;
        }
      }
      return true;
    }

    final RelDataTypeFamily family1 = family(type1);
    final RelDataTypeFamily family2 = family(type2);
    if (family1 == family2) {
      return true;
    }

    // If one of the arguments is of type 'ANY', return true.
    if (family1 == SqlTypeFamily.ANY
        || family2 == SqlTypeFamily.ANY) {
      return true;
    }

    // If one of the arguments is of type 'NULL', return true.
    if (family1 == SqlTypeFamily.NULL
        || family2 == SqlTypeFamily.NULL) {
      return true;
    }

    // We can implicitly convert from character to date
    if (family1 == SqlTypeFamily.CHARACTER
        && canConvertStringInCompare(family2)
        || family2 == SqlTypeFamily.CHARACTER
        && canConvertStringInCompare(family1)) {
      return true;
    }

    if ((family1 == SqlTypeFamily.DATE && family2 == SqlTypeFamily.TIMESTAMP)
        || (family1 == SqlTypeFamily.TIMESTAMP && family2 == SqlTypeFamily.DATE)) {
      return true;
    }

    return false;
  }

  /** Returns the least restrictive type T, such that a value of type T can be
   * compared with values of type {@code type0} and {@code type1} using
   * {@code =}. */
  public static RelDataType leastRestrictiveForComparison(
          RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    final RelDataType type =
            typeFactory.leastRestrictive(ImmutableList.of(type1, type2));
    if (type != null) {
      return type;
    }
    final RelDataTypeFamily family1 = family(type1);
    final RelDataTypeFamily family2 = family(type2);

    // If one of the arguments is of type 'ANY', we can compare.
    if (family1 == SqlTypeFamily.ANY) {
      return type2;
    }
    if (family2 == SqlTypeFamily.ANY) {
      return type1;
    }

    // If one of the arguments is of type 'NULL', we can compare.
    if (family1 == SqlTypeFamily.NULL) {
      return type2;
    }
    if (family2 == SqlTypeFamily.NULL) {
      return type1;
    }

    // We can implicitly convert from character to date, numeric, etc.
    if (family1 == SqlTypeFamily.CHARACTER
            && canConvertStringInCompare(family2)) {
      return type2;
    }
    if (family2 == SqlTypeFamily.CHARACTER
            && canConvertStringInCompare(family1)) {
      return type1;
    }

    return null;
  }

  protected static RelDataTypeFamily family(RelDataType type) {
    // REVIEW jvs 2-June-2005:  This is needed to keep
    // the Saffron type system happy.
    RelDataTypeFamily family = null;
    if (type.getSqlTypeName() != null) {
      family = type.getSqlTypeName().getFamily();
    }
    if (family == null) {
      family = type.getFamily();
    }
    return family;
  }

  /**
   * Returns whether all types in a collection have the same family, as
   * determined by {@link #isSameFamily(RelDataType, RelDataType)}.
   *
   * @param types Types to check
   * @return true if all types are of the same family
   */
  public static boolean areSameFamily(Iterable<RelDataType> types) {
    final List<RelDataType> typeList = ImmutableList.copyOf(types);
    if (Sets.newHashSet(RexUtil.families(typeList)).size() < 2) {
      return true;
    }
    for (Pair<RelDataType, RelDataType> adjacent : Pair.adjacents(typeList)) {
      if (!isSameFamily(adjacent.left, adjacent.right)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether two types are scalar types of the same family, or struct types whose fields
   * are pairwise of the same family.
   *
   * @param type1 First type
   * @param type2 Second type
   * @return Whether types have the same family
   */
  private static boolean isSameFamily(RelDataType type1, RelDataType type2) {
    if (type1.isStruct() != type2.isStruct()) {
      return false;
    }

    if (type1.isStruct()) {
      int n = type1.getFieldCount();
      if (n != type2.getFieldCount()) {
        return false;
      }
      for (Pair<RelDataTypeField, RelDataTypeField> pair
          : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
        if (!isSameFamily(pair.left.getType(), pair.right.getType())) {
          return false;
        }
      }
      return true;
    }

    final RelDataTypeFamily family1 = family(type1);
    final RelDataTypeFamily family2 = family(type2);
    return family1 == family2;
  }

  /** Returns whether a character data type can be implicitly converted to a
   * given family in a compare operation. */
  private static boolean canConvertStringInCompare(RelDataTypeFamily family) {
    if (family instanceof SqlTypeFamily) {
      SqlTypeFamily sqlTypeFamily = (SqlTypeFamily) family;
      switch (sqlTypeFamily) {
      case DATE:
      case TIME:
      case TIMESTAMP:
      case INTERVAL_DAY_TIME:
      case INTERVAL_YEAR_MONTH:
      case NUMERIC:
      case APPROXIMATE_NUMERIC:
      case EXACT_NUMERIC:
      case INTEGER:
      case BOOLEAN:
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether a type represents Unicode character data.
   *
   * @param type type to test
   * @return whether type represents Unicode character data
   */
  public static boolean isUnicode(RelDataType type) {
    Charset charset = type.getCharset();
    if (charset == null) {
      return false;
    }
    return charset.name().startsWith("UTF");
  }

  /** Returns the larger of two precisions, treating
   * {@link RelDataType#PRECISION_NOT_SPECIFIED} as infinity. */
  public static int maxPrecision(int p0, int p1) {
    return (p0 == RelDataType.PRECISION_NOT_SPECIFIED
        || p0 >= p1
        && p1 != RelDataType.PRECISION_NOT_SPECIFIED) ? p0 : p1;
  }

  /** Returns whether a precision is greater or equal than another,
   * treating {@link RelDataType#PRECISION_NOT_SPECIFIED} as infinity. */
  public static int comparePrecision(int p0, int p1) {
    if (p0 == p1) {
      return 0;
    }
    if (p0 == RelDataType.PRECISION_NOT_SPECIFIED) {
      return 1;
    }
    if (p1 == RelDataType.PRECISION_NOT_SPECIFIED) {
      return -1;
    }
    return Integer.compare(p0, p1);
  }

  /**
   * @return true if type is ARRAY
   */
  public static boolean isArray(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ARRAY;
  }
<<<<<<< HEAD
=======

  public static RelDataType consistentType(RelDataTypeFactory typeFactory,
      SqlOperandTypeChecker.Consistency consistency, List<RelDataType> types) {
    switch (consistency) {
    case COMPARE:
      final Set<RelDataTypeFamily> families =
          Sets.newHashSet(RexUtil.families(types));
      if (families.size() < 2) {
        // All arguments are of same family. No need for explicit casts.
        return null;
      }
      final List<RelDataType> nonCharacterTypes = new ArrayList<>();
      for (RelDataType type : types) {
        if (type.getFamily() != SqlTypeFamily.CHARACTER) {
          nonCharacterTypes.add(type);
        }
      }
      if (!nonCharacterTypes.isEmpty()) {
        final int typeCount = types.size();
        types = nonCharacterTypes;
        if (nonCharacterTypes.size() < typeCount) {
          final RelDataTypeFamily family =
              nonCharacterTypes.get(0).getFamily();
          if (family instanceof SqlTypeFamily) {
            // The character arguments might be larger than the numeric
            // argument. Give ourselves some headroom.
            switch ((SqlTypeFamily) family) {
            case INTEGER:
            case NUMERIC:
              nonCharacterTypes.add(
                  typeFactory.createSqlType(SqlTypeName.BIGINT));
            }
          }
        }
      }
      // fall through
    case LEAST_RESTRICTIVE:
      return typeFactory.leastRestrictive(types);
    default:
      return null;
    }
  }

  /**
   * @return true if type is MAP
   */
  public static boolean isMap(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return type.getSqlTypeName() == SqlTypeName.MAP;
  }

  /**
   * @return true if type is CHARACTER
   */
  public static boolean isCharacter(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.CHARACTER.contains(type);
  }

  /**
   * @return true if the type is a CHARACTER or contains a CHARACTER type
   */
  public static boolean hasCharactor(RelDataType type) {
    if (isCharacter(type)) {
      return true;
    }
    if (isArray(type)) {
      return hasCharactor(type.getComponentType());
    }
    return false;
  }

  /**
   * @return true if type is STRING
   */
  public static boolean isString(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.STRING.contains(type);
  }

  /**
   * @return true if type is BOOLEAN
   */
  public static boolean isBoolean(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.BOOLEAN.contains(type);
  }

  /**
   * @return true if type is BINARY
   */
  public static boolean isBinary(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.BINARY.contains(type);
  }

  /**
   * @return true if type is Atomic
   */
  public static boolean isAtomic(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeUtil.isDatetime(type)
        || SqlTypeUtil.isNumeric(type)
        || SqlTypeUtil.isString(type)
        || SqlTypeUtil.isBoolean(type);
  }

  /** Get decimal with max precision/scale for the current type system. */
  public static RelDataType getMaxPrecisionScaleDecimal(RelDataTypeFactory factory) {
    int maxPrecision = factory.getTypeSystem().getMaxNumericPrecision();
    int maxScale = factory.getTypeSystem().getMaxNumericScale();

    return factory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, maxScale);
  }
}

  public static RelDataType consistentType(RelDataTypeFactory typeFactory,
      SqlOperandTypeChecker.Consistency consistency, List<RelDataType> types) {
    switch (consistency) {
    case COMPARE:
      final Set<RelDataTypeFamily> families =
          Sets.newHashSet(RexUtil.families(types));
      if (families.size() < 2) {
        // All arguments are of same family. No need for explicit casts.
        return null;
      }
      final List<RelDataType> nonCharacterTypes = new ArrayList<>();
      for (RelDataType type : types) {
        if (type.getFamily() != SqlTypeFamily.CHARACTER) {
          nonCharacterTypes.add(type);
        }
      }
      if (!nonCharacterTypes.isEmpty()) {
        final int typeCount = types.size();
        types = nonCharacterTypes;
        if (nonCharacterTypes.size() < typeCount) {
          final RelDataTypeFamily family =
              nonCharacterTypes.get(0).getFamily();
          if (family instanceof SqlTypeFamily) {
            // The character arguments might be larger than the numeric
            // argument. Give ourselves some headroom.
            switch ((SqlTypeFamily) family) {
            case INTEGER:
            case NUMERIC:
              nonCharacterTypes.add(
                  typeFactory.createSqlType(SqlTypeName.BIGINT));
            }
          }
        }
      }
      // fall through
    case LEAST_RESTRICTIVE:
      return typeFactory.leastRestrictive(types);
    default:
      return null;
    }
  }

  /**
   * @return true if type is ROW
   */
  public static boolean isRow(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    return type.getSqlTypeName() == SqlTypeName.ROW;
  }

  /**
   * @return true if type is MAP
   */
  public static boolean isMap(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return type.getSqlTypeName() == SqlTypeName.MAP;
  }

  /** Returns whether a type is MULTISET. */
  public static boolean isMultiset(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    return type.getSqlTypeName() == SqlTypeName.MULTISET;
  }

  /** Returns whether a type is ARRAY or MULTISET. */
  public static boolean isCollection(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    return type.getSqlTypeName() == SqlTypeName.ARRAY
        || type.getSqlTypeName() == SqlTypeName.MULTISET;
  }

  /**
   * @return true if type is CHARACTER
   */
  public static boolean isCharacter(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.CHARACTER.contains(type);
  }

  /**
   * @return true if the type is a CHARACTER or contains a CHARACTER type
   */
  public static boolean hasCharactor(RelDataType type) {
    if (isCharacter(type)) {
      return true;
    }
    if (isArray(type)) {
      return hasCharactor(type.getComponentType());
    }
    return false;
  }

  /**
   * @return true if type is STRING
   */
  public static boolean isString(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.STRING.contains(type);
  }

  /**
   * @return true if type is BOOLEAN
   */
  public static boolean isBoolean(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.BOOLEAN.contains(type);
  }

  /**
   * @return true if type is BINARY
   */
  public static boolean isBinary(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.BINARY.contains(type);
  }

  /**
   * @return true if type is Atomic
   */
  public static boolean isAtomic(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeUtil.isDatetime(type)
        || SqlTypeUtil.isNumeric(type)
        || SqlTypeUtil.isString(type)
        || SqlTypeUtil.isBoolean(type);
  }

  /** Get decimal with max precision/scale for the current type system. */
  public static RelDataType getMaxPrecisionScaleDecimal(RelDataTypeFactory factory) {
    int maxPrecision = factory.getTypeSystem().getMaxNumericPrecision();
    int maxScale = factory.getTypeSystem().getMaxNumericScale();

    return factory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, maxScale);
  }

  /**
   * Keeps only the last N fields and returns the new struct type.
   */
  public static RelDataType extractLastNFields(RelDataTypeFactory typeFactory,
      RelDataType type, int numToKeep) {
    assert type.isStruct();
    assert type.getFieldCount() >= numToKeep;
    final int fieldsCnt = type.getFieldCount();
    return typeFactory.createStructType(
        type.getFieldList().subList(fieldsCnt - numToKeep, fieldsCnt));
  }
}
