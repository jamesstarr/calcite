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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
      SqlParserPos pos) {
    this(null, typeNameSpec, null, null, pos);
  }

  /**
   * Creates a type specification representing a regular, non-collection type.
   */
  public SqlDataTypeSpec(
      final SqlTypeNameSpec typeNameSpec,
      TimeZone timeZone,
      SqlParserPos pos) {
    this(null, typeNameSpec, timeZone, null, pos);
  }

  /**
   * Creates a type specification representing a type, with time zone specified.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type
   * @param timeZone     Specified time zone
   */
  public SqlDataTypeSpec(
      final SqlTypeNameSpec typeNameSpec,
      TimeZone timeZone,
      SqlParserPos pos) {
    this(typeNameSpec, timeZone, null, pos);
  }

  /**
   * Creates a type specification representing a type, with time zone,
   * nullability and base type name specified.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type
   * @param timeZone     Specified time zone
   * @param nullable     The nullability
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlTypeNameSpec typeNameSpec,
      SqlParserPos pos) {
    this(collectionsTypeName, typeNameSpec, null, null, pos);
  }

  /**
   * Creates a type specification that has no base type.
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlTypeNameSpec typeName,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    this(collectionsTypeName, typeName, typeName, timeZone, nullable, pos);
  }

  /**
   * Creates a type specification.
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlTypeNameSpec typeNameSpec,
      SqlTypeNameSpec baseTypeName,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    super(pos);
    this.collectionsTypeName = collectionsTypeName;
    this.typeNameSpec = typeNameSpec;
    this.baseTypeName = baseTypeName;
    this.timeZone = timeZone;
    this.nullable = nullable;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    return (collectionsTypeName != null)
        ? new SqlDataTypeSpec(collectionsTypeName, typeNameSpec, pos)
        : new SqlDataTypeSpec(typeNameSpec, timeZone, pos);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  public SqlIdentifier getCollectionsTypeName() {
    if (typeNameSpec instanceof SqlCollectionTypeNameSpec) {
      return typeNameSpec.getTypeName();
    }
    return null;
  }

  public SqlIdentifier getTypeName() {
    return typeNameSpec.getTypeName();
  }

  public SqlTypeNameSpec getTypeNameSpec() {
    return typeNameSpec;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public Boolean getNullable() {
    return nullable;
  }

  /** Returns a copy of this data type specification with a given
   * nullability. */
  public SqlDataTypeSpec withNullable(Boolean nullable) {
    if (Objects.equals(nullable, this.nullable)) {
      return this;
    }
    return new SqlDataTypeSpec(collectionsTypeName, typeNameSpec, timeZone,
        nullable, getParserPosition());
  }

  /**
   * Returns a new SqlDataTypeSpec corresponding to the component type if the
   * type spec is a collections type spec.<br>
   * Collection types are <code>ARRAY</code> and <code>MULTISET</code>.
   */
  public SqlDataTypeSpec getComponentTypeSpec() {
    assert getCollectionsTypeName() != null;
    return new SqlDataTypeSpec(
        typeNameSpec,
        timeZone,
        getParserPosition());
  }

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    typeNameSpec.unparse(writer, leftPrec, rightPrec);
    // collection type can have elements as builtin sql type and UDT.
    if (collectionsTypeName != null) {
      writer.keyword(collectionsTypeName.getSimple());
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDataType(this);
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlDataTypeSpec)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlDataTypeSpec that = (SqlDataTypeSpec) node;
    if (!Objects.equals(this.timeZone, that.timeZone)) {
      return litmus.fail("{} != {}", this, node);
    }
<<<<<<< HEAD
=======
    if (!SqlNode.equalDeep(
        this.collectionsTypeName,
        that.collectionsTypeName, litmus)) {
      return litmus.fail(null);
    }
    if (!this.typeNameSpec.equalsDeep(that.typeNameSpec, litmus)) {
      return litmus.fail(null);
    }
    return litmus.succeed();
  }

  /**
   * Throws an error if the type is not found.
   */
  public RelDataType deriveType(SqlValidator validator) {
    // validate collection type name first.
    if (null != collectionsTypeName) {
      final String collectionName = collectionsTypeName.getSimple();
      if (SqlTypeName.get(collectionName) == null) {
        throw validator.newValidationError(this,
            RESOURCE.unknownDatatypeName(collectionName));
      }
    }
    RelDataTypeFactory typeFactory = validator.getTypeFactory();
    RelDataType type = deriveType(typeFactory);
    if (type == null) {
      // the type is a UDT.
      type = validator.getValidatedNodeType(typeNameSpec.getTypeName());
      if (null != collectionsTypeName) {
        type = createCollectionType(type, typeFactory);
      }
    }
    return type;
  }

  /**
   * Does not throw an error if the type is not built-in.
   */
  public RelDataType deriveType(RelDataTypeFactory typeFactory) {
    return deriveType(typeFactory, false);
  }

  /**
   * Converts this type specification to a {@link RelDataType}.
   *
   * <p>Does not throw an error if the type is not built-in.
   *
   * @param nullable Whether the type is nullable if the type specification
   *                 does not explicitly state
   */
  public RelDataType deriveType(RelDataTypeFactory typeFactory,
      boolean nullable) {
    if (!typeNameSpec.getTypeName().isSimple()) {
      return null;
    }
    RelDataType type;
>>>>>>> f48aee584 ([CALCITE-2045] CREATE TYPE (Shuyi Chen))
    type = createTypeFromTypeNameSpec(typeFactory, typeNameSpec);
    if (type == null) {
      // This is definitely not a builtin data type, returns null.
      return null;
    }

    if (null != collectionsTypeName) {
      type = createCollectionType(type, typeFactory);
    }

  /**
   * Fix up the nullability of the {@code type}.
   *
   * @param typeFactory Type factory
   * @param type        The type to coerce nullability
   * @param nullable    Default nullability to use if this type specification does not
   *                    specify nullability
   * @return Type with specified nullability or the default(false)
   */
  private RelDataType fixUpNullability(RelDataTypeFactory typeFactory,
      RelDataType type, boolean nullable) {
    if (this.nullable != null) {
      nullable = this.nullable;
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  //~ Tools ------------------------------------------------------------------

  /**
   * Create collection data type.
   * @param elementType Type of the collection element.
   * @param typeFactory Type factory.
   * @return The collection data type, or throw exception if the collection
   *         type name does not belong to {@code SqlTypeName} enumerations.
   */
  private RelDataType createCollectionType(RelDataType elementType,
      RelDataTypeFactory typeFactory) {
    final String collectionName = collectionsTypeName.getSimple();
    final SqlTypeName collectionsSqlTypeName =
        Objects.requireNonNull(SqlTypeName.get(collectionName),
            collectionName);

    switch (collectionsSqlTypeName) {
    case MULTISET:
      return typeFactory.createMultisetType(elementType, -1);
    case ARRAY:
      return typeFactory.createArrayType(elementType, -1);

    default:
      throw Util.unexpected(collectionsSqlTypeName);
    }
  }

  /**
   * Create type from the type name specification directly.
   * @param typeFactory type factory.
   * @return the type.
   */
  private RelDataType createTypeFromTypeNameSpec(
      RelDataTypeFactory typeFactory,
      SqlTypeNameSpec typeNameSpec) {
    return typeNameSpec.deriveType(typeFactory);
  }
}
