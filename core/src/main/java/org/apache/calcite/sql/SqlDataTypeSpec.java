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
   * Creates a type specification representing a type, with time zone
   * and nullability specified.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type.
   * @param timeZone     Specified time zone.
   * @param nullable     The nullability.
   */
  public SqlDataTypeSpec(
      SqlTypeNameSpec typeNameSpec,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    this(typeNameSpec, typeNameSpec, timeZone, nullable, pos);
  }

  /**
   * Creates a type specification representing a type, with time zone,
   * nullability and base type name specified.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type.
   * @param baseTypeName The base type name.
   * @param timeZone     Specified time zone.
   * @param nullable     The nullability.
   */
  public SqlDataTypeSpec(
      SqlTypeNameSpec typeNameSpec,
      SqlTypeNameSpec baseTypeName,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    super(pos);
    this.typeNameSpec = typeNameSpec;
    this.baseTypeName = baseTypeName;
    this.timeZone = timeZone;
    this.nullable = nullable;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    return new SqlDataTypeSpec(typeNameSpec, timeZone, pos);
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
    return new SqlDataTypeSpec(typeNameSpec, timeZone, nullable, getParserPosition());
  }

  /**
   * Returns a new SqlDataTypeSpec corresponding to the component type if the
   * type spec is a collections type spec.<br>
   * Collection types are <code>ARRAY</code> and <code>MULTISET</code>.
   */
  public SqlDataTypeSpec getComponentTypeSpec() {
    assert typeNameSpec instanceof SqlCollectionTypeNameSpec;
    SqlTypeNameSpec elementTypeName =
        ((SqlCollectionTypeNameSpec) typeNameSpec).getElementTypeName();
    return new SqlDataTypeSpec(elementTypeName, timeZone, getParserPosition());
  }

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    typeNameSpec.unparse(writer, leftPrec, rightPrec);
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
    if (!SqlNode.equalDeep(
        this.collectionsTypeName,
        that.collectionsTypeName, litmus)) {
      return litmus.fail(null);
    }
=======
    if (!this.typeNameSpec.equalsDeep(that.typeNameSpec, litmus)) {
      return litmus.fail(null);
    }
    return litmus.succeed();
  }

  /**
   * Converts this type specification to a {@link RelDataType}.
   *
   * <p>Throws an error if the type is not found.
   */
  public RelDataType deriveType(SqlValidator validator) {
    return deriveType(validator, false);
  }

  /**
   * Converts this type specification to a {@link RelDataType}.
   *
   * <p>Throws an error if the type is not found.
   *
   * @param nullable Whether the type is nullable if the type specification
   *                 does not explicitly state.
   */
  public RelDataType deriveType(SqlValidator validator, boolean nullable) {
    RelDataType type;
    type = typeNameSpec.deriveType(validator);

    // Fix-up the nullability, default is false.
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    type = fixUpNullability(typeFactory, type, nullable);
    return type;
  }

  //~ Tools ------------------------------------------------------------------

  /**
   * Fix up the nullability of the {@code type}.
   * @param typeFactory Type factory.
   * @param type        The type to coerce nullability.
   * @param nullable    Default nullability to use if this type specification does not
   *                    specify nullability.
   * @return type with specified nullability or the default.
   */
  private RelDataType fixUpNullability(RelDataTypeFactory typeFactory,
      RelDataType type, boolean nullable) {
    if (this.nullable != null) {
      nullable = this.nullable;
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }
}
