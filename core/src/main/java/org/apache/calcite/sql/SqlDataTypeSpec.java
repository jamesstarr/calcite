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
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Objects;
import java.util.TimeZone;

/**
 * Represents a SQL data type specification in a parse tree.
 *
 * <p>A <code>SqlDataTypeSpec</code> is immutable; once created, you cannot
 * change any of the fields.</p>
 *
 * <p>We support the following data type expressions:
 *
 * <p>we support complex type expressions
 * like:</p>
 *
 * <blockquote><code>ROW(<br>
 *   foo NUMBER(5, 2) NOT NULL,<br>
 *   rec ROW(b BOOLEAN, i MyUDT NOT NULL))</code></blockquote>
 *
 * <p>Internally we use {@link SqlTypeNameSpec} to specify such complex data types.
 *
 * <p>We support simple data types like CHAR, VARCHAR and DOUBLE,
 * with optional precision and scale.</p>
 *
 * <p>Internally we use {@link SqlIdentifier} to specify simple data types.
 */
public class SqlDataTypeSpec extends SqlNode {
  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier collectionsTypeName;
  private final SqlIdentifier typeName;
  private final SqlIdentifier baseTypeName;
  private final int scale;
  private final int precision;
  private final String charSetName;
  private final TimeZone timeZone;

  /** Whether data type allows nulls.
   *
   * <p>Nullable is nullable! Null means "not specified". E.g.
   * {@code CAST(x AS INTEGER)} preserves the same nullability as {@code x}.
   */
  private Boolean nullable;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a type specification representing a type.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type
   */
  public SqlDataTypeSpec(
      final SqlTypeNameSpec typeNameSpec,
      SqlParserPos pos) {
    this(typeNameSpec, null, null, pos);
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
      SqlIdentifier typeName,
      int precision,
      int scale,
      String charSetName,
      SqlParserPos pos) {
    this(collectionsTypeName, typeName, precision, scale, charSetName, null,
        null, pos);
  }

  /**
   * Creates a type specification that has no base type.
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlIdentifier typeName,
      int precision,
      int scale,
      String charSetName,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    this(collectionsTypeName, typeName, typeName, precision, scale, charSetName,
        timeZone, nullable, pos);
  }

  /**
   * Creates a type specification.
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlIdentifier typeName,
      SqlIdentifier baseTypeName,
      int precision,
      int scale,
      String charSetName,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    super(pos);
    this.collectionsTypeName = collectionsTypeName;
    this.typeName = typeName;
    this.baseTypeName = baseTypeName;
    this.precision = precision;
    this.scale = scale;
    this.charSetName = charSetName;
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

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    String name = typeName.getSimple();
    if (typeName instanceof SqlTypeNameSpec) {
      typeName.unparse(writer, leftPrec, rightPrec);
      if (collectionsTypeName != null) {
        writer.keyword(collectionsTypeName.getSimple());
      }
    } else if (SqlTypeName.get(name) != null) {
      SqlTypeName sqlTypeName = SqlTypeName.get(name);

      // we have a built-in data type
      writer.keyword(name);

      if (sqlTypeName.allowsPrec() && (precision >= 0)) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
        writer.print(precision);
        if (sqlTypeName.allowsScale() && (scale >= 0)) {
          writer.sep(",", true);
          writer.print(scale);
        }
        writer.endList(frame);
      }

      if (charSetName != null) {
        writer.keyword("CHARACTER SET");
        writer.identifier(charSetName);
      }

      if (collectionsTypeName != null) {
        writer.keyword(collectionsTypeName.getSimple());
      }
    } else if (name.startsWith("_")) {
      // We're generating a type for an alien system. For example,
      // UNSIGNED is a built-in type in MySQL.
      // (Need a more elegant way than '_' of flagging this.)
      writer.keyword(name.substring(1));
    } else {
      // else we have a user defined type
      typeName.unparse(writer, leftPrec, rightPrec);
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
    if (!this.typeNameSpec.equalsDeep(that.typeNameSpec, litmus)) {
      return litmus.fail(null);
    }
    return litmus.succeed();
  }

  /**
   * Throws an error if the type is not found.
   */
  public RelDataType deriveType(SqlValidator validator) {
    RelDataType type = null;
    if (typeName.isSimple()) {
      if (null != collectionsTypeName) {
        final String collectionName = collectionsTypeName.getSimple();
        if (SqlTypeName.get(collectionName) == null) {
          throw validator.newValidationError(this,
              RESOURCE.unknownDatatypeName(collectionName));
        }
      }

      RelDataTypeFactory typeFactory = validator.getTypeFactory();
      type = deriveType(typeFactory);
    }
    if (type == null) {
      type = validator.getValidatedNodeType(typeName);
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
   * <p>Throws an error if the type is not found.
   *
   * @param nullable Whether the type is nullable if the type specification
   *                 does not explicitly state
   */
  public RelDataType deriveType(RelDataTypeFactory typeFactory,
      boolean nullable) {
    if (!typeName.isSimple()) {
      return null;
    }
    RelDataType type;
    if (typeName instanceof SqlTypeNameSpec) {
      // Create type directly if this typeName is a SqlTypeNameSpec.
      type = createTypeFromTypeNameSpec(typeFactory, (SqlTypeNameSpec) typeName);
      if (type == null) {
        return null;
      }
    } else {
      final String name = typeName.getSimple();
      final SqlTypeName sqlTypeName = SqlTypeName.get(name);
      if (sqlTypeName == null) {
        return null;
      }

      // NOTE jvs 15-Jan-2009:  earlier validation is supposed to
      // have caught these, which is why it's OK for them
      // to be assertions rather than user-level exceptions.
      if ((precision >= 0) && (scale >= 0)) {
        assert sqlTypeName.allowsPrecScale(true, true);
        type = typeFactory.createSqlType(sqlTypeName, precision, scale);
      } else if (precision >= 0) {
        assert sqlTypeName.allowsPrecNoScale();
        type = typeFactory.createSqlType(sqlTypeName, precision);
      } else {
        assert sqlTypeName.allowsNoPrecNoScale();
        type = typeFactory.createSqlType(sqlTypeName);
      }
    }

    // Fix-up the nullability, default is false.
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    type = fixUpNullability(typeFactory, type, nullable);
    return type;
  }

      Charset charset;
      if (null == charSetName) {
        charset = typeFactory.getDefaultCharset();
      } else {
        String javaCharSetName =
            Preconditions.checkNotNull(
                SqlUtil.translateCharacterSetName(charSetName), charSetName);
        charset = Charset.forName(javaCharSetName);
      }
      type =
          typeFactory.createTypeWithCharsetAndCollation(
              type,
              charset,
              collation);
    }

    if (null != collectionsTypeName) {
      final String collectionName = collectionsTypeName.getSimple();
      final SqlTypeName collectionsSqlTypeName =
          Preconditions.checkNotNull(SqlTypeName.get(collectionName),
              collectionName);

      switch (collectionsSqlTypeName) {
      case MULTISET:
        type = typeFactory.createMultisetType(type, -1);
        break;

      case ARRAY:
        type = typeFactory.createArrayType(type, -1);
        break;

      default:
        throw Util.unexpected(collectionsSqlTypeName);
      }
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
