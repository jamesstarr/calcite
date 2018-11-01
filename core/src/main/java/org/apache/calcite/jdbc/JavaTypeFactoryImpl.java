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
package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.runtime.Unit;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

      // We do not need to change the nullability of the nested fields,
      // since it can be overridden by the existing implementation of createTypeWithNullability
      // when we treat the nullability of the root struct type.
      sqlType = typeFactory.createStructType(
              Lists.transform(type.getFieldList(),
                field -> toSql(typeFactory, field.getType(), false)),
              type.getFieldNames());
    } else if (type instanceof JavaType) {
      sqlType = typeFactory.createSqlType(type.getSqlTypeName());
    }
    return mustSetNullability
            ? typeFactory.createTypeWithNullability(sqlType, type.isNullable())
            : sqlType;
  }

  private static RelDataType toSqlTypeWithNullToAny(
      final RelDataTypeFactory typeFactory, RelDataType type) {
    if (type == null) {
      return typeFactory.createSqlType(SqlTypeName.ANY);
    }
    return toSql(typeFactory, type);
  }

  public Type createSyntheticType(List<Type> types) {
    if (types.isEmpty()) {
      // Unit is a pre-defined synthetic type to be used when there are 0
      // fields. Because all instances are the same, we use a singleton.
      return Unit.class;
    }
    final String name =
        "Record" + types.size() + "_" + syntheticTypes.size();
    final SyntheticRecordType syntheticType =
        new SyntheticRecordType(null, name);
    for (final Ord<Type> ord : Ord.zip(types)) {
      syntheticType.fields.add(
          new RecordFieldImpl(
              syntheticType,
              "f" + ord.i,
              ord.e,
              !Primitive.is(ord.e),
              Modifier.PUBLIC));
    }
    return register(syntheticType);
  }

  private SyntheticRecordType register(
      final SyntheticRecordType syntheticType) {
    final List<Pair<Type, Boolean>> key =
        new AbstractList<Pair<Type, Boolean>>() {
          public Pair<Type, Boolean> get(int index) {
            final Types.RecordField field =
                syntheticType.getRecordFields().get(index);
            return Pair.of(field.getType(), field.nullable());
          }

          public int size() {
            return syntheticType.getRecordFields().size();
          }
        };
    SyntheticRecordType syntheticType2 = syntheticTypes.get(key);
    if (syntheticType2 == null) {
      syntheticTypes.put(key, syntheticType);
      return syntheticType;
    } else {
      return syntheticType2;
    }
  }

  /** Creates a synthetic Java class whose fields have the same names and
   * relational types. */
  private Type createSyntheticType(RelRecordType type) {
    final String name =
        "Record" + type.getFieldCount() + "_" + syntheticTypes.size();
    final SyntheticRecordType syntheticType =
        new SyntheticRecordType(type, name);
    for (final RelDataTypeField recordField : type.getFieldList()) {
      final Type javaClass = getJavaClass(recordField.getType());
      syntheticType.fields.add(
          new RecordFieldImpl(
              syntheticType,
              recordField.getName(),
              javaClass,
              recordField.getType().isNullable()
                  && !Primitive.is(javaClass),
              Modifier.PUBLIC));
    }
    return register(syntheticType);
  }

  /** Synthetic record type. */
  public static class SyntheticRecordType implements Types.RecordType {
    final List<Types.RecordField> fields = new ArrayList<>();
    final RelDataType relType;
    private final String name;

    private SyntheticRecordType(RelDataType relType, String name) {
      this.relType = relType;
      this.name = name;
      assert relType == null
             || Util.isDistinct(relType.getFieldNames())
          : "field names not distinct: " + relType;
    }

    public String getName() {
      return name;
    }

    public List<Types.RecordField> getRecordFields() {
      return fields;
    }

    public String toString() {
      return name;
    }
  }

  /** Implementation of a field. */
  private static class RecordFieldImpl implements Types.RecordField {
    private final SyntheticRecordType syntheticType;
    private final String name;
    private final Type type;
    private final boolean nullable;
    private final int modifiers;

    RecordFieldImpl(
        SyntheticRecordType syntheticType,
        String name,
        Type type,
        boolean nullable,
        int modifiers) {
      this.syntheticType = Objects.requireNonNull(syntheticType);
      this.name = Objects.requireNonNull(name);
      this.type = Objects.requireNonNull(type);
      this.nullable = nullable;
      this.modifiers = modifiers;
      assert !(nullable && Primitive.is(type))
          : "type [" + type + "] can never be null";
    }

    public Type getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public int getModifiers() {
      return modifiers;
    }

    public boolean nullable() {
      return nullable;
    }

    public Object get(Object o) {
      throw new UnsupportedOperationException();
    }

    public Type getDeclaringClass() {
      return syntheticType;
    }
  }
}
