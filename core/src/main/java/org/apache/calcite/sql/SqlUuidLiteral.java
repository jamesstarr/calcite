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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * A SQL literal representing a UUID value, for example <code>UUID
 * '123e4567-e89b-12d3-a456-426655440000'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createUuid}.
 *
 * <p>This class stores UUID values as strings to avoid the signed comparison
 * issue in {@link java.util.UUID#compareTo(UUID)} which does not comply with
 * RFC 4122.
 */
public class SqlUuidLiteral extends SqlLiteral {
  //~ Constructors -----------------------------------------------------------

  public SqlUuidLiteral(String uuidStr, SqlParserPos pos) {
    super(validateAndNormalize(uuidStr), SqlTypeName.UUID, pos);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlUuidLiteral clone(SqlParserPos pos) {
    return new SqlUuidLiteral(getUuidString(), pos);
  }

  public String getUuidString() {
    return (String) requireNonNull(value, "value");
  }

  @Override public String toString() {
    return "UUID '" + toFormattedString() + "'";
  }

  public String toFormattedString() {
    return getUuidString();
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal(this.toString());
  }
>>>>>>> c25d56fe6 (DX-108641: Fix UUID literal comparisons to be compliant with RFC 4122 (#184))

  private static String validateAndNormalize(String uuidStr) {
    if (uuidStr == null) {
      throw new IllegalArgumentException("UUID string cannot be null");
    }
    // Validate by parsing and then return the canonical string representation
    UUID uuid = UUID.fromString(uuidStr);
    return uuid.toString();
  }
<<<<<<< HEAD
=======
=======
>>>>>>> c25d56fe6 (DX-108641: Fix UUID literal comparisons to be compliant with RFC 4122 (#184))
}
