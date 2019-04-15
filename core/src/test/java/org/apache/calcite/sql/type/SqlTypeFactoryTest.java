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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Test for {@link SqlTypeFactoryImpl}.
 */
public class SqlTypeFactoryTest {

  @Test public void testLeastRestrictiveWithAny() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlBigInt, f.sqlAny));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ANY));
  }

  @Test public void testLeastRestrictiveWithNumbers() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlBigInt, f.sqlInt));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.BIGINT));
  }

  @Test public void testLeastRestrictiveTimestamps() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestamp3, f.sqlTimestamp6, f.sqlTimestamp9));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.TIMESTAMP));
    assertThat(leastRestrictive.getPrecision(), is(9));
    assertThat(leastRestrictive.isNullable(), is(false));
  }

  @Test public void testLeastRestrictiveTimestampsWithNullability() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestamp3, f.sqlTimestamp6, f.sqlNull));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.TIMESTAMP));
    assertThat(leastRestrictive.getPrecision(), is(6));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test public void testLeastRestrictiveTimestampsAndDatesWithNullability() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestamp3, f.sqlDate, f.sqlNull));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.TIMESTAMP));
    assertThat(leastRestrictive.getPrecision(), is(3));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test public void testLeastRestrictiveTimestampsAndIncompatible() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(
            Lists.newArrayList(f.sqlTimestamp3, f.sqlVarcharNullable));
    assertThat(leastRestrictive, is((RelDataType) null));
  }

  @Test public void testLeastRestrictiveWithNullability() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlVarcharNullable, f.sqlAny));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ANY));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2994">[CALCITE-2994]
   * Least restrictive type among structs does not consider nullability</a>. */
  @Test public void testLeastRestrictiveWithNullableStruct() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(ImmutableList.of(f.structOfIntNullable, f.structOfInt));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.ROW));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  @Test public void testLeastRestrictiveWithNull() {
    Fixture f = new Fixture();
    RelDataType leastRestrictive =
        f.typeFactory.leastRestrictive(Lists.newArrayList(f.sqlNull, f.sqlNull));
    assertThat(leastRestrictive.getSqlTypeName(), is(SqlTypeName.NULL));
    assertThat(leastRestrictive.isNullable(), is(true));
  }

  /** Unit test for {@link SqlTypeUtil#comparePrecision(int, int)}
   * and  {@link SqlTypeUtil#maxPrecision(int, int)}. */
  @Test public void testMaxPrecision() {
    final int un = RelDataType.PRECISION_NOT_SPECIFIED;
    checkPrecision(1, 1, 1, 0);
    checkPrecision(2, 1, 2, 1);
    checkPrecision(2, 100, 100, -1);
    checkPrecision(2, un, un, -1);
    checkPrecision(un, 2, un, 1);
    checkPrecision(un, un, un, 0);
  }

  /** Unit test for {@link ArraySqlType#getPrecedenceList()}. */
  @Test public void testArrayPrecedenceList() {
    Fixture f = new Fixture();
    assertThat(checkPrecendenceList(f.arrayBigInt, f.arrayBigInt, f.arrayFloat),
        is(3));
    assertThat(
        checkPrecendenceList(f.arrayOfArrayBigInt, f.arrayOfArrayBigInt,
            f.arrayOfArrayFloat), is(3));
    assertThat(checkPrecendenceList(f.sqlBigInt, f.sqlBigInt, f.sqlFloat),
        is(3));
    assertThat(
        checkPrecendenceList(f.multisetBigInt, f.multisetBigInt,
            f.multisetFloat), is(3));
    assertThat(
        checkPrecendenceList(f.arrayBigInt, f.arrayBigInt,
            f.arrayBigIntNullable), is(0));
    try {
      int i = checkPrecendenceList(f.arrayBigInt, f.sqlBigInt, f.sqlInt);
      fail("Expected assert, got " + i);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("must contain type: BIGINT"));
    }
  }

  private int checkPrecendenceList(RelDataType t, RelDataType type1, RelDataType type2) {
    return t.getPrecedenceList().compareTypePrecedence(type1, type2);
  }

  private void checkPrecision(int p0, int p1, int expectedMax,
      int expectedComparison) {
    assertThat(SqlTypeUtil.maxPrecision(p0, p1), is(expectedMax));
    assertThat(SqlTypeUtil.maxPrecision(p1, p0), is(expectedMax));
    assertThat(SqlTypeUtil.maxPrecision(p0, p0), is(p0));
    assertThat(SqlTypeUtil.maxPrecision(p1, p1), is(p1));
    assertThat(SqlTypeUtil.comparePrecision(p0, p1), is(expectedComparison));
    assertThat(SqlTypeUtil.comparePrecision(p0, p0), is(0));
    assertThat(SqlTypeUtil.comparePrecision(p1, p1), is(0));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2464">[CALCITE-2464]
   * Allow to set nullability for columns of structured types</a>. */
  @Test
  public void createStructTypeWithNullability() {
    Fixture f = new Fixture();
    RelDataTypeFactory typeFactory = f.typeFactory;
    List<RelDataTypeField> fields = new ArrayList<>();
    RelDataTypeField field0 = new RelDataTypeFieldImpl(
            "i", 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 = new RelDataTypeFieldImpl(
            "s", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR));
    fields.add(field0);
    fields.add(field1);
    final RelDataType recordType = new RelRecordType(fields); // nullable false by default
    final RelDataType copyRecordType = typeFactory.createTypeWithNullability(recordType, true);
    assertFalse(recordType.isNullable());
    assertTrue(copyRecordType.isNullable());
  }

}

// End SqlTypeFactoryTest.java
