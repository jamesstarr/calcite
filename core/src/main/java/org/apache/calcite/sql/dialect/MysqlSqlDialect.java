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
package org.apache.calcite.sql.dialect;

>>>>>>> ef578f572 ([CALCITE-2188] JDBC adapter generates invalid SQL for DATE/INTERVAL arithmetic)
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
<<<<<<< HEAD
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
=======
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;


  @Override public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {

    //  Unit Value         | Expected Format
    // --------------------+-------------------------------------------
    //  MICROSECOND        | MICROSECONDS
    //  SECOND             | SECONDS
    //  MINUTE             | MINUTES
    //  HOUR               | HOURS
    //  DAY                | DAYS
    //  WEEK               | WEEKS
    //  MONTH              | MONTHS
    //  QUARTER            | QUARTERS
    //  YEAR               | YEARS
    //  MINUTE_SECOND      | 'MINUTES:SECONDS'
    //  HOUR_MINUTE        | 'HOURS:MINUTES'
    //  DAY_HOUR           | 'DAYS HOURS'
    //  YEAR_MONTH         | 'YEARS-MONTHS'
    //  MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS'
    //  HOUR_MICROSECOND   | 'HOURS:MINUTES:SECONDS.MICROSECONDS'
    //  SECOND_MICROSECOND | 'SECONDS.MICROSECONDS'
    //  DAY_MINUTE         | 'DAYS HOURS:MINUTES'
    //  DAY_MICROSECOND    | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
    //  DAY_SECOND         | 'DAYS HOURS:MINUTES:SECONDS'
    //  HOUR_SECOND        | 'HOURS:MINUTES:SECONDS'

    if (!qualifier.useDefaultFractionalSecondPrecision()) {
      throw new AssertionError("Fractional second precision is not supported now ");
    }

    final String start = validate(qualifier.timeUnitRange.startUnit).name();
    if (qualifier.timeUnitRange.startUnit == TimeUnit.SECOND
        || qualifier.timeUnitRange.endUnit == null) {
      writer.keyword(start);
    } else {
      writer.keyword(start + "_" + qualifier.timeUnitRange.endUnit.name());
    }
  }

  private TimeUnit validate(TimeUnit timeUnit) {
    switch (timeUnit) {
    case MICROSECOND:
    case SECOND:
    case MINUTE:
    case HOUR:
    case DAY:
    case MONTH:
    case YEAR:
      return timeUnit;
    // Intervals cannot hold WEEK or QUARTERs. This can be the time unit
    // if a TimestampAdd call was transformed to Datetime_plus through the
    // TimestampAdd convertlet.
    case WEEK:
      return TimeUnit.DAY;
    case QUARTER:
      return TimeUnit.MONTH;
    default:
      throw new AssertionError(" Time unit " + timeUnit + "is not supported now.");
    }
  }
}


  @Override public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {

    //  Unit Value         | Expected Format
    // --------------------+-------------------------------------------
    //  MICROSECOND        | MICROSECONDS
    //  SECOND             | SECONDS
    //  MINUTE             | MINUTES
    //  HOUR               | HOURS
    //  DAY                | DAYS
    //  WEEK               | WEEKS
    //  MONTH              | MONTHS
    //  QUARTER            | QUARTERS
    //  YEAR               | YEARS
    //  MINUTE_SECOND      | 'MINUTES:SECONDS'
    //  HOUR_MINUTE        | 'HOURS:MINUTES'
    //  DAY_HOUR           | 'DAYS HOURS'
    //  YEAR_MONTH         | 'YEARS-MONTHS'
    //  MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS'
    //  HOUR_MICROSECOND   | 'HOURS:MINUTES:SECONDS.MICROSECONDS'
    //  SECOND_MICROSECOND | 'SECONDS.MICROSECONDS'
    //  DAY_MINUTE         | 'DAYS HOURS:MINUTES'
    //  DAY_MICROSECOND    | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
    //  DAY_SECOND         | 'DAYS HOURS:MINUTES:SECONDS'
    //  HOUR_SECOND        | 'HOURS:MINUTES:SECONDS'

    if (!qualifier.useDefaultFractionalSecondPrecision()) {
      throw new AssertionError("Fractional second precision is not supported now ");
    }

    final String start = validate(qualifier.timeUnitRange.startUnit).name();
    if (qualifier.timeUnitRange.startUnit == TimeUnit.SECOND
        || qualifier.timeUnitRange.endUnit == null) {
      writer.keyword(start);
    } else {
      writer.keyword(start + "_" + qualifier.timeUnitRange.endUnit.name());
    }
  }

  private TimeUnit validate(TimeUnit timeUnit) {
    switch (timeUnit) {
    case MICROSECOND:
    case SECOND:
    case MINUTE:
    case HOUR:
    case DAY:
    case MONTH:
    case YEAR:
      return timeUnit;
    // Intervals cannot hold WEEK or QUARTERs. This can be the time unit
    // if a TimestampAdd call was transformed to Datetime_plus through the
    // TimestampAdd convertlet.
    case WEEK:
      return TimeUnit.DAY;
    case QUARTER:
      return TimeUnit.MONTH;
    default:
      throw new AssertionError(" Time unit " + timeUnit + "is not supported now.");
    }
  }
}
