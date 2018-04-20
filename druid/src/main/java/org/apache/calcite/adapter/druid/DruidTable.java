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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Table mapped onto a Druid table.
 */
public class DruidTable extends AbstractTable implements TranslatableTable {

  public static final String DEFAULT_TIMESTAMP_COLUMN = "__time";
  public static final Interval DEFAULT_INTERVAL =
      new Interval(new DateTime("1900-01-01", ISOChronology.getInstanceUTC()),
          new DateTime("3000-01-01", ISOChronology.getInstanceUTC()));

  final DruidSchema schema;
  final String dataSource;
  final RelProtoDataType protoRowType;
  final ImmutableSet<String> metricFieldNames;
  final ImmutableList<Interval> intervals;
  final String timestampFieldName;

  /**
   * Creates a Druid table.
   *
   * @param schema Druid schema that contains this table
   * @param dataSource Druid data source name
   * @param protoRowType Field names and types
   * @param metricFieldNames Names of fields that are metrics
   * @param intervals Default interval if query does not constrain the time, or null
   * @param timestampFieldName Name of the column that contains the time
   */
  public DruidTable(DruidSchema schema, String dataSource,
      RelProtoDataType protoRowType, Set<String> metricFieldNames,
      String timestampFieldName, List<Interval> intervals) {
    this.timestampFieldName = Preconditions.checkNotNull(timestampFieldName);
    this.schema = Preconditions.checkNotNull(schema);
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.protoRowType = protoRowType;
    this.metricFieldNames = ImmutableSet.copyOf(metricFieldNames);
    this.intervals = intervals != null ? ImmutableList.copyOf(intervals)
        : ImmutableList.of(DEFAULT_INTERVAL);
  }

  /** Creates a {@link DruidTable} by using the given {@link DruidConnectionImpl}
   * to populate the other parameters. The parameters may be partially populated.
   *
   * @param druidSchema Druid schema
   * @param dataSourceName Data source name in Druid, also table name
   * @param intervals Intervals, or null to use default
   * @param fieldMap Partially populated map of fields (dimensions plus metrics)
   * @param metricNameSet Partially populated set of metric names
   * @param timestampColumnName Name of timestamp column, or null
   * @param connection Connection used to find column definitions; Must be non-null
   * @param complexMetrics List of complex metrics in Druid (thetaSketch, hyperUnique)
   *
   * @return A table
   */
  static Table create(DruidSchema druidSchema, String dataSourceName,
      List<Interval> intervals, Map<String, SqlTypeName> fieldMap,
      Set<String> metricNameSet, String timestampColumnName,
      DruidConnectionImpl connection) {
    assert connection != null;

    connection.metadata(dataSourceName, timestampColumnName, intervals,
            fieldMap, metricNameSet);

    return DruidTable.create(druidSchema, dataSourceName, intervals, fieldMap,
            metricNameSet, timestampColumnName);
  }

  /** Creates a {@link DruidTable} by copying the given parameters.
   *
   * @param druidSchema Druid schema
   * @param dataSourceName Data source name in Druid, also table name
   * @param intervals Intervals, or null to use default
   * @param fieldMap Fully populated map of fields (dimensions plus metrics)
   * @param metricNameSet Fully populated set of metric names
   * @param timestampColumnName Name of timestamp column, or null
   *
   * @return A table
   */
  static Table create(DruidSchema druidSchema, String dataSourceName,
                      List<Interval> intervals, Map<String, SqlTypeName> fieldMap,
                      Set<String> metricNameSet, String timestampColumnName) {
    final ImmutableMap<String, SqlTypeName> fields =
            ImmutableMap.copyOf(fieldMap);
    return new DruidTable(druidSchema,
        dataSourceName,
        new MapRelProtoDataType(fields, timestampColumnName),
        ImmutableSet.copyOf(metricNameSet),
        timestampColumnName,
        intervals);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType rowType = protoRowType.apply(typeFactory);
    final List<String> fieldNames = rowType.getFieldNames();
    Preconditions.checkArgument(fieldNames.contains(timestampFieldName));
    Preconditions.checkArgument(fieldNames.containsAll(metricFieldNames));
    return rowType;
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    final TableScan scan = LogicalTableScan.create(cluster, relOptTable);
    return DruidQuery.create(cluster,
        cluster.traitSetOf(BindableConvention.INSTANCE), relOptTable, this,
        ImmutableList.<RelNode>of(scan));
  }

  public boolean isMetric(String name) {
    return metricFieldNames.contains(name);
  }

  /** Creates a {@link RelDataType} from a map of
   * field names and types. */
  private static class MapRelProtoDataType implements RelProtoDataType {
    private final ImmutableMap<String, SqlTypeName> fields;
    private final String timestampColumn;

    MapRelProtoDataType(ImmutableMap<String, SqlTypeName> fields) {
      this.fields = fields;
      this.timestampColumn = DruidTable.DEFAULT_TIMESTAMP_COLUMN;
    }

    MapRelProtoDataType(ImmutableMap<String, SqlTypeName> fields, String timestampColumn) {
      this.fields = fields;
      this.timestampColumn = timestampColumn;
    }

    @Override public RelDataType apply(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (Map.Entry<String, SqlTypeName> field : fields.entrySet()) {
        final String key = field.getKey();
        builder.add(key, field.getValue())
            // Druid's time column is always not null and the only column called __time.
            .nullable(!timestampColumn.equals(key));
      }
      return builder.build();
    }
  }
}

// End DruidTable.java
