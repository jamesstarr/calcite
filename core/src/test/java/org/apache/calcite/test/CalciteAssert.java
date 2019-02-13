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
package org.apache.calcite.test;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.clone.CloneSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Lists;

import net.hydromatic.foodmart.data.hsqldb.FoodmartHsqldb;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import org.apiguardian.api.API;
import org.hamcrest.Matcher;

      } catch (Error | RuntimeException e) {
        // It is better to have AssertionError
        // at the very top level of the exception stack.
        throw e;
=======
      } catch (Throwable e) {
        throw TestUtil.rethrow(e);
      }
    }

    public AssertMetaData returns(String expected) {
      return returns(checkResult(expected));
    }
  }

  /** Connection configuration. Basically, a set of schemas that should be
   * instantiated in the connection. */
  public enum Config {
    /** Configuration that creates an empty connection. */
    EMPTY,

    /**
     * Configuration that creates a connection with two in-memory data sets:
     * {@link org.apache.calcite.test.JdbcTest.HrSchema} and
     * {@link org.apache.calcite.test.JdbcTest.FoodmartSchema}.
     */
    REGULAR,

    /**
     * Configuration that creates a connection with an in-memory data set
     * similar to the smoke test in Cascading Lingual.
     */
    LINGUAL,

    /**
     * Configuration that creates a connection to a MySQL server. Tables
     * such as "customer" and "sales_fact_1997" are available. Queries
     * are processed by generating Java that calls linq4j operators
     * such as
     * {@link org.apache.calcite.linq4j.Enumerable#where(org.apache.calcite.linq4j.function.Predicate1)}.
     */
    JDBC_FOODMART,

    /**
     * Configuration that creates a connection to hsqldb containing the
     * Scott schema via the JDBC adapter.
     */
    JDBC_SCOTT,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database. */
    FOODMART_CLONE,

    /** Configuration that contains geo-spatial functions. */
    GEO,

    /** Configuration that contains an in-memory clone of the FoodMart
     * database, plus a lattice to enable on-the-fly materializations. */
    JDBC_FOODMART_WITH_LATTICE,

    /** Configuration that includes the metadata schema. */
    REGULAR_PLUS_METADATA,

    /** Configuration that loads the "scott/tiger" database. */
    SCOTT,

    /** Configuration that loads Spark. */
    SPARK,

    /** Configuration that loads AUX schema for tests involving view expansions
     * and lateral joins tests. */
    AUX
  }

  /** Implementation of {@link AssertQuery} that does nothing. */
  private static class NopAssertQuery extends AssertQuery {
    private NopAssertQuery(String sql) {
      super(null, sql);
    }

    /** Returns an implementation of {@link AssertQuery} that does nothing. */
    static AssertQuery of(final String sql) {
      return new NopAssertQuery(sql);
    }

    @Override protected Connection createConnection() {
      throw new AssertionError("disabled");
    }

    @Override public AssertQuery returns(String sql,
        Consumer<ResultSet> checker) {
      return this;
    }

    @Override public AssertQuery throws_(String message) {
      return this;
    }

    @Override public AssertQuery runs() {
      return this;
    }

    @Override public AssertQuery convertMatches(
        Function<RelNode, Void> checker) {
      return this;
    }

    @Override public AssertQuery substitutionMatches(
        Function<RelNode, Void> checker) {
      return this;
    }

    @Override public AssertQuery planContains(String expected) {
      return this;
    }

    @Override public AssertQuery planHasSql(String expected) {
      return this;
    }

    @Override public AssertQuery planUpdateHasSql(String expected, int count) {
      return this;
    }

    @Override public AssertQuery queryContains(Consumer<List> predicate1) {

      return this;
    }
  }

  /** Information necessary to create a JDBC connection. Specify one to run
   * tests against a different database. (hsqldb is the default.) */
  public enum DatabaseInstance {
    HSQLDB(
        new ConnectionSpec(FoodmartHsqldb.URI, "FOODMART", "FOODMART",
            "org.hsqldb.jdbcDriver", "foodmart"),
        new ConnectionSpec(ScottHsqldb.URI, ScottHsqldb.USER,
            ScottHsqldb.PASSWORD, "org.hsqldb.jdbcDriver", "SCOTT")),
    H2(
        new ConnectionSpec("jdbc:h2:" + CalciteSystemProperty.TEST_DATASET_PATH.value()
            + "/h2/target/foodmart;user=foodmart;password=foodmart",
            "foodmart", "foodmart", "org.h2.Driver", "foodmart"), null),
    MYSQL(
        new ConnectionSpec("jdbc:mysql://localhost/foodmart", "foodmart",
            "foodmart", "com.mysql.jdbc.Driver", "foodmart"), null),
    ORACLE(
        new ConnectionSpec("jdbc:oracle:thin:@localhost:1521:XE", "foodmart",
            "foodmart", "oracle.jdbc.OracleDriver", "FOODMART"), null),
    POSTGRESQL(
        new ConnectionSpec(
            "jdbc:postgresql://localhost/foodmart?user=foodmart&password=foodmart&searchpath=foodmart",
            "foodmart", "foodmart", "org.postgresql.Driver", "foodmart"), null);

    public final ConnectionSpec foodmart;
    public final ConnectionSpec scott;

    DatabaseInstance(ConnectionSpec foodmart, ConnectionSpec scott) {
      this.foodmart = foodmart;
      this.scott = scott;
    }
  }

  /** Specification for common test schemas. */
  public enum SchemaSpec {
=======
    REFLECTIVE_FOODMART("foodmart"),
    JDBC_FOODMART("foodmart"),
    CLONE_FOODMART("foodmart2"),
    JDBC_FOODMART_WITH_LATTICE("lattice"),
    GEO("GEO"),
    HR("hr"),
    JDBC_SCOTT("JDBC_SCOTT"),
    SCOTT("scott"),
    BLANK("BLANK"),
    LINGUAL("SALES"),
    POST("POST"),
    ORINOCO("ORINOCO"),
    AUX("AUX"),
    BOOKSTORE("bookstore");

    /** The name of the schema that is usually created from this specification.
     * (Names are not unique, and you can use another name if you wish.) */
    public final String schemaName;

    SchemaSpec(String schemaName) {
      this.schemaName = schemaName;
    }
  }

  /** Converts a {@link ResultSet} to string. */
  static class ResultSetFormatter {
    final StringBuilder buf = new StringBuilder();

    public ResultSetFormatter resultSet(ResultSet resultSet)
        throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        buf.append("\n");
      }
      return this;
    }

    /** Converts one row to a string. */
    ResultSetFormatter rowToString(ResultSet resultSet,
        ResultSetMetaData metaData) throws SQLException {
      int n = metaData.getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(metaData.getColumnLabel(i))
              .append("=")
              .append(adjustValue(resultSet.getString(i)));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
      return this;
    }

    protected String adjustValue(String string) {
      return string;
    }

    public Collection<String> toStringList(ResultSet resultSet,
        Collection<String> list) throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        list.add(buf.toString());
        buf.setLength(0);
      }
      return list;
    }

    /** Flushes the buffer and returns its previous contents. */
    public String string() {
      String s = buf.toString();
      buf.setLength(0);
      return s;
    }
  }

  /** Builds a {@link java.util.Properties} containing connection property
   * settings. */
  static class PropBuilder {
    final Properties properties = new Properties();

    PropBuilder set(CalciteConnectionProperty p, String v) {
      properties.setProperty(p.camelName(), v);
      return this;
    }

    Properties build() {
      return properties;
    }
  }
<<<<<<< HEAD
=======

  /**
   * We want a consumer which can throw SqlException
   */
  public interface PreparedStatementConsumer {
    void accept(PreparedStatement statement) throws SQLException;
  }
}

  /**
   * We want a consumer which can throw SqlException
   */
  public interface PreparedStatementConsumer {
    void accept(PreparedStatement statement) throws SQLException;
  }

  /** An expected string that may contain either Java or a SQL string embedded
   * in the Java. */
  private static class JavaSql {
    private static final String START =
        ".unwrap(javax.sql.DataSource.class), \"";
    private static final String END = "\"";

    private final String java;
    private final String sql;

    JavaSql(String java, String sql) {
      this.java = Objects.requireNonNull(java);
      this.sql = sql;
    }

    static JavaSql fromJava(String java) {
      return new JavaSql(java, null);
    }

    static JavaSql fromSql(String sql) {
      return new JavaSql(wrap(sql), sql);
    }

    private static String wrap(String sql) {
      return START
          + sql.replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replace("\n", "\\\\n")
          + END;
    }

    /** Extracts the SQL statement(s) from within a Java plan. */
    public List<String> extractSql() {
      return unwrap(java);
    }

    static @Nonnull List<String> unwrap(String java) {
      final List<String> sqlList = new ArrayList<>();
      final StringBuilder b = new StringBuilder();
      hLoop:
      for (int h = 0;;) {
        final int i = java.indexOf(START, h);
        if (i < 0) {
          return sqlList;
        }
        for (int j = i + START.length(); j < java.length();) {
          char c = java.charAt(j++);
          switch (c) {
          case '"':
            sqlList.add(b.toString());
            b.setLength(0);
            h = j;
            continue hLoop;
          case '\\':
            c = java.charAt(j++);
            if (c == 'n') {
              b.append('\n');
              break;
            }
            if (c == 'r') {
              // Ignore CR, thus converting Windows strings to Unix.
              break;
            }
            // fall through for '\\' and '\"'
          default:
            b.append(c);
          }
        }
        return sqlList; // last SQL literal was incomplete
      }
    }
  }
}
