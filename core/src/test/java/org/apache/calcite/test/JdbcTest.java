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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.HandlerImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.sql.DataSource;

/**
 * Tests for using Calcite via JDBC.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class JdbcTest {

  public static final String FOODMART_SCHEMA = "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'foodmart',\n"
      + "       jdbcDriver: " + q(CalciteAssert.DB.foodmart.driver) + ",\n"
      + "       jdbcUser: " + q(CalciteAssert.DB.foodmart.username) + ",\n"
      + "       jdbcPassword: " + q(CalciteAssert.DB.foodmart.password) + ",\n"
      + "       jdbcUrl: " + q(CalciteAssert.DB.foodmart.url) + ",\n"
      + "       jdbcCatalog: " + q(CalciteAssert.DB.foodmart.catalog) + ",\n"
      + "       jdbcSchema: " + q(CalciteAssert.DB.foodmart.schema) + "\n"
      + "     }\n";

  public static final String FOODMART_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  public static final ConnectionSpec SCOTT =
      Util.first(CalciteAssert.DB.scott,
          CalciteAssert.DatabaseInstance.HSQLDB.scott);

  public static final String SCOTT_SCHEMA = "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'SCOTT',\n"
      + "       jdbcDriver: " + q(SCOTT.driver) + ",\n"
      + "       jdbcUser: " + q(SCOTT.username) + ",\n"
      + "       jdbcPassword: " + q(SCOTT.password) + ",\n"
      + "       jdbcUrl: " + q(SCOTT.url) + ",\n"
      + "       jdbcCatalog: " + q(SCOTT.catalog) + ",\n"
      + "       jdbcSchema: " + q(SCOTT.schema) + "\n"
      + "     }\n";

  public static final String SCOTT_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'SCOTT',\n"
      + "   schemas: [\n"
      + SCOTT_SCHEMA
      + "   ]\n"
      + "}";

  public static final String HR_SCHEMA = "     {\n"
      + "       type: 'custom',\n"
      + "       name: 'hr',\n"
      + "       factory: '"
      + ReflectiveSchema.Factory.class.getName()
      + "',\n"
      + "       operand: {\n"
      + "         class: '" + HrSchema.class.getName() + "'\n"
      + "       }\n"
      + "     }\n";

  public static final String HR_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'hr',\n"
      + "   schemas: [\n"
      + HR_SCHEMA
      + "   ]\n"
      + "}";

  public static final String FOODMART_SCOTT_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "   schemas: [\n"
      + FOODMART_SCHEMA
      + ",\n"
      + SCOTT_SCHEMA
      + "   ]\n"
      + "}";

  public static final String START_OF_GROUP_DATA = "(values"
      + "(1,0,1),\n"
      + "(2,0,1),\n"
      + "(3,1,2),\n"
      + "(4,0,3),\n"
      + "(5,0,3),\n"
      + "(6,0,3),\n"
      + "(7,1,4),\n"
      + "(8,1,4))\n"
      + " as t(rn,val,expected)";

  private static String q(String s) {
    return s == null ? "null" : "'" + s + "'";
  }


  static Stream<String> explainFormats() {
    return Stream.of("text", "dot");
  }

  @Test void testJoinFiveWay2() {
    testJoinFiveWay();
  }
  @Test void testJoinFiveWay3() {
    testJoinFiveWay();
  }

  @Test void testJoinFiveWay4() {
    testJoinFiveWay();
  }
  @Test void testJoinFiveWay5() {
    testJoinFiveWay();
  }

  @Test void testJoinFiveWay6() {
    testJoinFiveWay();
  }
  @Test void testJoinFiveWay7() {
    testJoinFiveWay();
  }

  @Test void testJoinFiveWay8() {
    testJoinFiveWay();
  }
  @Test void testJoinFiveWay9() {
    testJoinFiveWay();
  }

  /** Four-way join. Used to take 80 seconds. */
  @Test void testJoinFiveWay() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store\".\"store_country\" as \"c0\",\n"
            + " \"time_by_day\".\"the_year\" as \"c1\",\n"
            + " \"product_class\".\"product_family\" as \"c2\",\n"
            + " count(\"sales_fact_1997\".\"product_id\") as \"m0\"\n"
            + "from \"store\" as \"store\",\n"
            + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + " \"time_by_day\" as \"time_by_day\",\n"
            + " \"product_class\" as \"product_class\",\n"
            + " \"product\" as \"product\"\n"
//            + "/ (SELECT \"store_country\" AS sc, count(*)\n"
//            + "   FROM \"store\"\n"
//            + "   GROUP BY \"store_country\"\n"
//            + ") AS \"store_count\"\n"
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and \"store\".\"store_country\" = 'USA'\n"
//            + "and \"store\".\"store_country\" = 'sc'\n"
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and \"time_by_day\".\"the_year\" = 1997\n"
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "group by \"store\".\"store_country\",\n"
            + " \"time_by_day\".\"the_year\",\n"
            + " \"product_class\".\"product_family\"")
        .explainContains("XXXX PLAN=EnumerableAggregate(group=[{1, 6, 10}], m0=[COUNT()])\n"
            + "  EnumerableMergeJoin(condition=[=($2, $8)], joinType=[inner])\n"
            + "    EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "      EnumerableMergeJoin(condition=[=($3, $5)], joinType=[inner])\n"
            + "        EnumerableSort(sort0=[$3], dir0=[ASC])\n"
            + "          EnumerableHashJoin(condition=[=($0, $4)], joinType=[inner])\n"
            + "            EnumerableCalc(expr#0..23=[{inputs}], expr#24=['USA':VARCHAR(30)], "
            + "expr#25=[=($t9, $t24)], store_id=[$t0], store_country=[$t9], $condition=[$t25])\n"
            + "              EnumerableTableScan(table=[[foodmart2, store]])\n"
            + "            EnumerableCalc(expr#0..7=[{inputs}], proj#0..1=[{exprs}], "
            + "store_id=[$t4])\n"
            + "              EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "        EnumerableCalc(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], "
            + "expr#11=[1997], expr#12=[=($t10, $t11)], time_id=[$t0], the_year=[$t4], "
            + "$condition=[$t12])\n"
            + "          EnumerableTableScan(table=[[foodmart2, time_by_day]])\n"
            + "    EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "      EnumerableCalc(expr#0..14=[{inputs}], proj#0..1=[{exprs}])\n"
            + "        EnumerableTableScan(table=[[foodmart2, product]])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], product_class_id=[$t0], "
            + "product_family=[$t4])\n"
            + "        EnumerableTableScan(table=[[foodmart2, product_class]])\n"
            + "\n");
  }


  /** A schema that contains two tables by reflection.
   *
   * <p>Here is the SQL to create equivalent tables in Oracle:
   *
   * <blockquote>
   * <pre>
   * CREATE TABLE "emps" (
   *   "empid" INTEGER NOT NULL,
   *   "deptno" INTEGER NOT NULL,
   *   "name" VARCHAR2(10) NOT NULL,
   *   "salary" NUMBER(6, 2) NOT NULL,
   *   "commission" INTEGER);
   * INSERT INTO "emps" VALUES (100, 10, 'Bill', 10000, 1000);
   * INSERT INTO "emps" VALUES (200, 20, 'Eric', 8000, 500);
   * INSERT INTO "emps" VALUES (150, 10, 'Sebastian', 7000, null);
   * INSERT INTO "emps" VALUES (110, 10, 'Theodore', 11500, 250);
   *
   * CREATE TABLE "depts" (
   *   "deptno" INTEGER NOT NULL,
   *   "name" VARCHAR2(10) NOT NULL,
   *   "employees" ARRAY OF "Employee",
   *   "location" "Location");
   * INSERT INTO "depts" VALUES (10, 'Sales', null, (-122, 38));
   * INSERT INTO "depts" VALUES (30, 'Marketing', null, (0, 52));
   * INSERT INTO "depts" VALUES (40, 'HR', null, null);
   * </pre>
   * </blockquote>
   */
  public static class HrSchema {
    @Override public String toString() {
      return "HrSchema";
    }

    public final Employee[] emps = {
      new Employee(100, 10, "Bill", 10000, 1000),
      new Employee(200, 20, "Eric", 8000, 500),
      new Employee(150, 10, "Sebastian", 7000, null),
      new Employee(110, 10, "Theodore", 11500, 250),
    };
    public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(30, "Marketing", ImmutableList.of(), new Location(0, 52)),
      new Department(40, "HR", Collections.singletonList(emps[1]), null),
    };
    public final Dependent[] dependents = {
      new Dependent(10, "Michael"),
      new Dependent(10, "Jane"),
    };
    public final Dependent[] locations = {
      new Dependent(10, "San Francisco"),
      new Dependent(20, "San Diego"),
    };

    public QueryableTable foo(int count) {
      return Smalls.generateStrings(count);
    }

    public TranslatableTable view(String s) {
      return Smalls.view(s);
    }
  }

  public static class HrSchemaBig {
    @Override public String toString() {
      return "HrSchema";
    }

    public final Employee[] emps = {
        new Employee(1, 10, "Bill", 10000, 1000),
        new Employee(2, 20, "Eric", 8000, 500),
        new Employee(3, 10, "Sebastian", 7000, null),
        new Employee(4, 10, "Theodore", 11500, 250),
        new Employee(5, 10, "Marjorie", 10000, 1000),
        new Employee(6, 20, "Guy", 8000, 500),
        new Employee(7, 10, "Dieudonne", 7000, null),
        new Employee(8, 10, "Haroun", 11500, 250),
        new Employee(9, 10, "Sarah", 10000, 1000),
        new Employee(10, 20, "Gabriel", 8000, 500),
        new Employee(11, 10, "Pierre", 7000, null),
        new Employee(12, 10, "Paul", 11500, 250),
        new Employee(13, 10, "Jacques", 100, 1000),
        new Employee(14, 20, "Khawla", 8000, 500),
        new Employee(15, 10, "Brielle", 7000, null),
        new Employee(16, 10, "Hyuna", 11500, 250),
        new Employee(17, 10, "Ahmed", 10000, 1000),
        new Employee(18, 20, "Lara", 8000, 500),
        new Employee(19, 10, "Capucine", 7000, null),
        new Employee(20, 10, "Michelle", 11500, 250),
        new Employee(21, 10, "Cerise", 10000, 1000),
        new Employee(22, 80, "Travis", 8000, 500),
        new Employee(23, 10, "Taylor", 7000, null),
        new Employee(24, 10, "Seohyun", 11500, 250),
        new Employee(25, 70, "Helen", 10000, 1000),
        new Employee(26, 50, "Patric", 8000, 500),
        new Employee(27, 10, "Clara", 7000, null),
        new Employee(28, 10, "Catherine", 11500, 250),
        new Employee(29, 10, "Anibal", 10000, 1000),
        new Employee(30, 30, "Ursula", 8000, 500),
        new Employee(31, 10, "Arturito", 7000, null),
        new Employee(32, 70, "Diane", 11500, 250),
        new Employee(33, 10, "Phoebe", 10000, 1000),
        new Employee(34, 20, "Maria", 8000, 500),
        new Employee(35, 10, "Edouard", 7000, null),
        new Employee(36, 110, "Isabelle", 11500, 250),
        new Employee(37, 120, "Olivier", 10000, 1000),
        new Employee(38, 20, "Yann", 8000, 500),
        new Employee(39, 60, "Ralf", 7000, null),
        new Employee(40, 60, "Emmanuel", 11500, 250),
        new Employee(41, 10, "Berenice", 10000, 1000),
        new Employee(42, 20, "Kylie", 8000, 500),
        new Employee(43, 80, "Natacha", 7000, null),
        new Employee(44, 100, "Henri", 11500, 250),
        new Employee(45, 90, "Pascal", 10000, 1000),
        new Employee(46, 90, "Sabrina", 8000, 500),
        new Employee(47, 8, "Riyad", 7000, null),
        new Employee(48, 5, "Andy", 11500, 250),
    };
    public final Department[] depts = {
        new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(20, "Marketing", ImmutableList.of(), new Location(0, 52)),
        new Department(30, "HR", Collections.singletonList(emps[1]), null),
        new Department(40, "Administration", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(50, "Design", ImmutableList.of(), new Location(0, 52)),
        new Department(60, "IT", Collections.singletonList(emps[1]), null),
        new Department(70, "Production", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(80, "Finance", ImmutableList.of(), new Location(0, 52)),
        new Department(90, "Accounting", Collections.singletonList(emps[1]), null),
        new Department(100, "Research", Arrays.asList(emps[0], emps[2]),
            new Location(-122, 38)),
        new Department(110, "Maintenance", ImmutableList.of(), new Location(0, 52)),
        new Department(120, "Client Support", Collections.singletonList(emps[1]), null),
    };
  }

  public static class Employee {
    public final int empid;
    public final int deptno;
    public final String name;
    public final float salary;
    public final Integer commission;

    public Employee(int empid, int deptno, String name, float salary,
        Integer commission) {
      this.empid = empid;
      this.deptno = deptno;
      this.name = name;
      this.salary = salary;
      this.commission = commission;
    }

    @Override public String toString() {
      return "Employee [empid: " + empid + ", deptno: " + deptno
          + ", name: " + name + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Employee
          && empid == ((Employee) obj).empid;
    }
  }

  public static class Department {
    public final int deptno;
    public final String name;

    @org.apache.calcite.adapter.java.Array(component = Employee.class)
    public final List<Employee> employees;
    public final Location location;

    public Department(int deptno, String name, List<Employee> employees,
        Location location) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
      this.location = location;
    }

    @Override public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + ", location: " + location + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Department
          && deptno == ((Department) obj).deptno;
    }
  }

  public static class Location {
    public final int x;
    public final int y;

    public Location(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override public String toString() {
      return "Location [x: " + x + ", y: " + y + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Location
          && x == ((Location) obj).x
          && y == ((Location) obj).y;
    }
  }

  public static class Dependent {
    public final int empid;
    public final String name;

    public Dependent(int empid, String name) {
      this.empid = empid;
      this.name = name;
    }

    @Override public String toString() {
      return "Dependent [empid: " + empid + ", name: " + name + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Dependent
          && empid == ((Dependent) obj).empid
          && Objects.equals(name, ((Dependent) obj).name);
    }
  }

  public static class Event {
    public final int eventid;
    public final Timestamp ts;

    public Event(int eventid, Timestamp ts) {
      this.eventid = eventid;
      this.ts = ts;
    }

    @Override public String toString() {
      return "Event [eventid: " + eventid + ", ts: " + ts + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Event
          && eventid == ((Event) obj).eventid;
    }
  }

  public static class FoodmartSchema {
    public final SalesFact[] sales_fact_1997 = {
      new SalesFact(100, 10),
      new SalesFact(150, 20),
    };
  }

  public static class LingualSchema {
    public final LingualEmp[] EMPS = {
      new LingualEmp(1, 10),
      new LingualEmp(2, 30)
    };
  }

  public static class LingualEmp {
    public final int EMPNO;
    public final int DEPTNO;

    public LingualEmp(int EMPNO, int DEPTNO) {
      this.EMPNO = EMPNO;
      this.DEPTNO = DEPTNO;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof LingualEmp
          && EMPNO == ((LingualEmp) obj).EMPNO;
    }
  }

  public static class FoodmartJdbcSchema extends JdbcSchema {
    public FoodmartJdbcSchema(DataSource dataSource, SqlDialect dialect,
        JdbcConvention convention, String catalog, String schema) {
      super(dataSource, dialect, convention, catalog, schema);
    }

    public final Table customer = getTable("customer");
  }

  public static class Customer {
    public final int customer_id;

    public Customer(int customer_id) {
      this.customer_id = customer_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Customer
          && customer_id == ((Customer) obj).customer_id;
    }
  }

  public static class SalesFact {
    public final int cust_id;
    public final int prod_id;

    public SalesFact(int cust_id, int prod_id) {
      this.cust_id = cust_id;
      this.prod_id = prod_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof SalesFact
          && cust_id == ((SalesFact) obj).cust_id
          && prod_id == ((SalesFact) obj).prod_id;
    }
  }

  //CHECKSTYLE: ON

  /** Abstract base class for implementations of {@link ModifiableTable}. */
  public abstract static class AbstractModifiableTable
      extends AbstractTable implements ModifiableTable {
    protected AbstractModifiableTable(String tableName) {
    }

    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      return LogicalTableModify.create(table, catalogReader, child, operation,
          updateColumnList, sourceExpressionList, flattened);
    }
  }

  /** Abstract base class for implementations of {@link ModifiableView}. */
  public abstract static class AbstractModifiableView
      extends AbstractTable implements ModifiableView {
    protected AbstractModifiableView() {
    }
  }

  /** Factory for EMP and DEPT tables. */
  public static class EmpDeptTableFactory implements TableFactory<Table> {
    public static final TryThreadLocal<List<Employee>> THREAD_COLLECTION =
        TryThreadLocal.of(null);

    public Table create(
        SchemaPlus schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final Class clazz;
      final Object[] array;
      switch (name) {
      case "EMPLOYEES":
        clazz = Employee.class;
        array = new HrSchema().emps;
        break;
      case "MUTABLE_EMPLOYEES":
        List<Employee> employees = THREAD_COLLECTION.get();
        if (employees == null) {
          employees = Collections.emptyList();
        }
        return JdbcFrontLinqBackTest.mutable(name, employees);
      case "DEPARTMENTS":
        clazz = Department.class;
        array = new HrSchema().depts;
        break;
      default:
        throw new AssertionError(name);
      }
      return new AbstractQueryableTable(clazz) {
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return ((JavaTypeFactory) typeFactory).createType(clazz);
        }

        public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
            SchemaPlus schema, String tableName) {
          return new AbstractTableQueryable<T>(queryProvider, schema, this,
              tableName) {
            public Enumerator<T> enumerator() {
              @SuppressWarnings("unchecked") final List<T> list =
                  (List) Arrays.asList(array);
              return Linq4j.enumerator(list);
            }
          };
        }
      };
    }
  }

  /** Schema factory that creates {@link MySchema} objects. */
  public static class MySchemaFactory implements SchemaFactory {
    public Schema create(
        SchemaPlus parentSchema,
        String name,
        final Map<String, Object> operand) {
      final boolean mutable =
          SqlFunctions.isNotFalse((Boolean) operand.get("mutable"));
      return new ReflectiveSchema(new HrSchema()) {
        @Override protected Map<String, Table> getTableMap() {
          // Mine the EMPS table and add it under another name e.g. ELVIS
          final Map<String, Table> tableMap = super.getTableMap();
          final Table table = tableMap.get("emps");
          final String tableName = (String) operand.get("tableName");
          return FlatLists.append(tableMap, tableName, table);
        }

        @Override public boolean isMutable() {
          return mutable;
        }
      };
    }
  }

  /** Mock driver that has a handler that stores the results of each query in
   * a temporary table. */
  public static class AutoTempDriver
      extends org.apache.calcite.jdbc.Driver {
    private final List<Object> results;

    AutoTempDriver(List<Object> results) {
      this.results = results;
    }

    @Override protected Handler createHandler() {
      return new HandlerImpl() {
        @Override public void onStatementExecute(
            AvaticaStatement statement,
            ResultSink resultSink) {
          super.onStatementExecute(statement, resultSink);
          results.add(resultSink);
        }
      };
    }
  }

  /** Mock driver that a given {@link Handler}. */
  public static class HandlerDriver extends org.apache.calcite.jdbc.Driver {
    private static final TryThreadLocal<Handler> HANDLERS =
        TryThreadLocal.of(null);

    public HandlerDriver() {
    }

    @Override protected Handler createHandler() {
      return HANDLERS.get();
    }
  }

  /** Mock driver that can execute a trivial DDL statement. */
  public static class MockDdlDriver extends org.apache.calcite.jdbc.Driver {
    public int counter;

    public MockDdlDriver() {
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
      return new Function0<CalcitePrepare>() {
        @Override public CalcitePrepare apply() {
          return new CalcitePrepareImpl() {
            @Override protected SqlParser.Config parserConfig() {
              return super.parserConfig().withParserFactory(stream ->
                  new SqlParserImpl(stream) {
                    @Override public SqlNode parseSqlStmtEof() {
                      return new SqlCall(SqlParserPos.ZERO) {
                        @Override public SqlOperator getOperator() {
                          return new SqlSpecialOperator("COMMIT",
                              SqlKind.COMMIT);
                        }

                        @Override public List<SqlNode> getOperandList() {
                          return ImmutableList.of();
                        }
                      };
                    }
                  });
            }

            @Override public void executeDdl(Context context, SqlNode node) {
              ++counter;
            }
          };
        }
      };
    }
  }

  /** Dummy table. */
  public static class MyTable {
    public String mykey = "foo";
    public Integer myvalue = 1;
  }

  /** Another dummy table. */
  public static class MyTable2 {
    public String mykey = "foo";
    public Integer myvalue = 2;
  }

  /** Schema containing dummy tables. */
  public static class MySchema {
    public MyTable[] mytable = { new MyTable() };
    public MyTable2[] mytable2 = { new MyTable2() };
  }

  /** Locales for which to test DAYNAME and MONTHNAME functions,
   * and expected results of those functions. */
  enum TestLocale {
    ROOT(Locale.ROOT.toString(), shorten("Wednesday"), shorten("Sunday"),
        shorten("January"), shorten("February"), 0),
    EN("en", "Wednesday", "Sunday", "January", "February", 0),
    FR("fr", "mercredi", "dimanche", "janvier", "f\u00e9vrier", 6),
    FR_FR("fr_FR", "mercredi", "dimanche", "janvier", "f\u00e9vrier", 6),
    FR_CA("fr_CA", "mercredi", "dimanche", "janvier", "f\u00e9vrier", 6),
    ZH_CN("zh_CN", "\u661f\u671f\u4e09", "\u661f\u671f\u65e5", "\u4e00\u6708",
        "\u4e8c\u6708", 6),
    ZH("zh", "\u661f\u671f\u4e09", "\u661f\u671f\u65e5", "\u4e00\u6708",
        "\u4e8c\u6708", 6);

    private static String shorten(String name) {
      // In root locale, for Java versions 9 and higher, day and month names
      // are shortened to 3 letters. This means root locale behaves differently
      // to English.
      return TestUtil.getJavaMajorVersion() > 8 ? name.substring(0, 3) : name;
    }

    public final String localeName;
    public final String wednesday;
    public final String sunday;
    public final String january;
    public final String february;
    public final int sundayDayOfWeek;

    TestLocale(String localeName, String wednesday, String sunday,
        String january, String february, int sundayDayOfWeek) {
      this.localeName = localeName;
      this.wednesday = wednesday;
      this.sunday = sunday;
      this.january = january;
      this.february = february;
      this.sundayDayOfWeek = sundayDayOfWeek;
    }
  }
}
