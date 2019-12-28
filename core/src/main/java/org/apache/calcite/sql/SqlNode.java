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

import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import javax.annotation.Nonnull;

/**
 * A <code>SqlNode</code> is a SQL parse tree.
 *
 * <p>It may be an
 * {@link SqlOperator operator}, {@link SqlLiteral literal},
 * {@link SqlIdentifier identifier}, and so forth.
 */
public abstract class SqlNode implements Cloneable {
  //~ Static fields/initializers ---------------------------------------------

  public static final SqlNode[] EMPTY_ARRAY = new SqlNode[0];

  //~ Instance fields --------------------------------------------------------

  protected final SqlParserPos pos;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   */
  SqlNode(SqlParserPos pos) {
    this.pos = Objects.requireNonNull(pos);
  }

  //~ Methods ----------------------------------------------------------------

  /** @deprecated Please use {@link #clone(SqlNode)}; this method brings
   * along too much baggage from early versions of Java */
  @Deprecated
  @SuppressWarnings("MethodDoesntCallSuperMethod")
  public Object clone() {
    return clone(getParserPosition());
  }

  /** Creates a copy of a SqlNode. */
  public static <E extends SqlNode> E clone(E e) {
    //noinspection unchecked
    return (E) e.clone(e.pos);
  }

  /**
   * Clones a SqlNode with a different position.
   */
  public abstract SqlNode clone(SqlParserPos pos);

  /**
   * Returns the type of node this is, or
   * {@link org.apache.calcite.sql.SqlKind#OTHER} if it's nothing special.
   *
   * @return a {@link SqlKind} value, never null
   * @see #isA
   */
  public @Nonnull SqlKind getKind() {
    return SqlKind.OTHER;
  }

  /**
   * Returns whether this node is a member of an aggregate category.
   *
   * <p>For example, {@code node.isA(SqlKind.QUERY)} returns {@code true}
   * if the node is a SELECT, INSERT, UPDATE etc.
   *
   * <p>This method is shorthand: {@code node.isA(category)} is always
   * equivalent to {@code node.getKind().belongsTo(category)}.
   *
   * @param category Category
   * @return Whether this node belongs to the given category.
   */
  public final boolean isA(Set<SqlKind> category) {
    return getKind().belongsTo(category);
  }

  @Deprecated // to be removed before 2.0
  public static SqlNode[] cloneArray(SqlNode[] nodes) {
    SqlNode[] clones = nodes.clone();
    for (int i = 0; i < clones.length; i++) {
      SqlNode node = clones[i];
      if (node != null) {
        clones[i] = SqlNode.clone(node);
      }
    }
    return clones;
  }

  public String toString() {
    return toSqlString(c -> c.withDialect(AnsiSqlDialect.DEFAULT)
        .withAlwaysUseParentheses(false)
        .withSelectListItemsOnSeparateLines(false)
        .withUpdateSetListNewline(false)
        .withIndentation(0)).getSql();

  /**
   * Returns a {@code Collector} that accumulates the input elements into a
   * {@link SqlNodeList}, with zero position.
   *
   * @param <T> Type of the input elements
   *
   * @return a {@code Collector} that collects all the input elements into a
   * {@link SqlNodeList}, in encounter order
   */
  public static <T extends SqlNode> Collector<T, ArrayList<T>, SqlNodeList>
      toList() {
    return toList(SqlParserPos.ZERO);
  }

  /**
   * Returns a {@code Collector} that accumulates the input elements into a
   * {@link SqlNodeList}.
   *
   * @param <T> Type of the input elements
   *
   * @return a {@code Collector} that collects all the input elements into a
   * {@link SqlNodeList}, in encounter order
   */
  public static <T extends SqlNode> Collector<T, ArrayList<T>, SqlNodeList>
      toList(SqlParserPos pos) {
    return Collector.of(ArrayList::new, ArrayList::add, Util::combine,
        list -> new SqlNodeList(list, pos));
  }
}
<<<<<<< HEAD

  /**
   * Returns a {@code Collector} that accumulates the input elements into a
   * {@link SqlNodeList}, with zero position.
   *
   * @param <T> Type of the input elements
   *
   * @return a {@code Collector} that collects all the input elements into a
   * {@link SqlNodeList}, in encounter order
   */
  public static <T extends SqlNode> Collector<T, ArrayList<T>, SqlNodeList>
      toList() {
    return toList(SqlParserPos.ZERO);
  }

  /**
   * Returns a {@code Collector} that accumulates the input elements into a
   * {@link SqlNodeList}.
   *
   * @param <T> Type of the input elements
   *
   * @return a {@code Collector} that collects all the input elements into a
   * {@link SqlNodeList}, in encounter order
   */
  public static <T extends SqlNode> Collector<T, ArrayList<T>, SqlNodeList>
      toList(SqlParserPos pos) {
    return Collector.of(ArrayList::new, ArrayList::add, Util::combine,
        list -> new SqlNodeList(list, pos));
  }
}
=======
