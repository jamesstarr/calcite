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
package org.apache.calcite.util;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.RandomAccess;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.function.BiFunction;
>>>>>>> cd779c1b5 (DX-7801 [CALCITE-4433] Add UNPIVOT operator to SQL)
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
=======
  //~ Inner Classes ----------------------------------------------------------

  /**
   * Exception used to interrupt a tree walk of any kind.
   */
  public static class FoundOne extends ControlFlowException {
    private final Object node;

    /** Singleton instance. Can be used if you don't care about node. */
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final FoundOne NULL = new FoundOne(null);

    public FoundOne(Object node) {
      this.node = node;
    }

    public Object getNode() {
      return node;
    }
  }

  /**
   * Visitor which looks for an OVER clause inside a tree of
   * {@link SqlNode} objects.
   */
  public static class OverFinder extends SqlBasicVisitor<Void> {
    public static final OverFinder INSTANCE = new Util.OverFinder();

    @Override public Void visit(SqlCall call) {
      if (call.getKind() == SqlKind.OVER) {
        throw FoundOne.NULL;
      }
      return super.visit(call);
    }
  }
<<<<<<< HEAD
=======

  /** List that returns the same number of elements as a backing list,
   * applying a transformation function to each one.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class TransformingList<F, T> extends AbstractList<T> {
    private final java.util.function.Function<? super F, ? extends T> function;
    private final List<? extends F> list;

    TransformingList(List<? extends F> list,
                     java.util.function.Function<? super F, ? extends T> function) {
      this.function = function;
      this.list = list;
    }

    @Override public T get(int i) {
      return function.apply(list.get(i));
    }

    @Override public int size() {
      return list.size();
    }

    @Override public Iterator<T> iterator() {
      return listIterator();
    }
  }

  /** Extension to {@link TransformingList} that implements
   * {@link RandomAccess}.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class RandomAccessTransformingList<F, T>
          extends TransformingList<F, T> implements RandomAccess {
    RandomAccessTransformingList(List<? extends F> list,
                                 java.util.function.Function<? super F, ? extends T> function) {
      super(list, function);
    }
  }

  /** List that returns the same number of elements as a backing list,
   * applying a transformation function to each one.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class TransformingIndexedList<F, T> extends AbstractList<T> {
    private final BiFunction<? super F, Integer, ? extends T> function;
    private final List<? extends F> list;

    TransformingIndexedList(List<? extends F> list,
        BiFunction<? super F, Integer, ? extends T> function) {
      this.function = function;
      this.list = list;
    }

    @Override public T get(int i) {
      return function.apply(list.get(i), i);
    }

    @Override public int size() {
      return list.size();
    }

    @Override public Iterator<T> iterator() {
      return listIterator();
    }
  }

  /** Extension to {@link TransformingIndexedList} that implements
   * {@link RandomAccess}.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class RandomAccessTransformingIndexedList<F, T>
      extends TransformingIndexedList<F, T> implements RandomAccess {
    RandomAccessTransformingIndexedList(List<? extends F> list,
        BiFunction<? super F, Integer, ? extends T> function) {
      super(list, function);
    }
  }

  /**
   * An {@link java.util.Iterator} that transforms its elements on-the-fly.
   *
   * @param <F> The element type of the delegate iterator
   * @param <T> The element type of this iterator
   */
  private static class TransformingIterator<F, T> implements Iterator<T> {
    private final Iterator<? extends F> delegate;
    private final java.util.function.Function<? super F, ? extends T> function;

    TransformingIterator(Iterator<? extends F> delegate,
                         java.util.function.Function<? super F, ? extends T> function) {
      this.delegate = delegate;
      this.function = function;
    }

    @Override public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override public final T next() {
      return function.apply(delegate.next());
    }

    @Override public void remove() {
      delegate.remove();
    }
  }
}
>>>>>>> cd779c1b5 (DX-7801 [CALCITE-4433] Add UNPIVOT operator to SQL)

  /** List that returns the same number of elements as a backing list,
   * applying a transformation function to each one.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class TransformingList<F, T> extends AbstractList<T> {
    private final java.util.function.Function<? super F, ? extends T> function;
    private final List<? extends F> list;

    TransformingList(List<? extends F> list,
                     java.util.function.Function<? super F, ? extends T> function) {
      this.function = function;
      this.list = list;
    }

    @Override public T get(int i) {
      return function.apply(list.get(i));
    }

    @Override public int size() {
      return list.size();
    }

    @Override public Iterator<T> iterator() {
      return listIterator();
    }
  }

  /** Extension to {@link TransformingList} that implements
   * {@link RandomAccess}.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class RandomAccessTransformingList<F, T>
          extends TransformingList<F, T> implements RandomAccess {
    RandomAccessTransformingList(List<? extends F> list,
                                 java.util.function.Function<? super F, ? extends T> function) {
      super(list, function);
    }
  }

  /** List that returns the same number of elements as a backing list,
   * applying a transformation function to each one.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class TransformingIndexedList<F, T> extends AbstractList<T> {
    private final BiFunction<? super F, Integer, ? extends T> function;
    private final List<? extends F> list;

    TransformingIndexedList(List<? extends F> list,
        BiFunction<? super F, Integer, ? extends T> function) {
      this.function = function;
      this.list = list;
    }

    @Override public T get(int i) {
      return function.apply(list.get(i), i);
    }

    @Override public int size() {
      return list.size();
    }

    @Override public Iterator<T> iterator() {
      return listIterator();
    }
  }

  /** Extension to {@link TransformingIndexedList} that implements
   * {@link RandomAccess}.
   *
   * @param <F> Element type of backing list
   * @param <T> Element type of this list
   */
  private static class RandomAccessTransformingIndexedList<F, T>
      extends TransformingIndexedList<F, T> implements RandomAccess {
    RandomAccessTransformingIndexedList(List<? extends F> list,
        BiFunction<? super F, Integer, ? extends T> function) {
      super(list, function);
    }
  }

  /** Iterator that applies a predicate to each element.
   *
   * @param <T> Element type */
  private static class FilteringIterator<T> implements Iterator<T> {
    private static final Object DUMMY = new Object();
    final Iterator<? extends T> iterator;
    private final Predicate<? super T> predicate;
    T current;

    FilteringIterator(Iterator<? extends T> iterator,
        Predicate<? super T> predicate) {
      this.iterator = iterator;
      this.predicate = predicate;
      @SuppressWarnings("method.invocation.invalid")
      T current = moveNext();
      this.current = current;
    }

    @Override public boolean hasNext() {
      return current != DUMMY;
    }

    @Override public T next() {
      final T t = this.current;
      current = moveNext();
      return t;
    }

    protected T moveNext() {
      while (iterator.hasNext()) {
        T t = iterator.next();
        if (predicate.test(t)) {
          return t;
        }
      }
      return (T) DUMMY;
    }
  }

  /**
   * An {@link java.util.Iterator} that transforms its elements on-the-fly.
   *
   * @param <F> The element type of the delegate iterator
   * @param <T> The element type of this iterator
   */
  private static class TransformingIterator<F, T> implements Iterator<T> {
    private final Iterator<? extends F> delegate;
    private final java.util.function.Function<? super F, ? extends T> function;

    TransformingIterator(Iterator<? extends F> delegate,
                         java.util.function.Function<? super F, ? extends T> function) {
      this.delegate = delegate;
      this.function = function;
    }

    @Override public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override public final T next() {
      return function.apply(delegate.next());
    }

    @Override public void remove() {
      delegate.remove();
    }
  }
}
