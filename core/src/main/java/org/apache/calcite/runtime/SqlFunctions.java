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
package org.apache.calcite.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.CartesianProductEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.runtime.FlatLists.ComparableList;
import org.apache.calcite.runtime.variant.VariantValue;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.TimeWithTimeZoneString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Unsafe;
import org.apache.calcite.util.Util;

import com.google.common.base.Optional;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
>>>>>>> 999c7aa89 (Revert "Revert "[CALCITE-2355] Implement multiset operations (Sergey Nuyanzin)"")
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
=======
    final Set<E> result = new HashSet<E>(c1);
    result.retainAll(c2);
    return new ArrayList<E>(result);
  }

  /** Support the MULTISET INTERSECT ALL function. */
  public static <E> Collection<E> multisetIntersectAll(Collection<E> c1,
      Collection<E> c2) {
    final List<E> result = new ArrayList<>(c1.size());
    final List<E> c2Copy = new ArrayList<>(c2);
    for (E e : c1) {
      if (c2Copy.remove(e)) {
        result.add(e);
      }
    }
    return result;
  }

  /** Support the MULTISET EXCEPT ALL function. */
  public static <E> Collection<E> multisetExceptAll(Collection<E> c1,
      Collection<E> c2) {
    final List<E> result = new LinkedList<>(c1);
    for (E e : c2) {
      result.remove(e);
    }
    return result;
  }

  /** Support the MULTISET EXCEPT DISTINCT function. */
  public static <E> Collection<E> multisetExceptDistinct(Collection<E> c1,
      Collection<E> c2) {
    final Set<E> result = new HashSet<>(c1);
    result.removeAll(c2);
    return new ArrayList<>(result);
  }

  /** Support the IS A SET function. */
  public static boolean isASet(Collection collection) {
    if (collection instanceof Set) {
      return true;
    }
    // capacity calculation is in the same way like for new HashSet(Collection)
    // however return immediately in case of duplicates
    Set set = new HashSet(Math.max((int) (collection.size() / .75f) + 1, 16));
    for (Object e : collection) {
      if (!set.add(e)) {
        return false;
      }
    }
    return true;
  }

  /** Support the SUBMULTISET OF function. */
  public static boolean submultisetOf(Collection possibleSubMultiset,
      Collection multiset) {
    if (possibleSubMultiset.size() > multiset.size()) {
      return false;
    }
    Collection multisetLocal = new LinkedList(multiset);
    for (Object e : possibleSubMultiset) {
      if (!multisetLocal.remove(e)) {
        return false;
      }
    }
    return true;
  }

  /** Support the MULTISET UNION function. */
  public static Collection multisetUnionDistinct(Collection collection1,
      Collection collection2) {
    // capacity calculation is in the same way like for new HashSet(Collection)
    Set resultCollection =
        new HashSet(Math.max((int) ((collection1.size() + collection2.size()) / .75f) + 1, 16));
    resultCollection.addAll(collection1);
    resultCollection.addAll(collection2);
    return new ArrayList(resultCollection);
  }

  /** Support the MULTISET UNION ALL function. */
  public static Collection multisetUnionAll(Collection collection1,
      Collection collection2) {
    List resultCollection = new ArrayList(collection1.size() + collection2.size());
    resultCollection.addAll(collection1);
    resultCollection.addAll(collection2);
    return resultCollection;
  }

<<<<<<< HEAD
=======
=======
  public static Function1<Object, Enumerable<ComparableList<Comparable>>> flatProduct(
      final int[] fieldCounts, final boolean withOrdinality,
      final FlatProductInputType[] inputTypes) {
    if (fieldCounts.length == 1) {
      if (!withOrdinality && inputTypes[0] == FlatProductInputType.SCALAR) {
        //noinspection unchecked
        return (Function1) LIST_AS_ENUMERABLE;
      } else {
        return row -> p2(new Object[] { row }, fieldCounts, withOrdinality,
            inputTypes);
      }
    }
    return lists -> p2((Object[]) lists, fieldCounts, withOrdinality,
        inputTypes);
  }

  private static Enumerable<FlatLists.ComparableList<Comparable>> p2(
      Object[] lists, int[] fieldCounts, boolean withOrdinality,
      FlatProductInputType[] inputTypes) {
    final List<Enumerator<List<Comparable>>> enumerators = new ArrayList<>();
    int totalFieldCount = 0;
    for (int i = 0; i < lists.length; i++) {
      int fieldCount = fieldCounts[i];
      FlatProductInputType inputType = inputTypes[i];
      Object inputObject = lists[i];
      switch (inputType) {
      case SCALAR:
        @SuppressWarnings("unchecked") List<Comparable> list =
            (List<Comparable>) inputObject;
        enumerators.add(
            Linq4j.transform(
                Linq4j.enumerator(list), FlatLists::of));
        break;
      case LIST:
        @SuppressWarnings("unchecked") List<List<Comparable>> listList =
            (List<List<Comparable>>) inputObject;
        enumerators.add(Linq4j.enumerator(listList));
        break;
      case MAP:
        @SuppressWarnings("unchecked") Map<Comparable, Comparable> map =
            (Map<Comparable, Comparable>) inputObject;
        Enumerator<Entry<Comparable, Comparable>> enumerator =
            Linq4j.enumerator(map.entrySet());

        Enumerator<List<Comparable>> transformed = Linq4j.transform(enumerator,
            e -> FlatLists.of(e.getKey(), e.getValue()));
        enumerators.add(transformed);
        break;
      default:
        break;
      }
      if (fieldCount < 0) {
        ++totalFieldCount;
      } else {
        totalFieldCount += fieldCount;
      }
    }
    if (withOrdinality) {
      ++totalFieldCount;
    }
    return product(enumerators, totalFieldCount, withOrdinality);
  }

  public static Object[] array(Object... args) {
    return args;
  }

  /** Similar to {@link Linq4j#product(Iterable)} but each resulting list
   * implements {@link FlatLists.ComparableList}. */
  public static <E extends Comparable> Enumerable<FlatLists.ComparableList<E>> product(
      final List<Enumerator<List<E>>> enumerators, final int fieldCount,
      final boolean withOrdinality) {
    return new AbstractEnumerable<FlatLists.ComparableList<E>>() {
      public Enumerator<FlatLists.ComparableList<E>> enumerator() {
        return new ProductComparableListEnumerator<>(enumerators, fieldCount,
            withOrdinality);
      }
    };
  }

  /** Adds a given number of months to a timestamp, represented as the number
   * of milliseconds since the epoch. */
  public static long addMonths(long timestamp, int m) {
    final long millis =
        DateTimeUtils.floorMod(timestamp, DateTimeUtils.MILLIS_PER_DAY);
    timestamp -= millis;
    final long x =
        addMonths((int) (timestamp / DateTimeUtils.MILLIS_PER_DAY), m);
    return x * DateTimeUtils.MILLIS_PER_DAY + millis;
  }

  /** Adds a given number of months to a date, represented as the number of
   * days since the epoch. */
  public static int addMonths(int date, int m) {
    int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
    int d0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.DAY, date);
    m0 += m;
    int deltaYear = (int) DateTimeUtils.floorDiv(m0, 12);
    y0 += deltaYear;
    m0 = (int) DateTimeUtils.floorMod(m0, 12);
    if (m0 == 0) {
      y0 -= 1;
      m0 += 12;
    }

    int last = lastDay(y0, m0);
    if (d0 > last) {
      d0 = last;
    }
    return DateTimeUtils.ymdToUnixDate(y0, m0, d0);
  }

  private static int lastDay(int y, int m) {
    switch (m) {
    case 2:
      return y % 4 == 0
          && (y % 100 != 0
          || y % 400 == 0)
          ? 29 : 28;
    case 4:
    case 6:
    case 9:
    case 11:
      return 30;
    default:
      return 31;
    }
  }

  /** Finds the number of months between two dates, each represented as the
   * number of days since the epoch. */
  public static int subtractMonths(int date0, int date1) {
    if (date0 < date1) {
      return -subtractMonths(date1, date0);
    }
    // Start with an estimate.
    // Since no month has more than 31 days, the estimate is <= the true value.
    int m = (date0 - date1) / 31;
    for (;;) {
      int date2 = addMonths(date1, m);
      if (date2 >= date0) {
        return m;
      }
      int date3 = addMonths(date1, m + 1);
      if (date3 > date0) {
        return m;
      }
      ++m;
    }
  }

  public static int subtractMonths(long t0, long t1) {
    final long millis0 =
        DateTimeUtils.floorMod(t0, DateTimeUtils.MILLIS_PER_DAY);
    final int d0 = (int) DateTimeUtils.floorDiv(t0 - millis0,
        DateTimeUtils.MILLIS_PER_DAY);
    final long millis1 =
        DateTimeUtils.floorMod(t1, DateTimeUtils.MILLIS_PER_DAY);
    final int d1 = (int) DateTimeUtils.floorDiv(t1 - millis1,
        DateTimeUtils.MILLIS_PER_DAY);
    int x = subtractMonths(d0, d1);
    final long d2 = addMonths(d1, x);
    if (d2 == d0 && millis0 < millis1) {
      --x;
    }
    return x;
  }

  /**
   * Implements the {@code .} (field access) operator on an object
   * whose type is not known until runtime.
   *
   * <p>A struct object can be represented in various ways by the
   * runtime and depends on the
   * {@link org.apache.calcite.adapter.enumerable.JavaRowFormat}.
   */
  @Experimental
  public static Object structAccess(Object structObject, int index, String fieldName) {
    if (structObject == null) {
      return null;
    }

    if (structObject instanceof Object[]) {
      return ((Object[]) structObject)[index];
    } else if (structObject instanceof List) {
      return ((List) structObject).get(index);
    } else if (structObject instanceof Row) {
      return ((Row) structObject).getObject(index);
    } else {
      Class<?> beanClass = structObject.getClass();
      try {
        Field structField = beanClass.getDeclaredField(fieldName);
        return structField.get(structObject);
      } catch (NoSuchFieldException | IllegalAccessException ex) {
        throw RESOURCE.failedToAccessField(fieldName, beanClass.getName()).ex(ex);
      }
    }
  }

  /** Enumerates over the cartesian product of the given lists, returning
   * a comparable list for each row.
   *
   * @param <E> element type */
  private static class ProductComparableListEnumerator<E extends Comparable>
      extends CartesianProductEnumerator<List<E>, FlatLists.ComparableList<E>> {
    final E[] flatElements;
    final List<E> list;
    private final boolean withOrdinality;
    private int ordinality;

    ProductComparableListEnumerator(List<Enumerator<List<E>>> enumerators,
        int fieldCount, boolean withOrdinality) {
      super(enumerators);
      this.withOrdinality = withOrdinality;
      flatElements = (E[]) new Comparable[fieldCount];
      list = Arrays.asList(flatElements);
    }

    @Override public boolean moveNext() {
      boolean hasNext = super.moveNext();
      if (hasNext && withOrdinality) {
        ordinality++;
      }
      return hasNext;
    }

    public FlatLists.ComparableList<E> current() {
      int i = 0;
      for (Object element : (Object[]) elements) {
        Object[] a;
        if (element.getClass().isArray()) {
          a = (Object[]) element;
        } else {
          final List list2 = (List) element;
          a = list2.toArray();
        }
        System.arraycopy(a, 0, flatElements, i, a.length);
        i += a.length;
      }
      if (withOrdinality) {
        flatElements[i] = (E) Integer.valueOf(ordinality);
      }
      return FlatLists.ofComparable(list);
    }

    @Override public void reset() {
      super.reset();
      if (withOrdinality) {
        ordinality = 0;
      }
    }
  }

  /** Type of argument passed into {@link #flatProduct}. */
  public enum FlatProductInputType {
    SCALAR, LIST, MAP
  }

}
