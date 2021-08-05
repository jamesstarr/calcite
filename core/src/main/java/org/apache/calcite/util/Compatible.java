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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

import static java.util.Objects.requireNonNull;

/** Compatibility layer.
 *
 * <p>Allows to use advanced functionality if the latest JDK or Guava version
 * is present.
 */
public interface Compatible {
  Compatible INSTANCE = new Factory().create();


              if (method.getName().equals("asMap")) {
                // Use the Guava implementation Maps.asMap if it is available
                try {
                  //noinspection ConfusingArgumentToVarargsMethod
                  final Method guavaMethod = Maps.class.getMethod(
                        method.getName(), method.getParameterTypes());
                  return guavaMethod.invoke(null, args);
                } catch (NoSuchMethodException e) {
                  Set set = (Set) args[0];
                  Function function = (Function) args[1];
                  return CompatibleGuava11.asMap(set, function);
                }
              }
              if (method.getName().equals("navigableSet")) {
                ImmutableSortedSet set = (ImmutableSortedSet) args[0];
                return CompatibleGuava11.navigableSet(set);
              }
              if (method.getName().equals("navigableMap")) {
                ImmutableSortedMap map = (ImmutableSortedMap) args[0];
                return CompatibleGuava11.navigableMap(map);
              }
              if (method.getName().equals("immutableNavigableMap")) {
                Map map = (Map) args[0];
                ImmutableSortedMap sortedMap = ImmutableSortedMap.copyOf(map);
                return CompatibleGuava11.navigableMap(sortedMap);
              }
              if (method.getName().equals("setSchema")) {
                Connection connection = (Connection) args[0];
                String schema = (String) args[1];
                final Method method1 =
                    connection.getClass().getMethod("setSchema", String.class);
                return method1.invoke(connection, schema);
              }
              if (method.getName().equals("getParameterName")) {
                final Method m = (Method) args[0];
                final int i = (Integer) args[1];
                try {
                  final Method method1 =
                      m.getClass().getMethod("getParameters");
                  Object parameters = method1.invoke(m);
                  final Object parameter = Array.get(parameters, i);
                  final Method method3 = parameter.getClass().getMethod("getName");
                  return method3.invoke(parameter);
                } catch (NoSuchMethodException e) {
                  return "arg" + i;
                }
              }

              if (method.getName().equals("lookupPrivate")) {
              // Use MethodHandles.privateLookupIn if it is available (JDK 9
              // and above)
              @SuppressWarnings("rawtypes")
              final Class<?> clazz = (Class) requireNonNull(args[0], "args[0]");
              try {
                final Method privateLookupMethod =
                    MethodHandles.class.getMethod("privateLookupIn",
                        Class.class, MethodHandles.Lookup.class);
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                return privateLookupMethod.invoke(null, clazz, lookup);
              } catch (NoSuchMethodException e) {
                return privateLookupJdk8(clazz);
              }
            }
            return null;
          });
    }

    /** Emulates MethodHandles.privateLookupIn on JDK 8;
     * in later JDK versions, throws. */
    @SuppressWarnings("deprecation")
    static <T> MethodHandles.Lookup privateLookupJdk8(Class<T> clazz) {
      try {
        final Constructor<MethodHandles.Lookup> constructor =
            MethodHandles.Lookup.class.getDeclaredConstructor(Class.class,
                int.class);
        if (!constructor.isAccessible()) {
          constructor.setAccessible(true);
        }
        return constructor.newInstance(clazz, MethodHandles.Lookup.PRIVATE);
      } catch (InstantiationException | IllegalAccessException
          | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
