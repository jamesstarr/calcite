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
package org.apache.calcite.rel.metadata.janino;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DelegatingMetadataRel;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.argList;
import static org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.generateParamList;

/**
 * Generates the metadata dispatch.
 */
public class DispatchGenerator {
  private final Map<MetadataHandler<?>, String> metadataHandlerToName;

  public DispatchGenerator(Map<MetadataHandler<?>, String> metadataHandlerToName) {
    this.metadataHandlerToName = metadataHandlerToName;
  }

  public void generateDispatchMethod(StringBuilder sb, Method method,
      Collection<? extends MetadataHandler<?>> metadataHandlers) {
    String delRelClass = DelegatingMetadataRel.class.getName();
    Map<MetadataHandler<?>, Set<Class<? extends RelNode>>> handlersToClasses =
        metadataHandlers.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    mh -> methodAndInstanceToImplementingClass(method, mh)));

    Set<Class<? extends RelNode>> delegateClassSet = handlersToClasses.values().stream()
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
    List<Class<? extends RelNode>> delegateClassList = topologicalSort(delegateClassSet);
    sb
        .append("  private ")
        .append(method.getReturnType().getName())
        .append(" ")
        .append(method.getName())
        .append("_(\n")
        .append("      ")
        .append(RelNode.class.getName())
        .append(" r,\n")
        .append("      ")
        .append(RelMetadataQuery.class.getName())
        .append(" mq");
    generateParamList(sb, method)
        .append(") {\n");
    sb
        .append("    while (r instanceof ").append(delRelClass).append(") {\n")
        .append("      r = ((").append(delRelClass).append(") r).getCurrentRel();\n")
        .append("    }\n");

    sb
        .append(
            delegateClassList.stream()
                .map(clazz ->
                    generateIfInstanceDispatch(method, metadataHandlers, handlersToClasses, clazz))
                .collect(
                    Collectors.joining("    } else if ", "    if ", "    } else {\n")))
        .append("      throw new ")
        .append(IllegalArgumentException.class.getName())
        .append("(\"No handler for method [").append(method)
        .append("] applied to argument of type [\" + r.getClass() + ")
        .append("\"]; we recommend you create a catch-all (RelNode) handler\"")
        .append(");\n")
        .append("    }\n")
        .append("  }\n");
  }

  private CharSequence generateIfInstanceDispatch(Method method,
      Collection<? extends MetadataHandler<?>> metadataHandlers,
      Map<MetadataHandler<?>, Set<Class<? extends RelNode>>> handlersToClasses,

      Class<? extends RelNode> clazz) {
    String handlerName = findProvider(metadataHandlers, handlersToClasses, clazz);
    StringBuilder sb = new StringBuilder()
        .append("(r instanceof ").append(clazz.getName()).append(") {\n")
        .append("      return ");
    generateDispatchedCall(sb, handlerName, method, clazz);

    return sb;
  }

  private void generateDispatchedCall(StringBuilder sb, String handlerName, Method method,
      Class<? extends RelNode> clazz) {
    sb.append(handlerName).append(".").append(method.getName())
        .append("((").append(clazz.getName()).append(") r, mq");
    argList(sb, method);
    sb.append(");\n");
  }


  private static Set<Class<? extends RelNode>> methodAndInstanceToImplementingClass(
      Method method, MetadataHandler<?> handler) {
    Set<Class<? extends RelNode>> set = new HashSet<>();
    for (Method m : handler.getClass().getMethods()) {
      Class<? extends RelNode> aClass = toRelClass(method, m);
      if (aClass != null) {
        set.add(aClass);
      }
    }
    return set;
  }

  private static @Nullable Class<? extends RelNode> toRelClass(Method superMethod,
      Method candidate) {
    if (!superMethod.getName().equals(candidate.getName())) {
      return null;
    } else if (superMethod.getParameterCount() + 2 != candidate.getParameterCount()) {
      return null;
    } else {
      Class<?>[] cpt = candidate.getParameterTypes();
      Class<?>[] smpt = superMethod.getParameterTypes();
      if (!RelNode.class.isAssignableFrom(cpt[0])) {
        return null;
      } else if (!RelMetadataQuery.class.equals(cpt[1])) {
        return null;
      }
      for (int i = 0; i < smpt.length; i++) {
        if (cpt[i + 2] != smpt[i]) {
          return null;
        }
      }
      return (Class<? extends RelNode>) cpt[0];
    }
  }


  private static List<Class<? extends RelNode>> topologicalSort(
      Collection<Class<? extends RelNode>> list) {
    //This is currently N squared, wikipedia say it could be better
    List<Class<? extends RelNode>> l = new ArrayList<>();
    ArrayDeque<Class<? extends RelNode>> s = new ArrayDeque<>(list);

    while (!s.isEmpty()) {
      Class<? extends RelNode> n = s.remove();

      boolean found = false;
      for (Class<? extends RelNode> other : s) {
        if (n.isAssignableFrom(other)) {
          found = true;
          break;
        }
      }
      if (found) {
        s.add(n);
      } else {
        l.add(n);
      }
    }
    return l;
  }

  private String findProvider(Collection<? extends MetadataHandler<?>> metadataHandlers,
      Map<MetadataHandler<?>, Set<Class<? extends RelNode>>> handlerToClasses,
      Class<? extends RelNode> clazz) {
    for (MetadataHandler<?> mh : metadataHandlers) {
      if (handlerToClasses.getOrDefault(mh, ImmutableSet.of()).contains(clazz)) {
        return castNonNull(this.metadataHandlerToName.get(mh));
      }
    }
    throw new RuntimeException();
  }
}
