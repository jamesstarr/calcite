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

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Internal util methods for ElasticSearch tests
 */
public class ElasticsearchChecker {

>>>>>>> 8c96cf58c ([CALCITE-2376] Unify ES2 and ES5 adapters. Migrate to low-level ES rest client as main transport. (Andrei Sereda))
  private ElasticsearchChecker() {}


  /** Returns a function that checks that a particular Elasticsearch pipeline is
   * generated to implement a query.
   * @param strings expected expressions
   * @return validation function
   */
  public static Consumer<List> elasticsearchChecker(final String... strings) {
    Objects.requireNonNull(strings, "strings");
<<<<<<< HEAD
    return a -> {
      ObjectNode actual = a == null || a.isEmpty() ? null
            : ((ObjectNode) a.get(0));

      actual = expandDots(actual);
      try {

        String json = "{" + Arrays.stream(strings).collect(Collectors.joining(",")) + "}";
        ObjectNode expected = (ObjectNode) MAPPER.readTree(json);
        expected = expandDots(expected);

        if (!expected.equals(actual)) {
          assertEquals(MAPPER.writeValueAsString(expected), MAPPER.writeValueAsString(actual),
              "expected and actual Elasticsearch queries do not match");
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /**
   * Expands attributes with dots ({@code .}) into sub-nodes.
   * Use for more friendly JSON format:
   *
   * <pre>
   *   {'a.b.c': 1}
   *   expanded to
   *   {a: { b: {c: 1}}}}
   * </pre>
   * @param parent current node
   * @param <T> type of node (usually JsonNode).
   * @return copy of existing node with field {@code a.b.c} expanded.
   */
  @SuppressWarnings("unchecked")
  private static <T extends JsonNode> T expandDots(T parent) {
    Objects.requireNonNull(parent, "parent");

    if (parent.isValueNode()) {
      return parent.deepCopy();
    }

    // ArrayNode
    if (parent.isArray()) {
      ArrayNode arr = (ArrayNode) parent;
      ArrayNode copy = arr.arrayNode();
      arr.elements().forEachRemaining(e -> copy.add(expandDots(e)));
      return (T) copy;
    }

    // ObjectNode
    ObjectNode objectNode = (ObjectNode) parent;
    final ObjectNode copy = objectNode.objectNode();
    objectNode.fields().forEachRemaining(e -> {
      final String property = e.getKey();
      final JsonNode node = e.getValue();

      final String[] names = property.split("\\.");
      ObjectNode copy2 = copy;
      for (int i = 0; i < names.length - 1; i++) {
        copy2 = copy2.with(names[i]);
      }
      copy2.set(names[names.length - 1], expandDots(node));
    });

    return (T) copy;
  }

}
=======
    return actual -> {
      Object[] actualArray = actual == null || actual.isEmpty() ? null
            : ((List) actual.get(0)).toArray();
      CalciteAssert.assertArrayEqual("expected Elasticsearch query not found", strings,
            actualArray);
    };
  }
}

// End ElasticsearchChecker.java
