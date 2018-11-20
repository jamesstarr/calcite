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
package org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Stores Elasticsearch
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html">
 * mapping</a> information for particular index/type. This information is
 * extracted from {@code /$index/$type/_mapping} endpoint.
 *
 * <p>Instances of this class are immutable.
 */
class ElasticsearchMapping {

  private final String index;

  private final String type;

  private final Map<String, Datatype> mapping;

  ElasticsearchMapping(final String index, final String type,
      final Map<String, String> mapping) {
    this.index = Objects.requireNonNull(index, "index");
    this.type = Objects.requireNonNull(type, "type");
    Objects.requireNonNull(mapping, "mapping");

    final Map<String, Datatype> transformed = mapping.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new Datatype(e.getValue())));
    this.mapping = ImmutableMap.copyOf(transformed);
  }

  /**
   * Returns ES schema for each field. Mapping is represented as field name
   * {@code foo.bar.qux} and type ({@code keyword}, {@code boolean},
   * {@code long}).
   *
   * @return immutable mapping between field and ES type
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html">Mapping Types</a>
   */
  Map<String, Datatype> mapping() {
    return this.mapping;
  }

  /**
   * Used as special aggregation key for missing values (documents that are
   * missing a field).
   *
   * <p>Buckets with that value are then converted to {@code null}s in flat
   * tabular format.
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html">Missing Value</a>
   */
  Optional<JsonNode> missingValueFor(String fieldName) {
    if (!mapping().containsKey(fieldName)) {
      final String message = String.format(Locale.ROOT,
          "Field %s not defined for %s/%s", fieldName, index, type);
      throw new IllegalArgumentException(message);
    }

    return mapping().get(fieldName).missingValue();
  }

  String index() {
    return this.index;
  }


// End ElasticsearchMapping.java
