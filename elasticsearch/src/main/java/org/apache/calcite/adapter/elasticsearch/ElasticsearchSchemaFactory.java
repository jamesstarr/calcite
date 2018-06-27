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

import com.google.common.base.Preconditions;

import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory that creates an {@link ElasticsearchSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class ElasticsearchSchemaFactory implements SchemaFactory {

>>>>>>> 8c96cf58c ([CALCITE-2376] Unify ES2 and ES5 adapters. Migrate to low-level ES rest client as main transport. (Andrei Sereda))
  public ElasticsearchSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    final Map map = (Map) operand;

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    try {
<<<<<<< HEAD

      List<HttpHost> hosts;

      if (map.containsKey("hosts")) {
        final List<String> configHosts = mapper.readValue((String) map.get("hosts"),
                new TypeReference<List<String>>() { });

        hosts = configHosts
                .stream()
                .map(host -> HttpHost.create(host))
                .collect(Collectors.toList());
      } else if (map.containsKey("coordinates")) {
        final Map<String, Integer> coordinates = mapper.readValue((String) map.get("coordinates"),
                new TypeReference<Map<String, Integer>>() { });

        hosts =  coordinates
                .entrySet()
                .stream()
                .map(entry -> new HttpHost(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        LOGGER.warn("Prefer using hosts, coordinates is deprecated.");
      } else {
        throw new IllegalArgumentException
        ("Both 'coordinates' and 'hosts' is missing in configuration. Provide one of them.");
      }
      final String pathPrefix = (String) map.get("pathPrefix");
      // create client
      final RestClient client = connect(hosts, pathPrefix);
      final String index = (String) map.get("index");

=======
      final Map<String, Integer> coordinates =
          mapper.readValue((String) map.get("coordinates"),
              new TypeReference<Map<String, Integer>>() { });

      final RestClient client = connect(coordinates);

      final Map<String, String> userConfig =
          mapper.readValue((String) map.get("userConfig"),
              new TypeReference<Map<String, String>>() { });

      final String index = (String) map.get("index");
      Preconditions.checkArgument(index != null, "index is missing in configuration");
      return new ElasticsearchSchema(client, new ObjectMapper(), index);
    } catch (IOException e) {
      throw new RuntimeException("Cannot parse values from json", e);
    }
  }

  /**
   * Builds elastic rest client from user configuration
   * @param coordinates list of {@code hostname/port} to connect to
   * @return newly initialized low-level rest http client for ES
   */
  private static RestClient connect(Map<String, Integer> coordinates) {
    Objects.requireNonNull(coordinates, "coordinates");
    Preconditions.checkArgument(!coordinates.isEmpty(), "no ES coordinates specified");
    final Set<HttpHost> set = new LinkedHashSet<>();
    for (Map.Entry<String, Integer> entry: coordinates.entrySet()) {
      set.add(new HttpHost(entry.getKey(), entry.getValue()));
    }

    return RestClient.builder(set.toArray(new HttpHost[0])).build();
  }

}

// End ElasticsearchSchemaFactory.java
