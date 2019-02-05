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

      final Map<String, Integer> coordinates =
          mapper.readValue((String) map.get("coordinates"),
              new TypeReference<Map<String, Integer>>() { });
=======
      final String coordinatesString = (String) map.get("coordinates");
      Preconditions.checkState(coordinatesString != null,
          "'coordinates' is missing in configuration");

      final Map<String, Integer> coordinates = mapper.readValue(coordinatesString,
          new TypeReference<Map<String, Integer>>() { });

      // create client
      final RestClient client = connect(coordinates);

      final String index = (String) map.get("index");
      Preconditions.checkState(index != null, "'index' is missing in configuration");
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
