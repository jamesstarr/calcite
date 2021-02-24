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
package org.apache.calcite.rel.metadata.nwo;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple Registry base metadata provider.
 */
public class RegistryMetadataProvider implements NWOMetadataProvider {

  final Map<MetadataType<?, ?>, MetadataCallSite<?, ?>> registry;

  public RegistryMetadataProvider() {
    this.registry = new HashMap<>();
  }

  /**
   * Create call site for fetching metadata.
   * @param metadataType type of Metadata
   * @param <RESULT> metadata
   * @param <ARGUMENTS> arguments for generating metadata
   * @return the call site
   */
  @Override public <RESULT, ARGUMENTS extends MetadataArguments>
  MetadataCallSite<RESULT, ARGUMENTS> callSite(MetadataType<RESULT, ARGUMENTS> metadataType) {
    return (MetadataCallSite<RESULT, ARGUMENTS>) registry.get(metadataType);
  }

  public <RESULT, ARGUMENTS extends MetadataArguments>
  void register(MetadataCallSite<RESULT, ARGUMENTS> callsite) {
    registry.put(callsite.metadataType(), callsite);
  }
}
