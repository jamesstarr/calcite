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

import com.google.common.collect.Table;

import org.apache.calcite.rel.RelNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CachingMetadataProvider implements NWOMetadataProvider{
  private final NWOMetadataProvider baseProvider;

  public CachingMetadataProvider(NWOMetadataProvider baseProvider) {
    this.baseProvider = baseProvider;
  }

  @Override public <RESULT, ARGUMENTS extends MetadataArguments>
  MetadataCallSite<RESULT, ARGUMENTS> callSite(MetadataType<RESULT, ARGUMENTS> metadataType) {
    MetadataCallSite<RESULT, ARGUMENTS> baseSite = baseProvider.callSite(metadataType);
    return  new MetadataCallSite<RESULT, ARGUMENTS>() {

      @Override
      public MetadataType<RESULT, ARGUMENTS> metadataType() {
        return metadataType;
      }

      @Override
      public RESULT extract(ARGUMENTS arg) {
        Table<RelNode, Object, Object> myMap = arg.relNode.getCluster().getMetadataQuery().map();
        @Nullable Object v = arg.relNode.getCluster().getMetadataQuery().map()
            .get(arg.relNode, arg);
        if (v != null) {
          if (v == org.apache.calcite.rel.metadata.NullSentinel.ACTIVE) {
            throw new org.apache.calcite.rel.metadata.CyclicMetadataException();
          } else if (v == org.apache.calcite.rel.metadata.NullSentinel.INSTANCE) {
            return null;
          } else {
            return (RESULT) v;
          }
        }
        myMap.put(arg.relNode, arg, org.apache.calcite.rel.metadata.NullSentinel.ACTIVE);
        try {
          RESULT r = baseSite.extract(arg);
          myMap.put(arg.relNode, arg, org.apache.calcite.rel.metadata.NullSentinel.mask(r));
          return r;
        } catch (java.lang.Exception e) {
          myMap.row(arg.relNode).clear();
          throw e;
        }
      }
    };
  }
}
