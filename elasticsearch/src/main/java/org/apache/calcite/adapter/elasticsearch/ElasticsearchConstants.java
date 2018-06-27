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
  String SOURCE = SOURCE_GROOVY;
  String ID = "_id";
  String UID = "_uid";

  /* Aggregation pushdown operations supported */
  String AGG_SUM = "SUM";
  String AGG_SUM0 = "$SUM0";
  String AGG_COUNT = "COUNT";
  String AGG_MIN = "MIN";
  String AGG_MAX = "MAX";
  String AGG_AVG = "AVG";

  Set<String> META_COLUMNS = ImmutableSet.of(UID, ID, TYPE, INDEX);

}

// End ElasticsearchConstants.java
>>>>>>>> 8c96cf58c ([CALCITE-2376] Unify ES2 and ES5 adapters. Migrate to low-level ES rest client as main transport. (Andrei Sereda)):elasticsearch/src/main/java/org/apache/calcite/adapter/elasticsearch/ElasticsearchConstants.java
