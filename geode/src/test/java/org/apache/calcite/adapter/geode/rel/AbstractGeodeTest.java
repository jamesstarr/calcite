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
<<<<<<<< HEAD:file/src/test/resources/sales-json.json
 *
 * A JSON model of a Calcite schema based on CSV files.
 */
{
  "version": "1.0",
  "defaultSchema": "SALES",
  "schemas": [
    {
      "name": "SALES",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "sales-json"
      }
    }
  ]
========
 */
package org.apache.calcite.adapter.geode.rel;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Base class that allows sharing same geode instance across all tests.
 *
 * <p>Also, due to legacy reasons, there can't be more than one Geode
 * instance (running in parallel) for a single JVM.
 */
@Execution(ExecutionMode.CONCURRENT)
public abstract class AbstractGeodeTest {

  @RegisterExtension
  public static final GeodeEmbeddedPolicy POLICY = GeodeEmbeddedPolicy.create().share();

>>>>>>>> 1b41ebada ([CALCITE-2588] Run Geode Adapter tests with an Embedded Instance):geode/src/test/java/org/apache/calcite/adapter/geode/rel/AbstractGeodeTest.java
}
