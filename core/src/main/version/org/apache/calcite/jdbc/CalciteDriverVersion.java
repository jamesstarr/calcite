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
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.DriverVersion;

/**
 * Provides information on the current Calcite version.
 */
>>>>>>> 2619f909b ([CALCITE-2905] Use code generation to create DriverVersion)
class CalciteDriverVersion {
  static final DriverVersion INSTANCE =
      new DriverVersion(
          "Calcite JDBC Driver",
          "1.0.0-unprocessed-SNAPSHOT" /* :version */,
          "Calcite",
          "1.0.0-unprocessed-SNAPSHOT" /* :version */,
          true,
          1 /* :major */,
          0 /* :minor */,
          1 /* :major */,
          0 /* :minor */);

  private CalciteDriverVersion() {
  }
<<<<<<< HEAD
=======
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PrimitiveTest.class,
    Linq4jTest.class,
    ExpressionTest.class,
    OptimizerTest.class,
    InlinerTest.class,
    LookupImplTest.class,
    DeterministicTest.class,
    BlockBuilderTest.class,
    FunctionTest.class,
    TypeTest.class,
    CorrelateJoinTest.class,
    JoinPreserveOrderTest.class
    })
public class Linq4jSuite {
=======
>>>>>>> 2619f909b ([CALCITE-2905] Use code generation to create DriverVersion)
}

// End CalciteDriverVersion.java
