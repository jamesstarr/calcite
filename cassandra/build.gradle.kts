/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
=======
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
>>>>>>> 8900eaa6c ([CALCITE-3623] Run autostyleApply)
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

=======
 */
>>>>>>> 8900eaa6c ([CALCITE-3623] Run autostyleApply)
dependencies {
    api(project(":core"))
    api(project(":linq4j"))

    implementation("com.datastax.cassandra:cassandra-driver-core")
    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.slf4j:slf4j-api")

    testImplementation(project(":core", "testClasses"))
    testImplementation("org.apache.cassandra:cassandra-all") {
        exclude("org.slf4j", "log4j-over-slf4j")
            .because("log4j is already present in the classpath")
    }
    testImplementation("org.cassandraunit:cassandra-unit")
    testImplementation("com.github.stephenc.jcip:jcip-annotations")
=======
    testRuntimeOnly("net.java.dev.jna:jna")
>>>>>>> 54dc4f56f ([CALCITE-3516] Bump net.java.dev.jna:jna to 5.5.0 (#131))
}
