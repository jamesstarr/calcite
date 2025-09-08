/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
<<<<<<< HEAD
<<<<<<< HEAD
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
=======
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
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
<<<<<<< HEAD
<<<<<<< HEAD
 */
=======
 *
 */

>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
 */
>>>>>>> 8900eaa6c ([CALCITE-3623] Run autostyleApply)
import com.github.vlsi.gradle.properties.dsl.props

plugins {
    id("com.github.vlsi.gradle-extensions")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))

    api("com.fasterxml.jackson.core:jackson-annotations")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.code.findbugs:jsr305")
    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.apache.httpcomponents:httpclient")
    implementation("org.apache.httpcomponents:httpcore")
    implementation("org.elasticsearch.client:elasticsearch-rest-client")
    implementation("org.slf4j:slf4j-api")

    // https://github.com/elastic/elasticsearch/issues/49218
    if (project.props.bool("elasticStrictAsm", default = true)) {
        val asm = Action<DependencyConstraint> {
            version { strictly("5.1") }
        }
        constraints {
            testRuntimeOnly("org.ow2.asm:asm", asm)
            testRuntimeOnly("org.ow2.asm:asm-all", asm)
            testRuntimeOnly("org.ow2.asm:asm-debug-all", asm)
            testRuntimeOnly("org.ow2.asm:asm-analysis", asm)
            testRuntimeOnly("org.ow2.asm:asm-commons", asm)
            testRuntimeOnly("org.ow2.asm:asm-tree", asm)
            testRuntimeOnly("org.ow2.asm:asm-util", asm)
        }
    }

    testImplementation("org.apache.logging.log4j:log4j-api")
    testImplementation("org.apache.logging.log4j:log4j-core")
    testImplementation("org.codelibs.elasticsearch.module:lang-painless")
    testImplementation("org.elasticsearch.plugin:transport-netty4-client")
    testImplementation("org.elasticsearch:elasticsearch")
    testImplementation(project(":core", "testClasses"))
<<<<<<< HEAD
<<<<<<< HEAD
    testRuntimeOnly("net.java.dev.jna:jna")
<<<<<<< HEAD
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
    testRuntimeOnly("net.java.dev.jna:jna")
>>>>>>> 54dc4f56f ([CALCITE-3516] Bump net.java.dev.jna:jna to 5.5.0 (#131))
=======
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
>>>>>>> 9a0aad092 ([CALCITE-3654] Use single Elasticsearch instance for all elasticsearch tests (#167))
}
