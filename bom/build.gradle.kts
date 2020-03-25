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
plugins {
    `java-platform`
}

val String.v: String get() = rootProject.extra["$this.version"] as String

// Note: Gradle allows to declare dependency on "bom" as "api",
// and it makes the contraints to be transitively visible
// However Maven can't express that, so the approach is to use Gradle resolution
// and generate pom files with resolved versions
// See https://github.com/gradle/gradle/issues/9866

fun DependencyConstraintHandlerScope.apiv(
    notation: String,
    versionProp: String = notation.substringAfterLast(':')
) =
    "api"(notation + ":" + versionProp.v)

fun DependencyConstraintHandlerScope.runtimev(
    notation: String,
    versionProp: String = notation.substringAfterLast(':')
) =
    "runtime"(notation + ":" + versionProp.v)

dependencies {
    // Parenthesis are needed here: https://github.com/gradle/gradle/issues/9248
    (constraints) {
        // api means "the dependency is for both compilation and runtime"
        // runtime means "the dependency is only for runtime, not for compilation"
        // In other words, marking dependency as "runtime" would avoid accidental
        // dependency on it during compilation
        apiv("com.beust:jcommander")
        apiv("com.datastax.cassandra:cassandra-driver-core")
        apiv("com.esri.geometry:esri-geometry-api")
        apiv("com.fasterxml.jackson.core:jackson-annotations", "jackson")
        apiv("com.fasterxml.jackson.core:jackson-core", "jackson")
        apiv("com.fasterxml.jackson.core:jackson-databind")
        apiv("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml", "jackson")
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 278d51c33 ([CALCITE-3510] Implement Redis adapter  (#170))
        apiv("com.github.kstyrc:embedded-redis")
        apiv("com.github.stephenc.jcip:jcip-annotations")
        apiv("com.google.code.findbugs:jsr305", "findbugs.jsr305")
        apiv("com.google.guava:guava")
        api("com.google.errorprone:error_prone_annotations:2.28.0")
        api("org.checkerframework:checker-qual:3.10.0")
<<<<<<< HEAD
=======
        apiv("com.github.stephenc.jcip:jcip-annotations")
        apiv("com.google.code.findbugs:jsr305", "findbugs.jsr305")
        apiv("com.google.guava:guava")
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
        apiv("com.google.protobuf:protobuf-java", "protobuf")
        apiv("com.h2database:h2")
        apiv("com.jayway.jsonpath:json-path")
        apiv("com.joestelmach:natty")
        apiv("com.oracle.ojdbc:ojdbc8")
        apiv("com.teradata.tpcds:tpcds", "teradata.tpcds")
        apiv("com.yahoo.datasketches:sketches-core")
        apiv("commons-codec:commons-codec")
        apiv("commons-io:commons-io")
        apiv("de.bwaldvogel:mongo-java-server", "mongo-java-server")
        apiv("de.bwaldvogel:mongo-java-server-core", "mongo-java-server")
        apiv("de.bwaldvogel:mongo-java-server-memory-backend", "mongo-java-server")
        apiv("io.airlift.tpch:tpch")
        apiv("javax.servlet:javax.servlet-api", "servlet")
        apiv("joda-time:joda-time")
        apiv("junit:junit", "junit4")
<<<<<<< HEAD
<<<<<<< HEAD
        apiv("log4j:log4j", "log4j")
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
        apiv("log4j:log4j", "log4j")
>>>>>>> 2833961fa (Refine logging dependencies: keep slf4j bridges in runtime classpath … (#179))
        apiv("mysql:mysql-connector-java")
        apiv("net.hydromatic:aggdesigner-algorithm")
        apiv("net.hydromatic:chinook-data-hsqldb")
        apiv("net.hydromatic:foodmart-data-hsqldb")
        apiv("net.hydromatic:foodmart-data-json")
        apiv("net.hydromatic:foodmart-queries")
        apiv("net.hydromatic:quidem")
        apiv("net.hydromatic:scott-data-hsqldb")
        apiv("net.hydromatic:tpcds", "hydromatic.tpcds")
<<<<<<< HEAD
<<<<<<< HEAD
        apiv("net.java.dev.jna:jna")
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
        apiv("net.java.dev.jna:jna")
>>>>>>> 54dc4f56f ([CALCITE-3516] Bump net.java.dev.jna:jna to 5.5.0 (#131))
        apiv("net.sf.opencsv:opencsv")
        apiv("org.apache.calcite.avatica:avatica-core", "calcite.avatica")
        apiv("org.apache.calcite.avatica:avatica-server", "calcite.avatica")
        apiv("org.apache.cassandra:cassandra-all")
        apiv("org.apache.commons:commons-dbcp2")
        apiv("org.apache.commons:commons-lang3")
        apiv("org.apache.geode:geode-core")
        apiv("org.apache.hadoop:hadoop-client", "hadoop")
        apiv("org.apache.hadoop:hadoop-common", "hadoop")
        apiv("org.apache.httpcomponents:httpclient")
        apiv("org.apache.httpcomponents:httpcore")
        apiv("org.apache.kafka:kafka-clients")
        apiv("org.apache.kerby:kerb-client", "kerby")
        apiv("org.apache.kerby:kerb-core", "kerby")
        apiv("org.apache.kerby:kerb-simplekdc", "kerby")
<<<<<<< HEAD
<<<<<<< HEAD
        apiv("org.apache.logging.log4j:log4j-api", "log4j2")
        apiv("org.apache.logging.log4j:log4j-core", "log4j2")
        apiv("org.apache.logging.log4j:log4j-slf4j-impl", "log4j2")
        apiv("org.apache.pig:pig")
        apiv("org.apache.pig:pigunit", "pig")
        apiv("org.apache.spark:spark-core_2.10", "spark")
<<<<<<< HEAD
        apiv("org.apiguardian:apiguardian-api")
=======
        apiv("org.apache.logging.log4j:log4j-api", "log4j")
        apiv("org.apache.logging.log4j:log4j-core", "log4j")
=======
        apiv("org.apache.logging.log4j:log4j-api", "log4j2")
        apiv("org.apache.logging.log4j:log4j-core", "log4j2")
        apiv("org.apache.logging.log4j:log4j-slf4j-impl", "log4j2")
>>>>>>> 2833961fa (Refine logging dependencies: keep slf4j bridges in runtime classpath … (#179))
        apiv("org.apache.pig:pig")
        apiv("org.apache.pig:pigunit", "pig")
        apiv("org.apache.spark:spark-core_2.10", "spark")
<<<<<<< HEAD
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
        apiv("org.apiguardian:apiguardian-api")
>>>>>>> 20c78195c ([CALCITE-3652] Add org.apiguardian:apiguardian-api to specify API status (#165))
=======
>>>>>>> d4dded518 ([CALCITE-3871] Remove dependency of org.apiguardian:apiguardian-api)
        apiv("org.bouncycastle:bcpkix-jdk15on", "bouncycastle")
        apiv("org.bouncycastle:bcprov-jdk15on", "bouncycastle")
        apiv("org.cassandraunit:cassandra-unit")
        apiv("org.codehaus.janino:commons-compiler", "janino")
        apiv("org.codehaus.janino:janino")
        apiv("org.codelibs.elasticsearch.module:lang-painless", "elasticsearch")
        apiv("org.eclipse.jetty:jetty-http", "jetty")
        apiv("org.eclipse.jetty:jetty-security", "jetty")
        apiv("org.eclipse.jetty:jetty-server", "jetty")
        apiv("org.eclipse.jetty:jetty-util", "jetty")
        apiv("org.elasticsearch.client:elasticsearch-rest-client", "elasticsearch")
        apiv("org.elasticsearch.plugin:transport-netty4-client", "elasticsearch")
        apiv("org.elasticsearch:elasticsearch")
        apiv("org.exparity:hamcrest-date")
        apiv("org.hamcrest:hamcrest")
        apiv("org.hamcrest:hamcrest-core", "hamcrest")
        apiv("org.hamcrest:hamcrest-library", "hamcrest")
        apiv("org.hsqldb:hsqldb")
        apiv("org.incava:java-diff")
        apiv("org.jsoup:jsoup")
        apiv("org.junit.jupiter:junit-jupiter-api", "junit5")
        apiv("org.junit.jupiter:junit-jupiter-params", "junit5")
        apiv("org.mockito:mockito-core", "mockito")
        apiv("org.mongodb:mongo-java-driver")
        apiv("org.ow2.asm:asm")
        apiv("org.ow2.asm:asm-all", "asm")
        apiv("org.ow2.asm:asm-analysis", "asm")
        apiv("org.ow2.asm:asm-commons", "asm")
        apiv("org.ow2.asm:asm-tree", "asm")
        apiv("org.ow2.asm:asm-util", "asm")
        apiv("org.postgresql:postgresql")
        apiv("org.scala-lang:scala-library")
        apiv("org.slf4j:slf4j-api", "slf4j")
        apiv("org.slf4j:slf4j-log4j12", "slf4j")
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 278d51c33 ([CALCITE-3510] Implement Redis adapter  (#170))
        apiv("redis.clients:jedis")
        apiv("sqlline:sqlline")
        runtimev("org.junit.jupiter:junit-jupiter-engine", "junit5")
        runtimev("org.junit.vintage:junit-vintage-engine", "junit5")
        apiv("org.openjdk.jmh:jmh-core", "jmh")
        apiv("org.openjdk.jmh:jmh-generator-annprocess", "jmh")
=======
        apiv("sqlline:sqlline")
        runtimev("org.junit.jupiter:junit-jupiter-engine", "junit5")
        runtimev("org.junit.vintage:junit-vintage-engine", "junit5")
<<<<<<< HEAD
        runtimev("org.openjdk.jmh:jmh-core", "jmh")
        runtimev("org.openjdk.jmh:jmh-generator-annprocess", "jmh")
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
        apiv("org.openjdk.jmh:jmh-core", "jmh")
        apiv("org.openjdk.jmh:jmh-generator-annprocess", "jmh")
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
        runtimev("xalan:xalan")
        runtimev("xerces:xercesImpl")
    }
}
