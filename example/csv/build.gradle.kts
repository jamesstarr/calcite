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
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
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
val sqllineClasspath by configurations.creating {
    isCanBeConsumed = false
    extendsFrom(configurations.testRuntimeClasspath.get())
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
    api("org.checkerframework:checker-qual")

    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.guava:guava")
    implementation("commons-io:commons-io")
    implementation("net.sf.opencsv:opencsv")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.apache.commons:commons-lang3")

    testImplementation("sqlline:sqlline")
    testImplementation(project(":core", "testClasses"))

    sqllineClasspath(project(":example:csv", "testClasses"))
=======
    sqllineClasspath(project)
    sqllineClasspath(files(sourceSets.test.map { it.output }))
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
}

val buildSqllineClasspath by tasks.registering(Jar::class) {
    inputs.files(sqllineClasspath).withNormalizer(ClasspathNormalizer::class.java)
    archiveFileName.set("sqllineClasspath.jar")
    manifest {
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
        attributes(
            "Main-Class" to "sqlline.SqlLine",
            "Class-Path" to provider {
                // Class-Path is a list of URLs
                sqllineClasspath.joinToString(" ") {
                    it.toURI().toURL().toString()
                }
            }
        )
<<<<<<< HEAD
=======
        manifest {
            attributes(
                "Main-Class" to "sqlline.SqlLine",
                "Class-Path" to sqllineClasspath.map { it.absolutePath }.joinToString(" ")
            )
        }
=======
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
    }
}
