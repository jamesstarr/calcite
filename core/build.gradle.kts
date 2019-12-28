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
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings

plugins {
    kotlin("jvm")
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.ide")
    calcite.fmpp
    calcite.javacc
}

val integrationTestConfig: (Configuration.() -> Unit) = {
    isCanBeConsumed = false
    isTransitive = true
    extendsFrom(configurations.testRuntimeClasspath.get())
}

val testH2 by configurations.creating(integrationTestConfig)
val testOracle by configurations.creating(integrationTestConfig)
val testPostgresql by configurations.creating(integrationTestConfig)
val testMysql by configurations.creating(integrationTestConfig)

dependencies {
    api(project(":linq4j"))

    api("com.fasterxml.jackson.core:jackson-annotations")
    api("org.apache.calcite.avatica:avatica-core")
<<<<<<< HEAD
    api("org.apiguardian:apiguardian-api")
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)

    implementation("com.esri.geometry:esri-geometry-api")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.google.code.findbugs:jsr305"/* optional*/)
<<<<<<< HEAD
<<<<<<< HEAD
    implementation("com.google.errorprone:error_prone_annotations")
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
    implementation("com.google.errorprone:error_prone_annotations")
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
    implementation("com.google.guava:guava")
    implementation("com.jayway.jsonpath:json-path")
    implementation("com.yahoo.datasketches:sketches-core")
    implementation("commons-codec:commons-codec")
    implementation("net.hydromatic:aggdesigner-algorithm")
<<<<<<< HEAD
<<<<<<< HEAD
    implementation("org.apache.commons:commons-dbcp2")
    implementation("org.apache.commons:commons-lang3")
    implementation("commons-io:commons-io")
<<<<<<< HEAD
=======
    implementation("org.apache.calcite.avatica:avatica-server")
=======
>>>>>>> 87623995c ([CALCITE-3888] Switch avatica-server to be test dependency for core)
    implementation("org.apache.commons:commons-dbcp2")
    implementation("org.apache.commons:commons-lang3")
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
>>>>>>> ff50dd02a ([CALCITE-3560] Additional calcite.util.Source implementation for gene… (#125))
    implementation("org.codehaus.janino:commons-compiler")
    implementation("org.codehaus.janino:janino")
    implementation("org.slf4j:slf4j-api")

    testH2("com.h2database:h2")
    testMysql("mysql:mysql-connector-java")
    testOracle("com.oracle.ojdbc:ojdbc8")
    testPostgresql("org.postgresql:postgresql")

<<<<<<< HEAD
=======
    testImplementation("com.github.stephenc.jcip:jcip-annotations")
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
    testImplementation("net.hydromatic:foodmart-data-hsqldb")
    testImplementation("net.hydromatic:foodmart-queries")
    testImplementation("net.hydromatic:quidem")
    testImplementation("net.hydromatic:scott-data-hsqldb")
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87623995c ([CALCITE-3888] Switch avatica-server to be test dependency for core)
    testImplementation("org.apache.calcite.avatica:avatica-server")
    testImplementation("org.apache.commons:commons-pool2")
    testImplementation("log4j:log4j") {
        because("SqlHintsConverterTest needs to implement a MockAppender")
    }
=======
    testImplementation("org.apache.commons:commons-pool2")
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
    testImplementation("org.hsqldb:hsqldb")
    testImplementation("org.incava:java-diff")
    testImplementation("sqlline:sqlline")
    testImplementation(kotlin("stdlib-jdk8"))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3ab086683 ([CALCITE-2457] Migrate core parameterized to JUnit5, and make core junit5-only)
    testImplementation(kotlin("test-junit5"))
    testImplementation("org.slf4j:slf4j-log4j12")
}

// There are users that reuse/extend test code (e.g. Apache Felix)
// So publish test jar to Nexus repository
// TODO: remove when calcite-test-framework is extracted to a standalone artifact
publishing {
    publications {
        named<MavenPublication>(project.name) {
            artifact(tasks.testJar.get())
        }
    }
}

=======
    testImplementation(kotlin("test"))
=======
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
    testImplementation(kotlin("test-junit"))
    testRuntimeOnly("org.slf4j:slf4j-log4j12")
}

<<<<<<< HEAD
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
// There are users that reuse/extend test code (e.g. Apache Felix)
// So publish test jar to Nexus repository
// TODO: remove when calcite-test-framework is extracted to a standalone artifact
publishing {
    publications {
        named<MavenPublication>(project.name) {
            artifact(tasks.testJar.get())
        }
    }
}

>>>>>>> 9bfd54c63 ([CALCITE-2905] Publish calcite-core-...tests.jar to Nexus)
tasks.jar {
    CrLfSpec(LineEndings.LF).run {
        into("codegen") {
            textFrom("$projectDir/src/main/codegen")
        }
    }
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2619f909b ([CALCITE-2905] Use code generation to create DriverVersion)
val generatedVersionDir = File(buildDir, "generated/sources/version")
val versionClass by tasks.registering(Sync::class) {
    val re = Regex("^(\\d+)\\.(\\d+).*")

    val version = project.version.toString()
    val matchResult = re.find(version) ?: throw GradleException("Unable to parse major.minor version parts from project.version '$version'")
    val (major, minor) = matchResult.destructured

    // This makes Gradle re-execute the task when version is updated
    inputs.property("version", version)

    // Note: Gradle does not analyze regexps below, so this variable tells Gradle
    // to treat the task input out of date when filtering logic is updated.
    inputs.property("replace.logic.version.bump.when.updating.filter.below", 1)

    outputs.dir(generatedVersionDir)

    into(generatedVersionDir)
    from("$projectDir/src/main/version") {
        include("**/*.java")
        val prop = Regex("""("[^"]++"|\S+)\s+/\* :(\w+) \*/""")
        filter { x: String ->
            prop.replace(x) {
                val variableName = it.groups[2]?.value
                when (variableName) {
                    "version" -> "\"$version\""
                    "major" -> major
                    "minor" -> minor
                    else -> "unknown variable: $x"
                } + """ /* :$variableName */"""
            }
        }
    }
}

ide {
    generatedJavaSources(versionClass.get(), generatedVersionDir)
}

sourceSets {
    main {
        resources.exclude("version/org-apache-calcite-jdbc.properties")
    }
}

<<<<<<< HEAD
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
>>>>>>> 2619f909b ([CALCITE-2905] Use code generation to create DriverVersion)
tasks.withType<Checkstyle>().configureEach {
    exclude("org/apache/calcite/runtime/Resources.java")
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    config.set(file("src/main/codegen/config.fmpp"))
    templates.set(file("src/main/codegen/templates"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }.singleFile
    }
    inputFile.set(parserFile)
    packageName.set("org.apache.calcite.sql.parser.impl")
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
tasks.compileKotlin {
    dependsOn(versionClass)
    dependsOn(javaCCMain)
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
// Ensure spotless runs after version class generation and JAR tasks depend on JavaCC
afterEvaluate {
    tasks.findByName("spotlessJava")?.let { spotlessTask ->
        spotlessTask.dependsOn(versionClass)
    }

    // Ensure JAR tasks depend on JavaCC tasks
    tasks.findByName("sourcesJar")?.let { sourcesJarTask ->
        sourcesJarTask.dependsOn(javaCCMain)
    }
    tasks.findByName("testSourcesJar")?.let { testSourcesJarTask ->
        tasks.findByName("javaCCTest")?.let { javaCCTestTask ->
            testSourcesJarTask.dependsOn(javaCCTestTask)
        }
    }
}

<<<<<<< HEAD
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
=======
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
val fmppTest by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    config.set(file("src/test/codegen/config.fmpp"))
    templates.set(file("src/main/codegen/templates"))
}

val javaCCTest by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppTest)
    val parserFile = fmppTest.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }.singleFile
    }
    inputFile.set(parserFile)
    packageName.set("org.apache.calcite.sql.parser.parserextensiontesting")
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
tasks.compileTestKotlin {
    dependsOn(javaCCTest)
}

<<<<<<< HEAD
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
ide {
    fun generatedSource(javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>, sourceSet: String) =
        generatedJavaSources(javacc.get(), javacc.get().output.get().asFile, sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
    generatedSource(javaCCTest, "test")
}

val integTestAll by tasks.registering() {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Executes integration JDBC tests for all DBs"
}

val coreTestClasses = sourceSets.main.get().output
val coreClasses = sourceSets.main.get().output + coreTestClasses
for (db in listOf("h2", "mysql", "oracle", "postgresql")) {
    val task = tasks.register("integTest" + db.capitalize(), Test::class) {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Executes integration JDBC tests with $db database"
        include("org/apache/calcite/test/JdbcAdapterTest.class")
        include("org/apache/calcite/test/JdbcTest.class")
        systemProperty("calcite.test.db", db)
        testClassesDirs = coreTestClasses.classesDirs
        classpath = coreClasses + configurations.getAt("test" + db.capitalize())
    }
    integTestAll {
        dependsOn(task)
    }
}
