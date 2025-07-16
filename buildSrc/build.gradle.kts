/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

<<<<<<< HEAD
import com.github.vlsi.gradle.properties.dsl.props
=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    `kotlin-dsl` apply false
<<<<<<< HEAD
    id("com.github.autostyle")
    id("com.github.vlsi.gradle-extensions")
=======
    id("com.diffplug.gradle.spotless")
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
}

repositories {
    jcenter()
    gradlePluginPortal()
}

<<<<<<< HEAD
val skipAutostyle by props()

allprojects {
=======
subprojects {
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
    repositories {
        jcenter()
        gradlePluginPortal()
    }
    applyKotlinProjectConventions()
}

fun Project.applyKotlinProjectConventions() {
    apply(plugin = "org.gradle.kotlin.kotlin-dsl")

<<<<<<< HEAD
<<<<<<< HEAD
    tasks.withType<KotlinCompile> {
=======
    plugins.withType<KotlinDslPlugin> {
        configure<KotlinDslPluginOptions> {
            experimentalWarning.set(false)
        }
    }

=======
>>>>>>> af96e9835 ([CALCITE-4829] Bump Gradle to 7.2 and test with Java 17 at GitHub Actions)
    tasks.withType<KotlinCompile> {
<<<<<<< HEAD
        sourceCompatibility = "unused"
        targetCompatibility = "unused"
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
>>>>>>> 41e581fd6 ([CALCITE-5440] Bump gradle from 7.4.2 to 7.6.1)
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)

    // Configure task dependencies after evaluation
    afterEvaluate {
        tasks.findByName("compileKotlin")?.let { compileTask ->
            tasks.findByName("spotlessKotlin")?.let { spotlessTask ->
                compileTask.dependsOn(spotlessTask)
            }
        }
    }
<<<<<<< HEAD
    if (!skipAutostyle) {
        apply(plugin = "com.github.autostyle")
        autostyle {
            kotlin {
                ktlint()
                trimTrailingWhitespace()
                endWithNewline()
            }
            kotlinGradle {
                ktlint()
                trimTrailingWhitespace()
                endWithNewline()
            }
=======
=======
>>>>>>> a9bf75a77 (Manually update gradle build with updated dependencies, omit babel, and spotlessApply)
    apply(plugin = "com.diffplug.gradle.spotless")
    spotless {
        kotlin {
            ktlint()
            trimTrailingWhitespace()
            endWithNewline()
        }
        kotlinGradle {
            ktlint()
            trimTrailingWhitespace()
            endWithNewline()
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
        }
    }
}

dependencies {
    subprojects.forEach {
        runtimeOnly(project(it.path))
    }
}
