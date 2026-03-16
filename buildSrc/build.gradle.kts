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

>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
=======
import com.github.vlsi.gradle.properties.dsl.props
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    `kotlin-dsl` apply false
    id("com.diffplug.gradle.spotless")
=======
    id("com.github.autostyle")
>>>>>>> 678342a7b ([CALCITE-3623] Replace Spotless with Autostyle)
=======
>>>>>>> 38246acd0 (Gradle: Support skipAutostyle property in buildSrc (#191))
}

repositories {
    jcenter()
    gradlePluginPortal()
}

subprojects {
=======
val skipAutostyle by props()

allprojects {
>>>>>>> 38246acd0 (Gradle: Support skipAutostyle property in buildSrc (#191))
    repositories {
        jcenter()
        gradlePluginPortal()
    }
    applyKotlinProjectConventions()
}

fun Project.applyKotlinProjectConventions() {
    apply(plugin = "org.gradle.kotlin.kotlin-dsl")

    plugins.withType<KotlinDslPlugin> {
        configure<KotlinDslPluginOptions> {
            experimentalWarning.set(false)
        }
    }

=======
    tasks.withType<KotlinCompile> {
>>>>>>> 41e581fd6 ([CALCITE-5440] Bump gradle from 7.4.2 to 7.6.1)
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
<<<<<<< HEAD
<<<<<<< HEAD
=======

    // Configure task dependencies after evaluation
    afterEvaluate {
        tasks.findByName("compileKotlin")?.let { compileTask ->
            tasks.findByName("spotlessKotlin")?.let { spotlessTask ->
                compileTask.dependsOn(spotlessTask)
            }
        }
    }
>>>>>>> 38246acd0 (Gradle: Support skipAutostyle property in buildSrc (#191))
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
<<<<<<< HEAD
=======
=======
    apply(plugin = "com.diffplug.gradle.spotless")
    spotless {
=======
    apply(plugin = "com.github.autostyle")
    autostyle {
>>>>>>> 678342a7b ([CALCITE-3623] Replace Spotless with Autostyle)
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
=======
>>>>>>> 38246acd0 (Gradle: Support skipAutostyle property in buildSrc (#191))
        }
    }
}

dependencies {
    subprojects.forEach {
        runtimeOnly(project(it.path))
    }
}
