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

package org.apache.calcite.buildtools.fmpp

import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.artifacts.Configuration
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.property
import org.gradle.kotlin.dsl.withGroovyBuilder

open class FmppTask @Inject constructor(
    objectFactory: ObjectFactory
) : DefaultTask() {
    @Classpath
    val fmppClasspath = objectFactory.property<Configuration>()
        .convention(project.configurations.named(FmppPlugin.FMPP_CLASSPATH_CONFIGURATION_NAME))

    @InputFile
    val config = objectFactory.fileProperty()

    @InputDirectory
    val templates = objectFactory.directoryProperty()

    @OutputDirectory
    val output = objectFactory.directoryProperty()
        .convention(project.layout.buildDirectory.dir("fmpp/$name"))

    @TaskAction
    fun run() {
        project.delete(output.asFileTree)
<<<<<<< HEAD

        // Find the default_config.fmpp file
        val configFile = config.get().asFile
        var defaultConfigFile = configFile.parentFile.resolve("default_config.fmpp")

        // If not found in the same directory, look in src/main/codegen (for test configs)
        if (!defaultConfigFile.exists()) {
            val mainCodegenDir = project.file("src/main/codegen")
            if (mainCodegenDir.exists()) {
                defaultConfigFile = mainCodegenDir.resolve("default_config.fmpp")
            }
        }

        // If still not found, look in the core module's codegen directory (for server module)
        if (!defaultConfigFile.exists()) {
            val coreCodegenDir = project.rootProject.file("core/src/main/codegen")
            if (coreCodegenDir.exists()) {
                defaultConfigFile = coreCodegenDir.resolve("default_config.fmpp")
            }
        }

        // Build the data parameter like Maven does: main config + default config
        val dataParam = if (defaultConfigFile.exists()) {
            "tdd(${configFile.absolutePath}), default:tdd(${defaultConfigFile.absolutePath})"
        } else {
            "tdd(${configFile.absolutePath})"
        }

=======
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
        ant.withGroovyBuilder {
            "taskdef"("name" to "fmpp",
                "classname" to "fmpp.tools.AntTask",
                "classpath" to fmppClasspath.get().asPath)
            "fmpp"(
                "configuration" to config.get(),
                "sourceRoot" to templates.get().asFile,
<<<<<<< HEAD
                "outputRoot" to output.get().asFile,
                "data" to dataParam
=======
                "outputRoot" to output.get().asFile
>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
            )
        }
    }
}
