/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
<<<<<<< HEAD
 */
=======
 *
 */

>>>>>>> 17088b0f9 ([CALCITE-2905] Migrate build scripts to Gradle)
dependencies {
    api(project(":core"))
    api(project(":linq4j"))

    implementation("com.google.guava:guava") {
        because("""ForbiddenApis' signatures.txt contains com.google.common.base.Precondition
          and it needs the file on a classpath to parse the configuration"""
        )
    }
    testImplementation("sqlline:sqlline")
}
