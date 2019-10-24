@echo off
::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to you under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
:: http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

:: sqlline.bat - Windows script to launch SQL shell
:: Example:
:: > sqlline.bat
:: sqlline> !connect jdbc:calcite: admin admin

:: Copy dependency jars on first call.
<<<<<<< HEAD
:: To force jar refresh, remove core\target\dependencies)
if not exist build\libs\sqllineClasspath.jar (call gradlew buildSqllineClasspath)
=======
:: (To force jar refresh, remove core\target\dependencies)
:: We remove slf4j-log4j, otherwise slf4j complains about multiple bindings.
if not exist core\target\dependencies (
  call .\mvnw -B dependency:copy-dependencies -DoverWriteReleases=false -DoverWriteSnapshots=false -DoverWriteIfNewer=true -DoutputDirectory=target\dependencies
  del /q /s slf4j-log4j12-*.jar
)
>>>>>>> 040dd0c60 ([CALCITE-3444] Upgrade SQLLine to 1.9.0, and solve "Class path contains multiple SLF4J bindings" problem)

<<<<<<< HEAD
java -Xmx1G -jar build\libs\sqllineClasspath.jar %*
=======
java -Xmx1G -cp ".\target\dependencies\*;core\target\dependencies\*;cassandra\target\dependencies\*;druid\target\dependencies\*;elasticsearch\target\dependencies\*;geode\target\dependencies\*;file\target\dependencies\*;mongodb\target\dependencies\*;server\target\dependencies\*;spark\target\dependencies\*;splunk\target\dependencies\*" sqlline.SqlLine --verbose=true %*
>>>>>>> beef988cd ([CALCITE-3023] Upgrade elastic search to 7.x (Takako Shimamoto))

:: End sqlline.bat
