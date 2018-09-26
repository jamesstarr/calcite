<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8718191eb (Add license to TestKtTest and add apache-rat:check to Travis CI)
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
<<<<<<< HEAD
package org.apache.calcite

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
=======
=======
>>>>>>> 8718191eb (Add license to TestKtTest and add apache-rat:check to Travis CI)
package org.apache.calcite

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.junit.JUnitAsserter
>>>>>>> 1ff1a5da8 ([CALCITE-2458] Add Kotlin as a test dependency)

class TestKtTest {
    @Test
    fun `a test to verify how Kotlin test passes in CI`() {
<<<<<<< HEAD
=======
        JUnitAsserter.hashCode() // make maven-dependency-plugin happy
>>>>>>> 1ff1a5da8 ([CALCITE-2458] Add Kotlin as a test dependency)
        assertEquals("Hello, world", "Hello, world", message = "42")
    }
}
