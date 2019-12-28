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
>>>>>>> 3b8fd86b8 ([CALCITE-1935] Implement MATCH_RECOGNIZE, part 1: add basic classes: LogicalMatch, BindableMatch, EnumerableMatch, MatchRule (Zhiqiang-He))
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.core.Match;

/**
 * Interpreter node that implements a
 * {@link Match}.
 */
public class MatchNode extends AbstractSingleNode<Match> {
  MatchNode(Compiler compiler, Match rel) {
    super(compiler, rel);
  }

  public void run() throws InterruptedException {
    Row row;
    while ((row = source.receive()) != null) {
      sink.send(row);
    }
    sink.end();
  }
<<<<<<< HEAD
========
package org.apache.calcite.adapter.geode.rel;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Various validations for geode tests.
 */
class GeodeAssertions {

  private GeodeAssertions() {}

  static Consumer<List> query(final String query) {
    return actual -> {
      String actualString =
          actual == null || actual.isEmpty()
              ? null
              : ((String) actual.get(0));

      assertEquals(query, actualString);
    };
  }

>>>>>>>> 1b41ebada ([CALCITE-2588] Run Geode Adapter tests with an Embedded Instance):geode/src/test/java/org/apache/calcite/adapter/geode/rel/GeodeAssertions.java
}

// End GeodeAssertions.java
=======
}
