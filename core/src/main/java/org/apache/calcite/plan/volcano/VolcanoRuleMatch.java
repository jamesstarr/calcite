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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptRuleOperand;
      RelOptUtil.appendRelDescription(buf, rels[i]);
    }
    buf.append(']');
    return buf.toString();
  }

  /**
   * Recomputes the digest of this VolcanoRuleMatch.
   */
  @Deprecated // to be removed before 2.0
  public void recomputeDigest() {
    digest = computeDigest();
  }

  /** Returns whether all elements of a given array are not-null;
   * fails if any are null. */
  private static <E> boolean allNotNull(E[] es, Litmus litmus) {
    for (E e : es) {
      if (e == null) {
        return litmus.fail("was null", (Object) es);
      }
    }
    return litmus.succeed();
  }

}
