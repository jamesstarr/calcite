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
package org.apache.calcite.rex;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.math.BigDecimal;
>>>>>>> e8d5a20c4 ([CALCITE-2314] Verify RexNode transformations by evaluating before and after expressions against sample values)
    case VARCHAR:
      values.add(new NlsString("", null, null));
      values.add(new NlsString("hello", null, null));
      break;
    case TIMESTAMP:
      values.add(0L); // 1970-01-01 00:00:00
      break;
    case DATE:
      values.add(0); // 1970-01-01
      values.add(365); // 1971-01-01
      values.add(-365); // 1969-01-01
      break;
    case TIME:
      values.add(0); // 00:00:00.000
      values.add(86_399_000); // 23:59:59.000
      break;
    default:
      throw new AssertionError("don't know values for " + variable
          + " of type " + variable.getType());
    }
    if (variable.getType().isNullable()) {
      values.add(NullSentinel.INSTANCE);
    }
    return values.build();
  }

  /** Collects the variables (or other bindable sites) in an expression, and
   * counts features (such as CAST) that {@link RexInterpreter} cannot
   * handle. */
  private static class VariableCollector extends RexVisitorImpl<Void> {
    private final Set<RexNode> builder = new LinkedHashSet<>();
    private int unsupportedCount = 0;

    VariableCollector() {
      super(true);
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      builder.add(inputRef);
      return super.visitInputRef(inputRef);
    }

    @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexDynamicParam) {
        builder.add(fieldAccess);
        return null;
      } else {
        return super.visitFieldAccess(fieldAccess);
      }
    }

    @Override public Void visitCall(RexCall call) {
<<<<<<< HEAD
      if (!RexInterpreter.SUPPORTED_SQL_KIND.contains(call.getKind())) {
        ++unsupportedCount;
        return null;
      }
      return super.visitCall(call);
    }
  }
}
=======
      switch (call.getKind()) {
      case CAST:
        ++unsupportedCount;
        return null;
      case EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
      case LESS_THAN:
      case NOT_EQUALS:
        final List<RexNode> operands = new ArrayList<>(call.getOperands());
        final RexNode o0 = operands.get(0);
        final RexNode o1 = operands.get(1);
        if (o0.equals(o1) && !RexUtil.isDeterministic(o1)) {
          ++unsupportedCount;
          return null;
        }
        // fall through
      default:
        return super.visitCall(call);
      }
    }
  }
}

// End RexAnalyzer.java
