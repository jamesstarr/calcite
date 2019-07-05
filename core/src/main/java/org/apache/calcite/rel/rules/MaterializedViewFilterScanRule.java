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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
>>>>>>> 79e6848c7 (DX-34864 Add CoreRules and move static references of these rules to CoreRules)
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Suppliers;

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(Filter.class, TableScan.class);

    @Override default MaterializedViewFilterScanRule toRule() {
      return new MaterializedViewFilterScanRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
                                  Class<? extends TableScan> scanClass) {
      return withOperandSupplier(b0 ->
              b0.operand(filterClass).oneInput(b1 ->
                      b1.operand(scanClass).noInputs()))
              .as(Config.class);
    }
  }
}

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
            .withOperandFor(Filter.class, TableScan.class);

    @Override default MaterializedViewFilterScanRule toRule() {
      return new MaterializedViewFilterScanRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
                                  Class<? extends TableScan> scanClass) {
      return withOperandSupplier(b0 ->
              b0.operand(filterClass).oneInput(b1 ->
                      b1.operand(scanClass).noInputs()))
              .as(Config.class);
    }
  }
}
