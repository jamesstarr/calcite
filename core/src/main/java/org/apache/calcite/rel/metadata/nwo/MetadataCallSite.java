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
package org.apache.calcite.rel.metadata.nwo;

import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Returns a function for getting metadata.
 * @param <RESULT> metadata
 * @param <ARGUMENTS> arguments for metadata
 */
public interface MetadataCallSite<RESULT, ARGUMENTS extends MetadataArguments> {
  MetadataType<RESULT, ARGUMENTS> metadataType();
  RESULT extract(ARGUMENTS arg);

  interface NoArg<R> extends MetadataCallSite<R, MetadataArguments.NoArg> {

    default R extract(RelNode relNode)  {
      return extract(new MetadataArguments.NoArg(relNode, metadataType()));
    }

  }

  interface IntArg<R> extends MetadataCallSite<R, MetadataArguments.IntArg> {

    default R extract(RelNode relNode, int intValue) {
      return extract(new MetadataArguments.IntArg(relNode, metadataType(), intValue));
    }
  }

  interface BooleanArg <R> extends MetadataCallSite<R, MetadataArguments.BooleanArg> {

    default R extract(RelNode relNode, boolean booleanValue) {
      return extract(new MetadataArguments.BooleanArg(relNode, metadataType(), booleanValue));
    }
  }

  interface RexNodeArg<R> extends MetadataCallSite<R, MetadataArguments.RexNodeArg>  {

    default R extract(RelNode relNode, RexNode rexNode) {
      return extract(new MetadataArguments.RexNodeArg(relNode, metadataType(),rexNode));
    }
  }

  interface ImmutableBitSetArg<R>
      extends MetadataCallSite<R, MetadataArguments.ImmutableBitSetArg>  {

    default R extract(RelNode relNode, ImmutableBitSet bitSet) {
      return extract(new MetadataArguments.ImmutableBitSetArg(relNode, metadataType(), bitSet));
    }
  }

  interface ImmutableBitSetBooleanArg <R>
      extends MetadataCallSite<R, MetadataArguments.ImmutableBitSetBooleanArg> {

    default R extract(RelNode relNode, ImmutableBitSet bitSet, boolean booleanValue) {
      return extract(
          new MetadataArguments.ImmutableBitSetBooleanArg(relNode,
              metadataType(), bitSet, booleanValue));
    }
  }

  interface ImmutableBitSetRexNodeArg<R>
      extends MetadataCallSite<R, MetadataArguments.ImmutableBitSetRexNodeArg> {

    default R extract(RelNode relNode, ImmutableBitSet bitSet, RexNode rexNode) {
      return extract(
          new MetadataArguments.ImmutableBitSetRexNodeArg(relNode,
              metadataType(), bitSet, rexNode));
    }
  }

  interface SqlExplainLevelArg<R>
      extends MetadataCallSite<R, MetadataArguments.SqlExplainLevelArg> {

    default R extract(RelNode relNode, SqlExplainLevel sqlExplainLevel) {
      return extract(
          new MetadataArguments.SqlExplainLevelArg(relNode, metadataType(), sqlExplainLevel));
    }
  }

  interface VolcanoPlannerArg <R>
      extends MetadataCallSite<R, MetadataArguments.VolcanoPlannerArg> {

    default R extract(RelNode relNode, VolcanoPlanner volcanoPlanner) {
      return extract(
          new MetadataArguments.VolcanoPlannerArg(relNode, metadataType(), volcanoPlanner));
    }
  }
}
