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

import java.util.Objects;

public class MetadataArguments {
  public final RelNode relNode;
  public final MetadataType<?, ?> metadataType;

  public MetadataArguments(RelNode relNode, MetadataType<?, ?> metadataType) {
    this.relNode = relNode;
    this.metadataType = metadataType;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataArguments that = (MetadataArguments) o;
    return relNode.equals(that.relNode) && metadataType.equals(that.metadataType);
  }

  @Override public int hashCode() {
    return Objects.hash(relNode, metadataType);
  }

  public final static class NoArg extends MetadataArguments {

    public NoArg(RelNode relNode, MetadataType<?, NoArg> metadataType) {
      super(relNode, metadataType);
    }
  }

  public final static class IntArg extends MetadataArguments {

    private final int intValue;

    public IntArg(RelNode relNode, MetadataType<?, ?> metadataType, int intValue) {
      super(relNode, metadataType);
      this.intValue = intValue;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      IntArg intArg = (IntArg) o;
      return intValue == intArg.intValue;
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), intValue);
    }
  }

  public final static class BooleanArg extends MetadataArguments {

    private final boolean boolanValue;

    public BooleanArg(RelNode relNode, MetadataType<?, ?> metadataType, boolean boolanValue) {
      super(relNode, metadataType);
      this.boolanValue = boolanValue;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (getClass() != o.getClass()) {
        return false;
      } else if (!super.equals(o)) {
        return false;
      }
      BooleanArg that = (BooleanArg) o;
      return boolanValue == that.boolanValue;
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), boolanValue);
    }
  }

  public static final class RexNodeArg extends MetadataArguments {
    public final RexNode rexNode;

    public RexNodeArg(RelNode relNode, MetadataType<?, ?> metadataType, RexNode rexNode) {
      super(relNode, metadataType);
      this.rexNode = rexNode;
    }
  }

  public static final class ImmutableBitSetArg extends MetadataArguments {
    public final ImmutableBitSet bitset;

    public ImmutableBitSetArg(RelNode relNode, MetadataType<?, ?> metadataType,
        ImmutableBitSet bitset) {
      super(relNode, metadataType);
      this.bitset = bitset;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (getClass() != o.getClass()) {
        return false;
      } else if (!super.equals(o)) {
        return false;
      }
      ImmutableBitSetArg that = (ImmutableBitSetArg) o;
      return bitset.equals(that.bitset);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), bitset);
    }
  }

  public static final class ImmutableBitSetBooleanArg extends MetadataArguments {
    public final ImmutableBitSet bitset;
    public final boolean booleanValue;

    public ImmutableBitSetBooleanArg(RelNode relNode, MetadataType<?, ?> metadataType,
        ImmutableBitSet bitset, boolean booleanValue) {
      super(relNode, metadataType);
      this.bitset = bitset;
      this.booleanValue = booleanValue;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (getClass() != o.getClass()) {
        return false;
      } else if (!super.equals(o)) {
        return false;
      }
      ImmutableBitSetBooleanArg that = (ImmutableBitSetBooleanArg) o;
      return booleanValue == that.booleanValue && bitset.equals(that.bitset);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), bitset, booleanValue);
    }
  }

  public static final class ImmutableBitSetRexNodeArg extends MetadataArguments {
    public final ImmutableBitSet bitset;
    public final RexNode rexNode;

    public ImmutableBitSetRexNodeArg(RelNode relNode, MetadataType<?, ?> metadataType,
        ImmutableBitSet bitset, RexNode rexNode) {
      super(relNode, metadataType);
      this.bitset = bitset;
      this.rexNode = rexNode;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      ImmutableBitSetRexNodeArg that = (ImmutableBitSetRexNodeArg) o;
      return bitset.equals(that.bitset) && rexNode.equals(that.rexNode);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), bitset, rexNode);
    }
  }

  public static final class SqlExplainLevelArg extends MetadataArguments {
    public final SqlExplainLevel sqlExplainLevel;

    public SqlExplainLevelArg(RelNode relNode, MetadataType<?, ?> metadataType,
        SqlExplainLevel sqlExplainLevel) {
      super(relNode, metadataType);
      this.sqlExplainLevel = sqlExplainLevel;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      SqlExplainLevelArg that = (SqlExplainLevelArg) o;
      return sqlExplainLevel == that.sqlExplainLevel;
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), sqlExplainLevel);
    }
  }

  public static final class VolcanoPlannerArg extends MetadataArguments {
    private final VolcanoPlanner volcanoPlanner;

    public VolcanoPlannerArg(RelNode relNode, MetadataType<?, ?> metadataType,
        VolcanoPlanner volcanoPlanner) {
      super(relNode, metadataType);
      this.volcanoPlanner = volcanoPlanner;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      VolcanoPlannerArg that = (VolcanoPlannerArg) o;
      return volcanoPlanner.equals(that.volcanoPlanner);
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), volcanoPlanner);
    }
  }
}
