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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An operator describing the <code>LIKE ANY</code> and <code>LIKE ALL</code>
 * operators.
 *
 * <p>Syntax of the two operators:
 *
 * <ul>
 * <li><code>src-value [NOT] LIKE ANY (pattern [, ...])</code></li>
 * <li><code>src-value [NOT] LIKE SOME (pattern [, ...])</code></li>
 * <li><code>src-value [NOT] LIKE ALL (pattern [, ...])</code></li>
 * </ul>
 *
 * <p><b>NOTE</b> While we support the LIKE SOME syntax, Parser.jj treats LIKE SOME
 * as the same as LIKE ANY because they are functionally the same.
 * If the <code>NOT</code> clause is present the
 * {@link org.apache.calcite.sql.parser.SqlParser parser} will generate a
 * equivalent to <code>NOT (src LIKE (ANY|ALL) pattern ...)</code>
 */
public final class SqlLikeListOperator extends SqlSpecialOperator {
  //~ Instance fields --------------------------------------------------------

  private final boolean negated;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlLikeListOperator.
   *
   * @param name    Operator name
   * @param kind    Kind
   * @param negated Whether this is 'NOT LIKE...'
   */
  private SqlLikeListOperator(
      String name,
      SqlKind kind,
      boolean negated) {
    // LIKE is right-associative, because that makes it easier to capture
    // dangling ESCAPE clauses: "a like b like c escape d" becomes
    // "a like (b like c escape d)".
    super(
        name,
        kind,
        32,
        false,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_ARRAY);
    this.negated = negated;
  }

  //~ Methods ----------------------------------------------------------------

  public static SqlLikeListOperator createLikeAny(boolean isNot) {
    String name = isNot ? "NOT LIKE ANY" : "LIKE ANY";
    return new SqlLikeListOperator(name, SqlKind.LIKE, isNot);
  }

  public static SqlLikeListOperator createLikeSome(boolean isNot) {
    String name = isNot ? "NOT LIKE SOME" : "LIKE SOME";
    return new SqlLikeListOperator(name, SqlKind.LIKE, isNot);
  }

  public static SqlLikeListOperator createLikeAll(boolean isNot) {
    String name = isNot ? "NOT LIKE ALL" : "LIKE ALL";
    return new SqlLikeListOperator(name, SqlKind.LIKE, isNot);
  }

  /**
   * Returns whether this is the 'NOT LIKE (ANY|ALL)' operator.
   *
   * @return whether this is 'NOT LIKE (ANY|ALL)'
   */
  public boolean isNegated() {
    return negated;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (callBinding.getOperandCount() != 2) {
      throw new AssertionError("unexpected number of args to "
              + callBinding.getCall() + ": " + callBinding.getOperandCount());
    }

    if (!OperandTypes.STRING_ARRAY.checkOperandTypes(
            callBinding,
            throwOnFailure)) {
      return false;
    }

    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        callBinding.operands(),
        throwOnFailure);
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("", "");
    call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
    writer.sep(getName());

    call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
    writer.endList(frame);
  }

  public ReduceResult reduceExpr(
      final int opOrdinal,
      TokenSequence list) {
    // Example:
    //   a LIKE b || c AND d
    // |  |    |      |   |  |
    //  exp0     exp1      exp2
    SqlNode exp0 = list.node(opOrdinal - 1);
    SqlOperator op = list.op(opOrdinal);
    assert op instanceof SqlLikeListOperator;
    SqlNode exp1 =
        SqlParserUtil.toTreeEx(
            list,
            opOrdinal + 1,
            getRightPrec(),
            SqlKind.OTHER);
    final SqlNode[] operands = new SqlNode[]{exp0, exp1};
    int end = opOrdinal + 2;
    SqlCall call = createCall(SqlParserPos.ZERO, operands);
    return new ReduceResult(opOrdinal - 1, end, call);
  }

  public RelDataType deriveType(
          SqlValidator validator,
          SqlValidatorScope scope,
          SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 2;
    final SqlNode patterns = operands.get(1);

    final RelDataTypeFactory typeFactory = validator.getTypeFactory();

    assert patterns instanceof SqlNodeList;
    SqlNodeList nodeList = (SqlNodeList) patterns;

    List<RelDataType> rightTypeList = ((SqlNodeList) patterns).getList()
            .stream()
            .map(pattern -> validator.deriveType(scope, pattern))
            .collect(Collectors.toList());

    RelDataType rightType = typeFactory.leastRestrictive(rightTypeList);

    // First check that the expressions in the list are compatible
    // with each other.
    if ((null == rightType) && validator.config().typeCoercionEnabled()) {
      // Do implicit type cast if it is allowed to.
      rightType = validator.getTypeCoercion().getWiderTypeFor(rightTypeList, true);
    }
    // The expressions should all be strings.
    RelDataType leftType = validator.deriveType(scope, operands.get(0));
    if (!SqlTypeUtil.inCharFamily(leftType) || !SqlTypeUtil.inCharFamily(rightType)) {
      throw new UnsupportedOperationException("Expression must be a STRING.");
    }

    ((SqlValidatorImpl) validator).setValidatedNodeType(nodeList, rightType);

    // Result is a boolean.
    return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            false);
  }

}
