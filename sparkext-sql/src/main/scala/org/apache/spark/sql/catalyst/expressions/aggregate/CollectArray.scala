package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, TypeUtils}
import org.apache.spark.sql.types._

case class CollectArray(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = ArrayType(child.dataType, containsNull = false)

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private lazy val array = AttributeReference("array", dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = array :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* array = */ Literal.create(null, dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* array = */ new AppendToArray(array, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    /* array = */ ConcatArrays(Seq(array.left, array.right))
  )

  override lazy val evaluateExpression: AttributeReference = array

}

case class AppendToArray(base: Expression, child: Expression) extends Expression {

  override def children: Seq[Expression] = base :: child :: Nil

  override def foldable: Boolean = children.forall(_.foldable)

  override def dataType: DataType = {
    ArrayType(
      child.dataType,
      containsNull = false)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    // TODO: Implement it in case code generation turned off
    sys.error("AppendToArray supports only code generation mode")
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")

    val evalBase = base.gen(ctx)
    val evalChild = child.gen(ctx)

    s"""
      final boolean ${ev.isNull} = false;
      Object[] $values = new Object[0];
      ${evalBase.code}
      if (!${evalBase.isNull}) {
        $values = ${evalBase.value}.array();
      }
      ${evalChild.code}
      if (!${evalChild.isNull}) {
        $values = org.apache.commons.lang.ArrayUtils.add($values, ${evalChild.value});
      }
      final ArrayData ${ev.value} = new $arrayClass($values);
    """
  }

  override def prettyName: String = "appendToArray"
}

case class ConcatArrays(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function concatArrays")

  override def dataType: DataType = {
    children.headOption.map(_.dataType).getOrElse(NullType)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    // TODO: Implement it in case code generation turned off
    sys.error("ConcatArrays supports only code generation mode")
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")

    s"""
      final boolean ${ev.isNull} = false;
      Object[] $values = new Object[0];
    """ +
    children.zipWithIndex.map { case (e, i) =>
      val eval = e.gen(ctx)
      eval.code + s"""
        if (!${eval.isNull}) {
          $values = org.apache.commons.lang.ArrayUtils.addAll($values, ${eval.value}.array());
        }
       """
    }.mkString("\n") +
    s"final ArrayData ${ev.value} = new $arrayClass($values);"
  }

  override def prettyName: String = "concatArrays"
}
