package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{GenericArrayData, ArrayType, LongType, DataType}
import scala.collection.mutable

case class CollectArray(expression: Expression) extends PartialAggregate1 {
  def this() = this(null)

  override def children: Seq[Expression] = expression :: Nil

  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(expression.dataType, containsNull = false)
  override def toString: String = s"COLLECT_ARRAY($expression)"
  override def newInstance(): CollectArrayFunction = new CollectArrayFunction(expression, this)

  override def asPartial: SplitEvaluation = {
    val partialSet = Alias(CollectPartialArray(expression), "partialArrays")()
    SplitEvaluation(
      CombinePartialArrays(partialSet.toAttribute),
      partialSet :: Nil)
  }
}

case class CollectArrayFunction(
  @transient expr: Expression,
  @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val builder = mutable.ArrayBuilder.make[Any]

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      builder += evaluatedExpr
    }
  }

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(builder.result())
  }
}

case class CollectPartialArray(expression: Expression) extends AggregateExpression1 {
  def this() = this(null)

  override def children: Seq[Expression] = expression :: Nil
  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(expression.dataType, containsNull = false)
  override def toString: String = s"AddToPartialArray($expression)"
  override def newInstance(): CollectPartialArrayFunction =
    new CollectPartialArrayFunction(expression, this)
}

case class CollectPartialArrayFunction(
  @transient expr: Expression,
  @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val builder = mutable.ArrayBuilder.make[Any]

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      builder += evaluatedExpr
    }
  }

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(builder.result())
  }
}

case class CombinePartialArrays(inputSet: Expression) extends AggregateExpression1 {
  def this() = this(null)

  override def children: Seq[Expression] = inputSet :: Nil
  override def nullable: Boolean = false
  override def dataType: DataType = inputSet.dataType
  override def toString: String = s"CombinePartialArrays($inputSet)"
  override def newInstance(): CombinePartialArraysFunction = {
    new CombinePartialArraysFunction(inputSet, this)
  }
}

case class CombinePartialArraysFunction(
  @transient inputSet: Expression,
  @transient base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  val builder = mutable.ArrayBuilder.make[Any]

  override def update(input: InternalRow): Unit = {
    val inputSetEval = inputSet.eval(input).asInstanceOf[GenericArrayData].array
    val inputIterator = inputSetEval.iterator
    while (inputIterator.hasNext) {
      builder += inputIterator.next
    }
  }

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(builder.result())
  }
}
