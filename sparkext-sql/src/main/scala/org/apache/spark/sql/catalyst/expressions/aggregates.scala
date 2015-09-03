package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{GenericArrayData, ArrayType, DataType}
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

  // Reducing GC pressure with this trick

  var firstValue: Any = _
  var builder: mutable.ListBuffer[Any] = _

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      if (firstValue == null && builder == null) {
        // Got first value
        firstValue = evaluatedExpr
      } else if (firstValue != null && builder == null) {
        // Got second value
        builder = mutable.ListBuffer.empty[Any]
        builder += firstValue
        builder += evaluatedExpr
        firstValue = null
      } else if (firstValue == null && builder != null) {
        // Got 2+ values
        builder += evaluatedExpr
      } else {
        throw new IllegalStateException(s"Both state variables are defined")
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (firstValue == null && builder == null) {
      Const.emptyGenericArrayData
    } else if (firstValue != null && builder == null) {
      new GenericArrayData(Array(firstValue))
    } else if (firstValue == null && builder != null) {
      new GenericArrayData(builder.toArray)
    } else {
      throw new IllegalStateException("Both state variables are defined")
    }
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

  // Reducing GC pressure with this trick

  var firstValue: Any = _
  var builder: mutable.ListBuffer[Any] = _

  override def update(input: InternalRow): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      if (firstValue == null && builder == null) {
        // Got first value
        firstValue = evaluatedExpr
      } else if (firstValue != null && builder == null) {
        // Got second value
        builder = mutable.ListBuffer.empty[Any]
        builder += firstValue
        builder += evaluatedExpr
        firstValue = null
      } else if (firstValue == null && builder != null) {
        // Got 2+ values
        builder += evaluatedExpr
      } else {
        throw new IllegalStateException(s"Both state variables are defined")
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (firstValue == null && builder == null) {
      Const.emptyGenericArrayData
    } else if (firstValue != null && builder == null) {
      new GenericArrayData(Array(firstValue))
    } else if (firstValue == null && builder != null) {
      new GenericArrayData(builder.toArray)
    } else {
      throw new IllegalStateException("Both state variables are defined")
    }
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

  // Reducing GC pressure with this trick

  var firstArray: GenericArrayData = _
  var builder: mutable.ListBuffer[Any] = _

  override def update(input: InternalRow): Unit = {
    val inputSetEval = inputSet.eval(input).asInstanceOf[GenericArrayData]

    if (firstArray == null && builder == null) {
      // Got first array
      firstArray = inputSetEval
    } else if (firstArray != null && builder == null) {
      // Got second value
      builder = mutable.ListBuffer.empty[Any]
      val inputIterator = firstArray.array.iterator ++ inputSetEval.array.iterator
      while (inputIterator.hasNext) {
        builder += inputIterator.next
      }
      firstArray = null
    } else if (firstArray == null && builder != null) {
      // Got 2+ values
      val inputIterator = inputSetEval.array.iterator
      while (inputIterator.hasNext) {
        builder += inputIterator.next
      }
    } else {
      throw new IllegalStateException(s"Both state variables are defined")
    }
  }

  override def eval(input: InternalRow): Any = {
    if (firstArray == null && builder == null) {
      Const.emptyGenericArrayData
    } else if (firstArray != null && builder == null) {
      firstArray
    } else if (firstArray == null && builder != null) {
      new GenericArrayData(builder.toArray)
    } else {
      throw new IllegalStateException("Both state variables are defined")
    }
  }
}
