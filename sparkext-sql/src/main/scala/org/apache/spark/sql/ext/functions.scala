package org.apache.spark.sql.ext

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Expression


import scala.language.implicitConversions

// scalastyle:off
object functions {
  // scalastyle:on

  private def withExpr(expr: Expression): Column = Column(expr)

  private def withAggregateFunction(
    func: AggregateFunction,
    isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  def collectArray(e: Expression): Column = withAggregateFunction {
    CollectArray(e)
  }

  def collectArray(c: Column): Column = collectArray(c.expr)

  def concatArrays(cols: Column*): Column = withExpr { ConcatArrays(cols.map(_.expr)) }

  def appendToArray(base: Column, child: Column): Column = withExpr { AppendToArray(base.expr, child.expr) }

}
