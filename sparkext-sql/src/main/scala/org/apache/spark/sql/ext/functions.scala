package org.apache.spark.sql.ext

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql._

import scala.language.implicitConversions

// scalastyle:off
object functions {
  // scalastyle:on

  private[this] implicit def toColumn(expr: Expression): Column = Column(expr)

  // TODO: Workaround for https://issues.apache.org/jira/browse/SPARK-9301
  def collectArray(expr: Column): Column = CollectArray(expr.expr)

}
