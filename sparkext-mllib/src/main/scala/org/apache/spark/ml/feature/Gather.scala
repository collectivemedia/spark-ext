package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ext.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[feature] trait GatherParams extends Params with HasKeyCol with HasValueCol with HasOutputCol {

  val primaryKeyCols: Param[Array[String]] = new StringArrayParam(this, "primaryKeyCols",
    "Primary key column names",
    ParamValidators.arrayLengthGt(0))

  val valueAgg: Param[String] = new Param[String](this, "valueAgg",
    "Aggregate function applied to valueCol: 'sum' or 'count'",
    ParamValidators.inArray(Array("sum", "count")))

  def getPrimaryKeyCols: Array[String] = $(primaryKeyCols)

  def getValueAgg: String = $(valueAgg)
}

/**
 * Inspired by R `tidyr` and `reshape2` packages. Convert long [[org.apache.spark.sql.DataFrame DataFrame]] with values
 * for each key into wide [[org.apache.spark.sql.DataFrame DataFrame]], applying aggregation function if single
 * key has multiple values
 * {{{
 * cookie_id | site_id | impressions
 * ----------|---------|--------------
 *  cookieAA |   123   | 10
 *  cookieAA |   123   | 5
 *  cookieAA |   456   | 20
 * }}}
 *
 * gathered using `sum` agregate
 *
 * {{{
 *  cookie_id | output_col
 *  ----------|------------------------
 *  cookieAA  | [{ site_id: 123, impressions: 15.0 }, { site_id: 456, impressions: 20.0 }]
 *  }}}
 */
class Gather(override val uid: String) extends Transformer with GatherParams {

  def this() = this(Identifiable.randomUID("gather"))

  def setPrimaryKeyCols(value: String*): this.type = set(primaryKeyCols, value.toArray)

  def setKeyCol(value: String): this.type = set(keyCol, value)

  def setValueCol(value: String): this.type = set(valueCol, value)

  def setValueAgg(value: String): this.type = set(valueAgg, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(
    valueAgg -> "sum"
  )

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val pkCols = $(primaryKeyCols).map(col)

    val grouped = dataset.groupBy(pkCols :+ col($(keyCol)) : _*)
    val aggregateCol = s"${uid}_value_aggregate"
    val aggregated = $(valueAgg) match {
      case "sum"   => grouped.agg(sum($(valueCol))   as aggregateCol)
      case "count" => grouped.agg(count($(valueCol)) as aggregateCol)
    }

    val metadata = outputSchema($(outputCol)).metadata

    aggregated
      .groupBy(pkCols: _*)
      .agg(collectArray(struct(
          col($(keyCol)),
          col(aggregateCol).cast(DoubleType).as($(valueCol))
      )).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val valueFunName = $(valueAgg)

    val keyColName = $(keyCol)
    val keyColDataType = schema(keyColName).dataType
    keyColDataType match {
      case _: NumericType =>
      case _: StringType =>
      case other =>
        throw new IllegalArgumentException(s"Key column data type $other is not supported.")
    }

    val valueColName = $(valueCol)
    val valueColDataType = schema(valueColName).dataType
    valueColDataType match {
      case _: NumericType =>
      case _: StringType if valueFunName == "count" =>
      case other =>
        throw new IllegalArgumentException(s"Value data type $other is not supported with value aggregate $valueAgg.")
    }

    val pkFields = $(primaryKeyCols).map(schema.apply)
    val rollupType = StructType(Array(
      StructField($(keyCol), keyColDataType),
      StructField($(valueCol), DoubleType)
    ))
    val rollupField = StructField($(outputCol), ArrayType(rollupType), nullable = false)

    StructType(pkFields :+ rollupField)
  }

  override def copy(extra: ParamMap): S2CellTransformer = defaultCopy(extra)

}
