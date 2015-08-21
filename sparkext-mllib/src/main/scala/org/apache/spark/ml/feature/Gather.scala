package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ext.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Inspired by R 'tidyr' and 'reshape2' packages. Convert 'long' DataFrame with values
 * for each category into 'wide' DataFrame, applying aggregation function if single
 * category has multiple values
 *
 * cookie_id | site_id | impressions
 * -----------------------------------
 *  cookieAA |   123   | 10
 *  cookieAA |   123   | 5
 *  cookieAA |   456   | 20
 *
 *  ---------->>>>>>>> gather using 'sum' aggregate
 *
 *  cookie_id | output_col
 *  -----------------------------------
 *  cookieAA  | [
 *            |   { site_id: 123, impressions: 15.0 },
 *            |   { site_id: 456, impressions: 20.0 }
 *            | ]
 */
private[feature] trait GatherParams extends Params with HasOutputCol {

  val primaryKeyCols: Param[Array[String]] = new StringArrayParam(this, "primaryKeyCols",
    "Primary key column names",
    ParamValidators.arrayLengthGt(0))

  val categoryCol: Param[String] = new Param[String](this, "categoryCol",
    "Column that holds value for category name")

  val valueCol: Param[String] = new Param[String](this, "valueCol",
    "Column that holds a value for category")

  val valueAgg: Param[String] = new Param[String](this, "valueAgg",
    "Aggregate function applied to valueCol: 'sum' or 'count'",
    ParamValidators.inArray(Array("sum", "count")))

  def getPrimaryKeyCols: Array[String] = $(primaryKeyCols)

  def getCategoryCol: String = $(categoryCol)

  def getValueCol: String = $(valueCol)

  def getValueAgg: String = $(valueAgg)
}

class Gather(override val uid: String) extends Transformer with GatherParams {

  def this() = this(Identifiable.randomUID("gather"))

  def setPrimaryKeyCols(value: String*): this.type = set(primaryKeyCols, value.toArray)

  def setCategoryCol(value: String): this.type = set(categoryCol, value)

  def setValueCol(value: String): this.type = set(valueCol, value)

  def setValueAgg(value: String): this.type = set(valueAgg, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(
    valueAgg -> "sum"
  )

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val pkCols = $(primaryKeyCols).map(col)

    val grouped = dataset.groupBy(pkCols :+ col($(categoryCol)) : _*)
    val aggregateCol = s"${uid}_value_aggregate"
    val aggregated = $(valueAgg) match {
      case "sum" => grouped.agg(sum($(valueCol)) as aggregateCol)
      case "count" => grouped.agg(count($(valueCol)) as aggregateCol)
    }

    val metadata = outputSchema($(outputCol)).metadata

    aggregated
      .groupBy(pkCols: _*)
      .agg(collectArray(struct(
          col($(categoryCol)),
          col(aggregateCol).cast(DoubleType).as($(valueCol))
      )).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val valueFunName = $(valueAgg)

    val categoryColName = $(categoryCol)
    val categoryColDataType = schema(categoryColName).dataType
    categoryColDataType match {
      case _: NumericType =>
      case _: StringType =>
      case other =>
        throw new IllegalArgumentException(s"Category column data type $other is not supported.")
    }

    val valueColName = $(valueCol)
    val valueColDataType = schema(valueColName).dataType
    valueColDataType match {
      case _: NumericType =>
      case _: StringType if valueFunName == "count" =>
      case other =>
        throw new IllegalArgumentException(s"Category value data type $other is not supported with value aggregate $valueAgg.")
    }

    val pkFields = $(primaryKeyCols).map(schema.apply)
    val rollupType = StructType(Array(
      StructField($(categoryCol), categoryColDataType),
      StructField($(valueCol), DoubleType)
    ))
    val rollupField = StructField($(outputCol), ArrayType(rollupType), nullable = false)

    StructType(pkFields :+ rollupField)
  }

  override def copy(extra: ParamMap): S2CellTransformer = defaultCopy(extra)

}
