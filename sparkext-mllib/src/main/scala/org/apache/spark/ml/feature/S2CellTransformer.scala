package org.apache.spark.ml.feature

import com.google.common.geometry.{S2LatLng, S2CellId}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * Transform latitude and longitude into S2 Cell id
 */
class S2CellTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("S2CellTransformer"))

  // Input/Output column names

  val latCol: Param[String] = new Param[String](this, "latCol", "latitude column")

  val lonCol: Param[String] = new Param[String](this, "lonCol", "longitude column")

  val cellCol: Param[String] = new Param[String](this, "cellCol", "S2 Cell Id column")

  val level: Param[Int] = new IntParam(this, "level", "S2 Level [0, 30]",
    (i: Int) => ParamValidators.gtEq(0)(i) && ParamValidators.ltEq(30)(i))

  // Default parameters

  setDefault(
    latCol  -> "lat",
    lonCol  -> "lon",
    cellCol -> "cell",
    level   -> 10
  )

  def getLatCol: String = $(latCol)

  def getLonCol: String = $(lonCol)

  def getCellCol: String = $(cellCol)

  def getLevel: Int = $(level)

  def setLatCol(value: String): this.type = set(latCol, value)

  def setLonCol(value: String): this.type = set(lonCol, value)

  def setCellCol(value: String): this.type = set(cellCol, value)

  def setLevel(value: Int): this.type = set(level, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val currentLevel = $(level)
    val t = udf { (lat: Double, lon: Double) =>
      val cellId = S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon))
      cellId.parent(currentLevel).toToken
    }
    val metadata = outputSchema($(cellCol)).metadata
    dataset.select(col("*"), t(col($(latCol)), col($(lonCol))).as($(cellCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val latColumnName = $(latCol)
    val latDataType = schema(latColumnName).dataType
    require(latDataType == DoubleType,
      s"The latitude column $latColumnName must be Double type, " +
        s"but got $latDataType.")

    val lonColumnName = $(lonCol)
    val lonDataType = schema(lonColumnName).dataType
    require(lonDataType == DoubleType,
      s"The longitude column $lonColumnName must be Double type, " +
        s"but got $lonDataType.")

    val inputFields = schema.fields
    val outputColName = $(cellCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")

    val attr = NominalAttribute.defaultAttr.withName($(cellCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): S2CellTransformer = defaultCopy(extra)
}
