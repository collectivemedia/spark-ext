package org.apache.spark.ml.sampling

import org.apache.spark.ml.param.shared.{HasLabelCol, HasOutputCol}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[sampling] trait DownsamplingParams
  extends Params with HasLabelCol with HasOutputCol {

  val primaryClass: Param[Double] = new Param[Double](this, "primaryClass",
    "Primary class to keep (0.0 or 1.0)",
    (v: Double) => v == 0.0 || v == 1.0)

  val sampleWithReplacement: Param[Boolean] = new Param[Boolean](this, "sampleWithReplacement",
    "Sample secondary class with replacement")

  def getPrimaryClass: Double = $(primaryClass)

  def getSampleWithReplacement: Boolean = $(sampleWithReplacement)

  setDefault(outputCol, uid + "_sample_weight")

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val labelColName = $(labelCol)
    val labelColDataType = schema(labelColName).dataType
    labelColDataType match {
      case _: DoubleType =>
      case other =>
        throw new IllegalArgumentException(s"Label column data type $other is not supported.")
    }
    SchemaUtils.appendColumn(schema, StructField(getOutputCol, DoubleType, nullable = false))
  }

}

/**
 * Downsample input dataset in order to reduce class ratio
 * between positive (primary) and negative (secondary) classes
  */
class Downsampling(override val uid: String) extends Estimator[DownsamplingModel] with DownsamplingParams {

  def this() = this(Identifiable.randomUID("downsampling"))

  val maxClassRatio: Param[Double] = new Param[Double](this, "maxClassRatio",
    "Max class ratio",
    (v: Double) => ParamValidators.gt(0.0)(v) && ParamValidators.ltEq(1000.0)(v))

  def getMaxClassRatio: Double = $(maxClassRatio)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setPrimaryClass(value: Double): this.type = set(primaryClass, value)
  setDefault(primaryClass -> 1.0)

  def setMaxClassRatio(value: Double): this.type = set(maxClassRatio, value)
  setDefault(maxClassRatio -> 30.0)

  def setSampleWithReplacement(value: Boolean): this.type = set(sampleWithReplacement, value)
  setDefault(sampleWithReplacement -> false)

  override def fit(dataset: DataFrame): DownsamplingModel = {
    log.info(s"Compute downsampling model with primary class: $getPrimaryClass")

    val primaryCnt = dataset.filter(col(getLabelCol) === getPrimaryClass).count()
    val secondaryCnt = dataset.filter(col(getLabelCol) !== getPrimaryClass).count()

    require(primaryCnt > 0,
      s"Primary class $getPrimaryClass should be presented in dataset")

    val classRatio = secondaryCnt.toDouble / primaryCnt

    if (classRatio <= getMaxClassRatio) {
      log.debug(s"Class ratio: $classRatio is below max class ratio: $getMaxClassRatio. Skip downsampling.")
      copyValues(new DownsamplingModel(uid, None).setParent(this))
    } else {
      val desiredSecondaryCnt = primaryCnt * getMaxClassRatio
      val sampleFraction = desiredSecondaryCnt / secondaryCnt
      log.debug(s"Class ratio: $classRatio is above max class ratio: $getMaxClassRatio. Sample fraction: $sampleFraction")
      copyValues(new DownsamplingModel(uid, Some(sampleFraction)).setParent(this))
    }

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Downsampling = defaultCopy(extra)

}

class DownsamplingModel(
  override val uid: String,
  val sampleFraction: Option[Double]
) extends Model[DownsamplingModel] with DownsamplingParams {

  def this(sampleFraction: Option[Double]) = this(Identifiable.randomUID("downsampling"), sampleFraction)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setPrimaryClass(value: Double): this.type = set(primaryClass, value)
  setDefault(primaryClass -> 1.0)

  def setSampleWithReplacement(value: Boolean): this.type = set(sampleWithReplacement, value)
  setDefault(sampleWithReplacement -> false)

  override def transform(dataset: DataFrame): DataFrame = sampleFraction match {
    case None =>
      log.debug(s"Skip dataset downsampling")
      dataset.select(col("*"), lit(1.0) as getOutputCol)

    case Some(fraction) =>
      log.debug(s"Downsample dataset with sample fraction: $fraction")

      val primary = dataset.filter(col(getLabelCol) === getPrimaryClass)
        .select(col("*"), lit(1.0) as getOutputCol)

      val secondary = dataset.filter(col(getLabelCol) !== getPrimaryClass)
        .sample(withReplacement = getSampleWithReplacement, fraction)
        .select(col("*"), lit(1.0 / fraction) as getOutputCol)

      primary.unionAll(secondary)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): DownsamplingModel = {
    val copied = new DownsamplingModel(uid, sampleFraction)
    copyValues(copied, extra).setParent(parent)
  }
}
