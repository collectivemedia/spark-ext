package org.apache.spark.ml.feature

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable


private[feature] trait GatherEncoderParams
  extends Params with HasInputCol with HasOutputCol with HasKeyCol with HasValueCol {

  val cover: Param[Double] = new Param[Double](this, "cover",
    "Top coverage",
    (v: Double) => ParamValidators.gt(0.0)(v) && ParamValidators.ltEq(100.0)(v))

  val allOther: Param[Boolean] = new Param[Boolean](this, "allOther",
    "Add all other column")

  def getCover: Double = $(cover)

  def getAllOther: Boolean = $(allOther)

  protected def validateSchema(schema: StructType): Unit = {
    // Check that inputCol is array of StructType
    val inputColName = $(inputCol)
    val inputColDataType = schema(inputColName).dataType
    val inputColStructSchema = inputColDataType match {
      case ArrayType(structType: StructType, _) =>
        structType
      case other =>
        throw new IllegalArgumentException(s"Input column data type $other is not supported.")
    }

    // Check that key type is supported
    val keyColName = $(keyCol)
    val keyColDataType = inputColStructSchema(keyColName).dataType
    keyColDataType match {
      case _: NumericType =>
      case _: StringType =>
      case other =>
        throw new IllegalArgumentException(s"Key column data type $other is not supported.")
    }

    // Check that value type is numerical
    val valueColName = $(valueCol)
    val valueColDataType = inputColStructSchema(valueColName).dataType
    valueColDataType match {
      case _: NumericType =>
      case other =>
        throw new IllegalArgumentException(s"Value data type $other is not supported.")
    }
  }

}

/**
 * Encode categorical values collected by [[Gather]] transformation as feature vector using
 * dummy variables inside [[org.apache.spark.ml.attribute.AttributeGroup AttributeGroup]]
 * with attached metadata
 *
 * {{{
 *  cookie_id | sites
 *  ----------|------------------------------------------------------------------------
 *  cookieAA  | [{ site_id: 1, impressions: 15.0 }, { site_id: 2, impressions: 20.0 }]
 *  cookieBB  | [{ site_id: 2, impressions: 7.0 }, { site_id: 3, impressions: 5.0 }]
 *  }}}
 *
 * transformed into
 *
 * {{{
 *  cookie_id | site_features
 *  ----------|------------------------
 *  cookieAA  | [ 15.0 , 20.0 , 0   ]
 *  cookieBB  | [ 0.0  ,  7.0 , 5.0 ]
 *  }}}
 *
 * Optionally apply dimensionality reduction using top transformation:
 *  - Top coverage, is selecting categorical values by computing the count of distinct users for each value,
 *    sorting the values in descending order by the count of users, and choosing the top values from the resulting
 *    list such that the sum of the distinct user counts over these values covers c percent of all users,
 *    for example, selecting top geographic locations covering 99% of users.
 */
class GatherEncoder(override val uid: String) extends Estimator[GatherEncoderModel] with GatherEncoderParams {

  def this() = this(Identifiable.randomUID("gatheredEncoder"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setKeyCol(value: String): this.type = set(keyCol, value)

  def setValueCol(value: String): this.type = set(valueCol, value)

  def setCover(value: Double): this.type = set(cover, value)

  def setAllOther(value: Boolean): this.type = set(allOther, value)

  setDefault(
    cover -> 100.0,
    allOther -> true
  )

  override def fit(dataset: DataFrame): GatherEncoderModel = {
    validateSchema(dataset.schema)

    val inputColName = $(inputCol)
    val keyColName = $(keyCol)
    val valueColName = $(valueCol)

    log.info(s"Fit gather encoder for input column: $inputColName. " +
      s"Key column: $keyColName " +
      s"Value column: $valueColName" +
      s"Cover: ${$(cover)}. " +
      s"All other: ${$(allOther)}.")

    val gatherKeys: Array[Any] = if ($(cover) == 100.0) {
      // With cover 100% it's required to collect all keys
      val keyCol = s"${uid}_key"
      dataset.select(explode(col(s"$inputColName.$keyColName")) as keyCol)
        .groupBy(keyCol).agg(col(keyCol)).collect().map(_.get(0))
    } else {

      val keyCol = s"${uid}_key"
      val grouped: DataFrame = dataset.select(explode(col(s"$inputColName.$keyColName")) as keyCol).groupBy(keyCol).count()
      val keys: Array[(Any, Long)] = grouped.collect().map { row =>
        val key = row.get(0)
        val cnt = row.getLong(1)
        (key, cnt)
      }

      log.debug(s"Collected ${keys.length} unique keys")

      val topKeys = keys.sortBy(_._2)(implicitly[Ordering[Long]].reverse)

      // Get number of columns below cover threshold
      val threshold = ($(cover) / 100) * topKeys.map(_._2).sum
      val keysBelowThreshold = topKeys.map(_._2).scanLeft(0L)((cum, cnt) => cum + cnt).takeWhile(_ < threshold).length

      topKeys.take(keysBelowThreshold).map(_._1)
    }

    copyValues(new GatherEncoderModel(uid, gatherKeys).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)
    // at this point labels and size of feature vectors is unknown
    SchemaUtils.appendColumn(schema, StructField($(outputCol), new VectorUDT))
  }

  override def copy(extra: ParamMap): GatherEncoder = defaultCopy(extra)

}

/**
 * Model fitted by [[GatherEncoder]]
 *
 * @param modelKeys  Ordered list of keys, corresponding column indices in feature vector
 */
class GatherEncoderModel(
  override val uid: String,
  val modelKeys: Array[Any]
) extends Model[GatherEncoderModel] with GatherEncoderParams {

  def this(keys: Array[Any]) = this(Identifiable.randomUID("gatheredEncoder"), keys)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setKeyCol(value: String): this.type = set(keyCol, value)

  def setValueCol(value: String): this.type = set(valueCol, value)

  def setCover(value: Double): this.type = set(cover, value)

  def setAllOther(value: Boolean): this.type = set(allOther, value)

  setDefault(
    cover -> 100.0,
    allOther -> false
  )

  private val labels: Array[String] = modelKeys.map(_.toString)

  private val keyIndex: Map[Any, Int] = modelKeys.zipWithIndex.toMap

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val inputColName = $(inputCol)
    val keyColName = $(keyCol)
    val valueColName = $(valueCol)

    val allOtherEnabled = $(allOther)
    val featureSize = if (allOtherEnabled) modelKeys.length + 1 else modelKeys.length

    val encoder = udf { (keys: mutable.WrappedArray[AnyRef], values: mutable.WrappedArray[Double]) =>

      if (keys == null && values == null) {
        Vectors.sparse(featureSize, Nil)

      } else if (keys != null && values != null) {

        require(keys.length == values.length,
          s"Keys names length doesn't match with values length")

        if (keys.length > 0) {
          var i: Int = 0
          val elements = mutable.Map.empty[Int, Double]
          while (i < keys.length) {
            val key = keys(i)
            val value = values(i)

            keyIndex.get(key) match {
              // Take latest value for key
              case Some(idx) =>
                elements(idx) = value
              // Accumulate values is all other enabled
              case None if allOtherEnabled =>
                val allOther = elements.getOrElse(modelKeys.length, 0.0)
                elements.update(modelKeys.length, allOther + value)
              // Ignore key if all other is disables
              case None =>
            }

            i += 1
          }
          Vectors.sparse(featureSize, elements.toBuffer)

        } else {
          Vectors.sparse(featureSize, Nil)
        }

      } else {
        throw new IllegalArgumentException(s"Keys and Values are not consistent")
      }
    }

    val outputColName = $(outputCol)

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"),
      encoder(
        dataset(s"$inputColName.$keyColName").cast(ArrayType(StringType)),
        dataset(s"$inputColName.$valueColName").cast(ArrayType(DoubleType))
      ).as(outputColName, metadata))

  }

  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    val attrLabels = if ($(allOther)) labels :+ "all other" else labels
    val attrs: Array[Attribute] = attrLabels.map(lbl => new NumericAttribute(Some(lbl)))
    val attrGroup = new AttributeGroup($(outputCol), attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): GatherEncoderModel = {
    val copied = new GatherEncoderModel(uid, modelKeys)
    copyValues(copied, extra).setParent(parent)
  }

}
