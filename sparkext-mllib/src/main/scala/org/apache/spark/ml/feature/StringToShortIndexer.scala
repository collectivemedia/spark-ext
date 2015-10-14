package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

/**
 * A label indexer that maps a string column of labels to an ML column of label indices.
 * If the input column is numeric, we cast it to string and index the string values.
 * The indices are in [0, numLabels), ordered by label frequencies.
 * So the most frequent label gets index 0.
 *
 * In contrast to Spark [[StringIndexer]] use Short for labels (instead of Double)
 */
class StringToShortIndexer(override val uid: String) extends Estimator[StringToShortIndexerModel]
with StringIndexerBase {

  def this() = this(Identifiable.randomUID("strShortIdx"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): StringToShortIndexerModel = {
    val counts = dataset.select(col($(inputCol)).cast(StringType))
      .map(_.getString(0))
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    require(labels.length <= Short.MaxValue,
      s"Unique labels count (${labels.length}) should be less then Short.MaxValue (${Short.MaxValue})")
    copyValues(new StringToShortIndexerModel(uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): StringToShortIndexer = defaultCopy(extra)
}

class StringToShortIndexerModel (
  override val uid: String,
  val labels: Array[String]) extends Model[StringToShortIndexerModel] with StringIndexerBase {

  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), labels)

  require(labels.length <= Short.MaxValue,
    s"Unique labels count (${labels.length}) should be less then Short.MaxValue (${Short.MaxValue})")

  private val labelToIndex: OpenHashMap[String, Short] = {
    val n = labels.length.toShort
    val map = new OpenHashMap[String, Short](n)
    var i: Short = 0
    while (i < n) {
      map.update(labels(i), i)
      i = (i + 1).toShort
    }
    map
  }

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip StringToShortIndexerModel.")
      return dataset
    }

    val indexer = udf { label: String =>
      if (labelToIndex.contains(label)) {
        labelToIndex(label)
      } else {
        // TODO: handle unseen labels
        throw new SparkException(s"Unseen label: $label.")
      }
    }
    val outputColName = $(outputCol)
    val metadata = NominalAttribute.defaultAttr
      .withName(outputColName).withValues(labels).toMetadata()
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as(outputColName, metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip StringToShortIndexerModel.
      schema
    }
  }

  override def copy(extra: ParamMap): StringToShortIndexerModel = {
    val copied = new StringToShortIndexerModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }
}
