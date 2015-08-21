package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.Model
import org.apache.spark.ml.attribute.{NominalAttribute, AttributeGroup, NumericAttribute, Attribute}
import org.apache.spark.ml.param.{ParamMap, ParamValidators, Param, Params}
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable


private[feature] trait GatheredEncoderParams
  extends Params with HasInputCol with HasOutputCol with HasCategoryCol with HasValueCol {

  val cover: Param[Double] = new Param[Double](this, "cover",
    "Top coverage",
    (v: Double) => ParamValidators.gt(0.0)(v) && ParamValidators.ltEq(100.0)(v))

  val allOther: Param[Boolean] = new Param[Boolean](this, "allOther",
    "Add all other column")

  def getCover: Double = $(cover)

  def getAllOther: Boolean = $(allOther)

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
class GatheredEncoder {

}

class GatheredEncoderModel(
  override val uid: String,
  val categories: Array[AnyRef]
) extends Model[GatheredEncoderModel] with GatheredEncoderParams {

  def this(categories: Array[AnyRef]) = this(Identifiable.randomUID("gatherEncoder"), categories)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setCategoryCol(value: String): this.type = set(categoryCol, value)

  def setValueCol(value: String): this.type = set(valueCol, value)

  def setCover(value: Double): this.type = set(cover, value)

  def setAllOther(value: Boolean): this.type = set(allOther, value)

  setDefault(
    cover -> 100.0,
    allOther -> true
  )

  private def labels = categories.map(_.toString)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val inputColName = $(inputCol)
    val categoryColName = $(categoryCol)
    val valueColName = $(valueCol)

    val encoder = udf { (names: mutable.WrappedArray[AnyRef], values: mutable.WrappedArray[Double]) =>
      require(names.length == values.length,
        s"Categories names length doesn't match with values length")

      var i: Int = 0
      val elements = mutable.Map.empty[Int, Double]
      while (i < names.length) {
        val name = names(i)
        val value = values(i)
        val idx = categories.indexOf(name)

        // Take latest value for key and accumulate for all other
        if (idx >= 0) {
          elements(idx) = value
        } else if ($(allOther)) {
          val allOther = elements.getOrElse(categories.length, 0.0)
          elements.update(categories.length, allOther + value)
        }
        i += 1
      }

      val size = if ($(allOther)) categories.length + 1 else categories.length
      Vectors.sparse(size, elements.toArray)
    }

    val outputColName = $(outputCol)

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"),
      encoder(
        dataset(s"$inputColName.$categoryColName").cast(ArrayType(StringType, containsNull = false)),
        dataset(s"$inputColName.$valueColName").cast(ArrayType(DoubleType, containsNull = false))
      ).as(outputColName, metadata))

  }

  override def transformSchema(schema: StructType): StructType = {

    // Check that inputCol is array of StructType
    val inputColName = $(inputCol)
    val inputColDataType = schema(inputColName).dataType
    val inputColStructSchema = inputColDataType match {
      case ArrayType(structType: StructType, false) => structType
      case other =>
        throw new IllegalArgumentException(s"Input column data type $other is not supported.")
    }

    // Check that categorical type is supported
    val categoryColName = $(categoryCol)
    val categoryColDataType = inputColStructSchema(categoryColName).dataType
    categoryColDataType match {
      case _: NumericType =>
      case _: StringType =>
      case other =>
        throw new IllegalArgumentException(s"Category column data type $other is not supported.")
    }

    // Check that value type is numerical
    val valueColName = $(valueCol)
    val valueColDataType = inputColStructSchema(valueColName).dataType
    valueColDataType match {
      case _: NumericType =>
      case other =>
        throw new IllegalArgumentException(s"Category value data type $other is not supported.")
    }

    val attrLabels = if ($(allOther)) labels :+ "all other" else labels
    val attrs: Array[Attribute] = attrLabels.map(lbl => new NumericAttribute(Some(lbl)))
    val attrGroup = new AttributeGroup($(outputCol), attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): GatheredEncoderModel = {
    defaultCopy[GatheredEncoderModel](extra).setParent(parent)
  }

}
