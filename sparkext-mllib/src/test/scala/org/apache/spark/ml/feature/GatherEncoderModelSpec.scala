package org.apache.spark.ml.feature

import breeze.linalg.DenseVector
import com.collective.TestSparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec

class GatherEncoderModelSpec extends FlatSpec with TestSparkContext {

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("sites", ArrayType(StructType(Seq(
      StructField("site", StringType),
      StructField("impressions", LongType
    ))), containsNull = true))
  ))

  val cookie1 = "cookie1"
  val cookie2 = "cookie2"
  val cookie3 = "cookie3"
  val cookie4 = "cookie4"
  val cookie5 = "cookie5"

  val dataset = sqlContext.createDataFrame(sc.parallelize(Seq(
    Row(cookie1, Array(
      Row("google.com", 12L),
      Row("cnn.com", 14L)
    )),
    Row(cookie2, Array(
      Row("bbc.com", 20L),
      Row("auto.com", 1L),
      Row("moto.com", 3L)
    )),
    Row(cookie3, Array(
      Row("sport.com", 100L)
    )),
    Row(cookie4, Array.empty[Row]),
    Row(cookie5, null)
  )), schema)

  def createEncoder(keys: Array[Any]) =
    new GatherEncoderModel(keys)
    .setInputCol("sites")
    .setOutputCol("features")
    .setKeyCol("site")
    .setValueCol("impressions")

  val baseEncoder = createEncoder(Array("google.com", "bbc.com", "cnn.com"))

  def toFeatures(encoder: GatherEncoderModel, dataset: DataFrame): Map[String, Vector] = {
    val encodedDf = encoder.transform(dataset).select("cookie_id", "features")
    encodedDf.collect().map { case Row(cookieId: String, features: Vector) =>
      cookieId -> features
    }.toMap
  }

  "Gather Encoder Model" should "encode categories ignoring all other" in {
    val encoder = baseEncoder.setAllOther(false)
    val features = toFeatures(encoder, dataset)

    assert(features(cookie1).size == 3)
    assert(features(cookie1).toSparse.indices.toSeq == 0 :: 2 :: Nil)
    assert(features(cookie1).toSparse.values.toSeq == 12 :: 14 :: Nil)

    assert(features(cookie2).size == 3)
    assert(features(cookie2).toSparse.indices.toSeq == 1 :: Nil)
    assert(features(cookie2).toSparse.values.toSeq == 20 :: Nil)

    def assertEmptyFeatures(cookie: String): Unit = {
      assert(features(cookie).size == 3)
      assert(features(cookie).toSparse.indices.toSeq == Nil)
      assert(features(cookie).toSparse.values.toSeq == Nil)
    }

    assertEmptyFeatures(cookie3)
    assertEmptyFeatures(cookie4)
    assertEmptyFeatures(cookie5)
  }

  it should "encode categories with all other" in {
    val encoder = baseEncoder.setAllOther(true)
    val features = toFeatures(encoder, dataset)

    assert(features(cookie1).size == 4)
    assert(features(cookie1).toSparse.indices.toSeq == 0 :: 2 :: Nil)
    assert(features(cookie1).toSparse.values.toSeq == 12 :: 14 :: Nil)

    assert(features(cookie2).size == 4)
    assert(features(cookie2).toSparse.indices.toSeq == 1 :: 3 :: Nil)
    assert(features(cookie2).toSparse.values.toSeq == 20 :: 4 :: Nil)

    assert(features(cookie3).size == 4)
    assert(features(cookie3).toSparse.indices.toSeq == 3 :: Nil)
    assert(features(cookie3).toSparse.values.toSeq == 100 :: Nil)

    def assertEmptyFeatures(cookie: String): Unit = {
      assert(features(cookie).size == 4)
      assert(features(cookie).toSparse.indices.toSeq == Nil)
      assert(features(cookie).toSparse.values.toSeq == Nil)
    }

    assertEmptyFeatures(cookie4)
    assertEmptyFeatures(cookie5)
  }

  it should "remove input col" in {
    val encoder = baseEncoder.setKeepInputCol(false)
    val encoded = encoder.transform(dataset)
    assert(encoded.schema.size == dataset.schema.size)
    assert(!encoded.schema.exists(_.name == "sites"))
  }

  it should "fail to encode with empty key set" in {
    val encoder = createEncoder(Array.empty)
    intercept[IllegalArgumentException] {
      encoder.transform(dataset)
    }
  }

  it should "output empty vectors for empty keys with all other disabled" in {
    val encoder = createEncoder(Array.empty)
      .setFailOnEmptyKeys(false)
      .setAllOther(false)
    val features = toFeatures(encoder, dataset)
    assert(features(cookie1).size == 0)
  }

  it should "put all values into all other column for empty keys" in {
    val encoder = createEncoder(Array.empty)
      .setFailOnEmptyKeys(false)
      .setAllOther(true)

    val features = toFeatures(encoder, dataset)

    assert(features(cookie1).toArray.toSeq == Seq(26.0))
    assert(features(cookie2).toArray.toSeq == Seq(24.0))
    assert(features(cookie3).toArray.toSeq == Seq(100.0))

    def assertEmptyFeatures(cookie: String): Unit = {
      assert(features(cookie).size == 1)
      assert(features(cookie).toSparse.indices.toSeq == Nil)
      assert(features(cookie).toSparse.values.toSeq == Nil)
    }

    assertEmptyFeatures(cookie4)
    assertEmptyFeatures(cookie5)
  }

}
