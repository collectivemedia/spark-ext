package org.apache.spark.ml.feature

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
      StructField("site_id", IntegerType),
      StructField("impressions", LongType
    ))), containsNull = true))
  ))

  val cookie1 = "cookie1"
  val cookie2 = "cookie2"
  val cookie3 = "cookie3"
  val cookie4 = "cookie4"
  val cookie5 = "cookie5"

  val (google, googleId) = "google.com" -> 1
  val (cnn, cnnId) = "cnn.com" -> 2
  val (bbc, bbcId) = "bbc.com" -> 3
  val (auto, autoId) = "auto.com" -> 4
  val (moto, motoId) = "moto.com" -> 5
  val (sport, sportId) = "sport.com" -> 6

  val dataset = sqlContext.createDataFrame(sc.parallelize(Seq(
    Row(cookie1, Array(
      Row(google, googleId, 12L),
      Row(cnn, cnnId, 14L)
    )),
    Row(cookie2, Array(
      Row(bbc, bbcId, 20L),
      Row(auto, autoId, 1L),
      Row(moto, motoId, 3L)
    )),
    Row(cookie3, Array(
      Row(sport, sportId, 100L)
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

  val sites: Array[Any] = Array(google, bbc, cnn)
  val siteIds: Array[Any] = Array(googleId, bbcId, cnnId)

  def toFeatures(encoder: GatherEncoderModel, dataset: DataFrame): Map[String, Vector] = {
    val encodedDf = encoder.transform(dataset).select("cookie_id", "features")
    encodedDf.collect().map { case Row(cookieId: String, features: Vector) =>
      cookieId -> features
    }.toMap
  }

  "Gather Encoder Model" should "encode categories ignoring all other" in {
    val sitesEncoder = createEncoder(sites).setAllOther(false)
    val siteIdsEncoder = createEncoder(siteIds).setKeyCol("site_id").setAllOther(false)

    // Check that type of the keys doesn't matter
    val siteFeatures = toFeatures(sitesEncoder, dataset)
    val idFeatures = toFeatures(siteIdsEncoder, dataset)
    assert(siteFeatures == idFeatures)

    assert(siteFeatures(cookie1).size == 3)
    assert(siteFeatures(cookie1).toSparse.indices.toSeq == 0 :: 2 :: Nil)
    assert(siteFeatures(cookie1).toSparse.values.toSeq == 12 :: 14 :: Nil)

    assert(siteFeatures(cookie2).size == 3)
    assert(siteFeatures(cookie2).toSparse.indices.toSeq == 1 :: Nil)
    assert(siteFeatures(cookie2).toSparse.values.toSeq == 20 :: Nil)

    def assertEmptyFeatures(cookie: String): Unit = {
      assert(siteFeatures(cookie).size == 3)
      assert(siteFeatures(cookie).toSparse.indices.toSeq == Nil)
      assert(siteFeatures(cookie).toSparse.values.toSeq == Nil)
    }

    assertEmptyFeatures(cookie3)
    assertEmptyFeatures(cookie4)
    assertEmptyFeatures(cookie5)
  }

  it should "encode categories with all other" in {
    val sitesEncoder = createEncoder(sites).setAllOther(true)
    val features = toFeatures(sitesEncoder, dataset)

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
    val sitesEncoder = createEncoder(sites).setKeepInputCol(false)
    val encoded = sitesEncoder.transform(dataset)
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
    val sitesEncoder = createEncoder(Array.empty)
      .setFailOnEmptyKeys(false)
      .setAllOther(false)
    val features = toFeatures(sitesEncoder, dataset)
    assert(features(cookie1).size == 0)
  }

  it should "put all values into all other column for empty keys" in {
    val sitesEncoder = createEncoder(Array.empty)
      .setFailOnEmptyKeys(false)
      .setAllOther(true)

    val features = toFeatures(sitesEncoder, dataset)

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
