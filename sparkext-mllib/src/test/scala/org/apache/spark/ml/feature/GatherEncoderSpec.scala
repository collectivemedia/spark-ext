package org.apache.spark.ml.feature

import com.collective.TestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

class GatherEncoderSpec extends FlatSpec with TestSparkContext {

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("sites", ArrayType(StructType(Seq(
      StructField("site", StringType),
      StructField("impressions", LongType
      ))), containsNull = false))
  ))

  val cookie1 = "cookie1"
  val cookie2 = "cookie2"
  val cookie3 = "cookie3"

  val dataset = sqlContext.createDataFrame(sc.parallelize(
    Seq.fill(250)(Row(cookie1, Array( // 250 * 2 = 500   // total: 500  // cover: 50%
      Row("google.com", 12L),
      Row("cnn.com", 14L)
    ))) ++
    Seq.fill(100)(Row(cookie2, Array( // 100 * 3 = 300   // total: 800  // cover: 80%
      Row("bbc.com", 20L),
      Row("auto.com", 1L),
      Row("moto.com", 3L)
    ))) ++
    Seq.fill(80)(Row(cookie3, Array( // 80              // total: 880   // cover: 88%
      Row("sport.com", 100L)
    ))) ++
    Seq.fill(50)(Row(cookie3, Array( // 50              // total: 930   // cover: 93%
      Row("netflix.com", 1L)
    ))) ++
    Seq.fill(40)(Row(cookie3, Array( // 40              // total: 970   // cover: 97%
      Row("amazon.com", 1L)
    ))) ++
    Seq.fill(30)(Row(cookie3, Array( // 30              // total: 1000  // cover: 100%
      Row("imdb.com", 1L)
    )))
  ), schema)

  val baseEncoder = new GatherEncoder()
    .setInputCol("sites")
    .setOutputCol("features")
    .setKeyCol("site")
    .setValueCol("impressions")

  "Gather Encoder" should "collect all keys when cover is 100.0" in {
    val encoder = baseEncoder.setCover(100.0)
    val features = encoder.fit(dataset)
    assert(features.keys.length == 9)
  }

  it should "exclude imdb.com for 95% coverage" in {
    val encoder = baseEncoder.setCover(95.0)
    val features = encoder.fit(dataset)
    assert(features.keys.length == 8)
    assert(!features.keys.contains("imdb.com"))
  }

  it should "exclude amazon.com for 90% coverage" in {
    val encoder = baseEncoder.setCover(90.0)
    val features = encoder.fit(dataset)
    assert(features.keys.length == 7)
    assert(!features.keys.contains("amazon.com"))
  }

  it should "exclude netflix.com for 85% coverage" in {
    val encoder = baseEncoder.setCover(85.0)
    val features = encoder.fit(dataset)
    assert(features.keys.length == 6)
    assert(!features.keys.contains("netflix.com"))
  }

  it should "exclude sport.com for 75% coverage" in {
    val encoder = baseEncoder.setCover(75.0)
    val features = encoder.fit(dataset)
    assert(features.keys.length == 5)
    assert(!features.keys.contains("sport.com"))
  }

}
