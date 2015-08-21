package org.apache.spark.ml.feature

import com.collective.TestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._

import scala.collection.mutable

class GatherSpec extends FlatSpec with GivenWhenThen with ShouldMatchers with TestSparkContext {

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("site", StringType),
    StructField("impressions", LongType)
  ))

  val cookie1 = "cookie1"
  val cookie2 = "cookie2"
  val cookie3 = "cookie3"

  val impressionLog = sqlContext.createDataFrame(sc.parallelize(Seq(
    Row(cookie1, "google.com", 10L),
    Row(cookie1, "cnn.com", 14L),
    Row(cookie1, "google.com", 2L),
    Row(cookie2, "bbc.com", 20L),
    Row(cookie2, "auto.com", null),
    Row(cookie2, "auto.com", 1L),
    Row(cookie3, "sport.com", 100L)
  )), schema)

  "Gather Transformer" should "transform 'long' DataFrame into 'wide'" in {
    val gather = new Gather()
      .setPrimaryKeyCols("cookie_id")
      .setKeyCol("site")
      .setValueCol("impressions")
      .setOutputCol("sites")

    val gathered = gather.transform(impressionLog)

    val lookupMap: Map[String, Map[String, Double]] =
      gathered.collect().map { case Row(cookieId: String, map: mutable.WrappedArray[_]) =>
        val imps = map.map { case Row(site: String, impressions: Double) => site -> impressions }.toMap
        cookieId -> imps
      }.toMap

    assert(lookupMap(cookie1)("google.com") == 12.0)
    assert(lookupMap(cookie1)("cnn.com") == 14.0)
    assert(lookupMap(cookie2)("bbc.com") == 20.0)
    assert(lookupMap(cookie2)("auto.com") == 1.0)
    assert(lookupMap(cookie3)("sport.com") == 100.0)

  }

}
