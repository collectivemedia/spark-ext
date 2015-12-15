package org.apache.spark.sql

import com.collective.TestSparkContext
import org.apache.spark.sql.ext.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

import scala.collection.mutable

class CollectArraySpec extends FlatSpec with TestSparkContext {

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("site", StringType),
    StructField("impressions", LongType)
  ))

  val cookie1 = "cookie1"
  val cookie2 = "cookie2"
  val cookie3 = "cookie3"
  val cookie4 = "cookie4"

  val impressionLog = sqlContext.createDataFrame(sc.parallelize(Seq(
    Row(cookie1, "google.com", 10L),
    Row(cookie1, "cnn.com", 14L),
    Row(cookie1, "google.com", 2L),
    Row(cookie2, "bbc.com", 20L),
    Row(cookie2, "auto.com", null),
    Row(cookie2, "auto.com", 1L),
    Row(cookie3, "sport.com", 100L),
    Row(cookie4, null, 12L)
  )), schema)

  "Append to Array" should "append non null values to array" in {
    val rows = impressionLog.select(appendToArray(array("cookie_id"), col("site"))).collect()
    val arrays = rows.map(_.getAs[mutable.WrappedArray[String]](0))
    assert(arrays.count(_.length == 1) == 1)
    assert(arrays.count(_.length == 2) == 7)
  }

  "Concat Arrays" should "concat two arrays" in {
    val rows = impressionLog.select(concatArrays(array("cookie_id"), array("site"))).collect()
    val arrays = rows.map(_.getAs[mutable.WrappedArray[String]](0))
    assert(arrays.count(_.length == 2) == 8)
  }

  "Collect Array" should "collect column values as array" in {
    val cookies = impressionLog
      .select(collectArray(col("cookie_id")))
      .first().getAs[mutable.WrappedArray[String]](0)
    assert(cookies.length == 8)
    assert(cookies.toSet.size == 4)
  }

  it should "collect distinct values as array" in {
    val distinctCookies = impressionLog.select(col("cookie_id"))
      .distinct()
      .select(collectArray(col("cookie_id")))
      .first().getAs[mutable.WrappedArray[String]](0)
    assert(distinctCookies.length == 4)
  }

  it should "collect values after group by" in {
    val result = impressionLog
      .groupBy(col("cookie_id"))
      .agg(collectArray(col("site")))

    val cookieSites = result.collect().map { case Row(cookie: String, sites: mutable.WrappedArray[_]) =>
      cookie -> sites.toSeq
    }.toMap

    assert(cookieSites(cookie1).length == 3)
    assert(cookieSites(cookie2).length == 3)
    assert(cookieSites(cookie3).length == 1)
    assert(cookieSites(cookie4).length == 0)

  }

}
