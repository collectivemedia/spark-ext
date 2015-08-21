package org.apache.spark.sql

import com.collective.TestSparkContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FlatSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.ext.functions._

import scala.collection.mutable

class ExtAggregatesSpec extends FlatSpec with TestSparkContext {

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

  "Ext Aggregates" should "collect column values as array" in {
    val cookies = impressionLog
      .select(collectArray(col("cookie_id")))
      .first().getAs[mutable.WrappedArray[String]](0)
    assert(cookies.length == 7)
    assert(cookies.toSet.size == 3)
  }

  it should "collect distinct values as array" in {
    val distinctCookies = impressionLog.select(col("cookie_id"))
      .distinct()
      .select(collectArray(col("cookie_id")))
      .first().getAs[mutable.WrappedArray[String]](0)
    assert(distinctCookies.length == 3)
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

  }

}
