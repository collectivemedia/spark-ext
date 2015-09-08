package com.collective.sparkext.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object InMemorySparkContext {

  private[this] val conf =
    new SparkConf()
      .setMaster("local[4]")
      .set("spark.local.ip", "localhost")
      .set("spark.driver.host", "localhost")
      .set("spark.sql.tungsten.enabled", "false")
      .setAppName("Spark Ext Example App")

  lazy val sc: SparkContext = new SparkContext(conf)

  lazy val sqlContext: SQLContext = new SQLContext(sc)
}


trait InMemorySparkContext {

  lazy val sc: SparkContext = InMemorySparkContext.sc

  lazy val sqlContext: SQLContext = InMemorySparkContext.sqlContext

  def waitFor[T](f: Future[T], timeout: Duration = 5.second): T = {
    Await.result(f, timeout)
  }

}

