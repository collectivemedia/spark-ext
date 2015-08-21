package com.collective

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object TestSparkContext {

  private[this] val conf =
    new SparkConf()
      .setMaster("local[1]")
      .set("spark.local.ip","localhost")
      .set("spark.driver.host","localhost")
      .setAppName("Spark Ext Test")

  lazy val sc: SparkContext = new SparkContext(conf)

  lazy val sqlContext: SQLContext = new SQLContext(sc)
}


trait TestSparkContext {

  lazy val sc: SparkContext = TestSparkContext.sc

  lazy val sqlContext: SQLContext = TestSparkContext.sqlContext

  def waitFor[T](f: Future[T], timeout: Duration = 5.second): T = {
    Await.result(f, timeout)
  }

}
