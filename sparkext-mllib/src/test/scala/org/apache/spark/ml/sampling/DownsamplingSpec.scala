package org.apache.spark.ml.sampling

import java.util.UUID

import com.collective.TestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._

import scala.util.Random


class DownsamplingSpec extends FlatSpec with GivenWhenThen with ShouldMatchers with TestSparkContext {

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("label", DoubleType)
  ))

  def cookieId = UUID.randomUUID().toString

  def positives(n: Int): Seq[Row] = Seq.fill(n)(Row(cookieId, 1.0))
  def negatives(n: Int): Seq[Row] = Seq.fill(n)(Row(cookieId, 0.0))

  val dataset1 = sqlContext.createDataFrame(sc.parallelize(Random.shuffle(positives(100) ++ negatives(900))), schema)
  val dataset2 = sqlContext.createDataFrame(sc.parallelize(Random.shuffle(positives(100) ++ negatives(9000))), schema)

  "Downsampling" should "skip sampling if class ratio is below threshold" in {
    val downsampling = new Downsampling()
      .setLabelCol("label")
      .setOutputCol("sample_weight")
      .setPrimaryClass(1.0)

    val model = downsampling.fit(dataset1)
    assert(model.sampleFraction.isEmpty)

    val sampled = model.transform(dataset1)
    assert(sampled.schema("sample_weight").dataType == DoubleType)

    val w = sampled.select("sample_weight").collect().map(_.getDouble(0)).toSet
    assert(w.size == 1)
    assert(w.head == 1.0)
  }

  it should "sample negatives if class ratio is above threshold" in {
    val downsampling = new Downsampling()
      .setLabelCol("label")
      .setOutputCol("sample_weight")
      .setMaxClassRatio(29.0)
      .setPrimaryClass(1.0)

    val model = downsampling.fit(dataset2)
    assert(model.sampleFraction.isDefined)
    val fraction = model.sampleFraction.get
    val expectedFraction = 2900.0 / 9000
    fraction should (be >= 0.9 * expectedFraction and be <= 1.1 * expectedFraction)

    val sampled = model.transform(dataset2)
    assert(sampled.schema("sample_weight").dataType == DoubleType)

    sampled.count() should (be >= 2900L and be <= 3100L)

    val sampleWeight = sampled.select("label", "sample_weight").collect().map(r => r.getDouble(0) -> r.getDouble(1)).toMap
    assert(sampleWeight.size == 2)

    val expectedSampleWeight = 9000.0 / 2900
    sampleWeight(1.0) should equal (1.0)
    sampleWeight(0.0) should (be >= 0.9 * expectedSampleWeight and be <= 1.1 * expectedSampleWeight)
  }

}
