package org.apache.spark.mllib.evaluation

import com.collective.TestSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{GivenWhenThen, FlatSpec}

/**
 * We are just testing gains and lift methods.
 * Since code for this class was copied from spark 1.5.0
 */
class BinaryModelMetricsSpec extends FlatSpec with GivenWhenThen with TestSparkContext {

  val scoreAndLabels: RDD[(Double, Double)] = sc.parallelize(Seq(
    (0.8, 0.0),
    (0.7, 1.0),
    (0.3, 0.0),
    (0.9, 1.0),
    (0.6, 0.0),
    (0.6, 1.0),
    (0.6, 0.0),
    (0.8, 1.0),
    (0.2, 0.0),
    (0.5, 1.0)
  ), 1)

  val modelMetricsNoBin = new BinaryModelMetrics(scoreAndLabels)

  behavior of "BinaryModelMetrics"

  it should "compute gains chart" in {
    Given(s"score and labels set with 7 unique scores")
    When("creating BinaryModelMetrics without bins specified")
    val modelMetricsNoBin = new BinaryModelMetrics(scoreAndLabels)
    val gainsChart = modelMetricsNoBin.gains()

    Then("resulting gains chart should have 9 pair of coordinates")
    assert(gainsChart.count() === 9)
  }


  it should "compute gains chart with numBins = 3" in {
    Given(s"score and labels set with 7 unique scores")
    When("creating BinaryModelMetrics with 3 bins specified")
    val modelMetricsNoBin = new BinaryModelMetrics(scoreAndLabels, 3)
    val gainsChart = modelMetricsNoBin.gains()

    val expectedGainsPoints = (1 + Math.ceil(7.toDouble/(7/3)) + 1).toInt
    Then(s"resulting gains chart should have $expectedGainsPoints pair of coordinates")
    assert(gainsChart.count() === expectedGainsPoints)
  }
}
