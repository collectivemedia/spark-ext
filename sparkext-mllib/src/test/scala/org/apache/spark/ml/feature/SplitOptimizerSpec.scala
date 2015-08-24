package org.apache.spark.ml.feature

import org.scalatest._

class SplitOptimizerSpec extends FlatSpec with ShouldMatchers with SplitOptimizer {

  "SplitOptimizer" should "get from diff to original values" in {
    val diff = Array(0.1, 0.21, 0.05, 0.5)
    assert(fromDiff(diff).toSeq == Seq(0.1, 0.31, 0.36, 0.86))
  }

  it should "get diff from original values" in {
    val values = Array(0.1, 0.31, 0.37, 0.88)
    assert(toDiff(values).toSeq == Seq(0.1, 0.21, 0.06, 0.51))
  }

  it should "calculate perfect split of 9" in {
    val x = (0 until 100).toArray.map(_.toDouble + math.random - math.random)

    val splits = optimalSplit(x, 9)
    assert(splits.length == 9)

    splits.zipWithIndex.foreach { case (s, idx) =>
      s should be ((idx + 1) * (x.length.toDouble / 10) +- 2.5)
    }
  }

  it should "calculate perfect split for highly skewed data" in {

    // R: x <- exp(rnorm(1000))

    // Heavy right skewed data
    val g = breeze.stats.distributions.Gaussian(0, 1)
    val skewed = g.sample(1000).map(d => math.exp(d)).toArray

    val splits = optimalSplit(skewed, 9)
    assert(splits.length == 9)

    val cnt = counts(skewed)(splits)
    assert(cnt.sum == skewed.length)

    cnt.foreach { count =>
      count should be((skewed.length / 10) +- 5)
    }
  }

  private def counts(x: Array[Double])(p: Seq[Double]): Seq[Int] = {
    val splits = Double.NegativeInfinity +: p :+ Double.PositiveInfinity

    val count = splits.sliding(2) map { case split =>
      val low = split(0)
      val high = split(1)
      val filter = (v: Double) => v >= low && v < high
      x.count(filter)
    }

    count.toSeq
  }

}
