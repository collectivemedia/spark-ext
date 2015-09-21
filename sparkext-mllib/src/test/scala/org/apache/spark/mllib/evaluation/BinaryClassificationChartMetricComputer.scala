package org.apache.spark.mllib.evaluation

import org.apache.spark.mllib.evaluation.binary.{Recall, BinaryClassificationMetricComputer, BinaryConfusionMatrix}

/** Precision. Defined as 1.0 when there are no positive examples. */
private[evaluation] object AudienceReach extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    val totalPopulation = c.numNegatives + c.numPositives
    if (totalPopulation == 0) {
      1.0
    } else {
      (c.numTruePositives.toDouble + c.numFalseNegatives) / totalPopulation
    }
  }
}

private[evaluation] object Lift extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    Recall(c)/AudienceReach(c)
  }
}