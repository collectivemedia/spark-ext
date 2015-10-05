package org.apache.spark.mllib.evaluation

import org.apache.spark.mllib.evaluation.binary.{BinaryClassificationMetricComputer, BinaryConfusionMatrix, Recall}

/** Precision. Defined as 1.0 when there are no positive examples. */
private[evaluation] object Reach extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    val totalPopulation = c.numNegatives + c.numPositives
    if (totalPopulation == 0) {
      1.0
    } else {
      (c.numTruePositives.toDouble + c.numFalsePositives.toDouble) / totalPopulation
    }
  }
}

private[evaluation] object Lift extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    Recall(c) / Reach(c)
  }
}
