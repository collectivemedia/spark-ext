package org.apache.spark.mllib.evaluation

import com.collective.TestSparkContext
import org.apache.spark.mllib.evaluation.binary.{Recall, BinaryConfusionMatrixImpl, BinaryLabelCounter}
import org.scalatest.{GivenWhenThen, FlatSpec}

class BinaryModelMetricComputerSpec extends FlatSpec with GivenWhenThen with TestSparkContext {

  val confusions = Seq(
    BinaryConfusionMatrixImpl(new BinaryLabelCounter(1, 0), new BinaryLabelCounter(5, 5)),
    BinaryConfusionMatrixImpl(new BinaryLabelCounter(5, 2), new BinaryLabelCounter(5, 5))
  )

  behavior of "AudienceReach"
  confusions foreach {
    b => {
      it should s"compute proper reach for $b" in {
        Given(s"confusion matrix entry $b")
        val expectedAudienceReach = (b.count.numPositives + b.count.numNegatives).toDouble /
          (b.totalCount.numNegatives + b.totalCount.numPositives)

        Then(s"audience reach should be equal to $expectedAudienceReach")
        assert(AudienceReach(b) === expectedAudienceReach)
      }
    }
  }

  behavior of "Lift"
  confusions foreach {
    b => {
      it should s"compute proper lift for $b" in {
        Given(s"confusion matrix entry $b")
        val expectedAudienceReach = (b.count.numPositives + b.count.numNegatives).toDouble /
          (b.totalCount.numNegatives + b.totalCount.numPositives)
        val expectedLift = Recall(b)/expectedAudienceReach

        Then(s"lift should be equal to $expectedLift")
        assert(Lift(b) === expectedLift)
      }
    }
  }
}
