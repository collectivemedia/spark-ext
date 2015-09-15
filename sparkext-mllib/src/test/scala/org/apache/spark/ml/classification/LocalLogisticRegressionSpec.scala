package org.apache.spark.ml.classification

import com.collective.TestSparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.scalatest._

import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.ml.TestingUtils._

/**
 * Copy Pasted from Spark LogisticRegressionSuite to verify that nothing is broken
 */
object LocalLogisticRegressionSpec {

  // Generate input of the form Y = logistic(offset + scale*X)
  def generateLogisticInput(
    offset: Double,
    scale: Double,
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array(x1(i)))))
    testData
  }

  /**
   * Generates `k` classes multinomial synthetic logistic input in `n` dimensional space given the
   * model weights and mean/variance of the features. The synthetic data will be drawn from
   * the probability distribution constructed by weights using the following formula.
   *
   * P(y = 0 | x) = 1 / norm
   * P(y = 1 | x) = exp(x * w_1) / norm
   * P(y = 2 | x) = exp(x * w_2) / norm
   * ...
   * P(y = k-1 | x) = exp(x * w_{k-1}) / norm
   * where norm = 1 + exp(x * w_1) + exp(x * w_2) + ... + exp(x * w_{k-1})
   *
   * @param weights matrix is flatten into a vector; as a result, the dimension of weights vector
   *                will be (k - 1) * (n + 1) if `addIntercept == true`, and
   *                if `addIntercept != true`, the dimension will be (k - 1) * n.
   * @param xMean the mean of the generated features. Lots of time, if the features are not properly
   *              standardized, the algorithm with poor implementation will have difficulty
   *              to converge.
   * @param xVariance the variance of the generated features.
   * @param addIntercept whether to add intercept.
   * @param nPoints the number of instance of generated data.
   * @param seed the seed for random generator. For consistent testing result, it will be fixed.
   */
  def generateMultinomialLogisticInput(
    weights: Array[Double],
    xMean: Array[Double],
    xVariance: Array[Double],
    addIntercept: Boolean,
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)

    val xDim = xMean.length
    val xWithInterceptsDim = if (addIntercept) xDim + 1 else xDim
    val nClasses = weights.length / xWithInterceptsDim + 1

    val x = Array.fill[Vector](nPoints)(Vectors.dense(Array.fill[Double](xDim)(rnd.nextGaussian())))

    x.foreach { vector =>
      // This doesn't work if `vector` is a sparse vector.
      val vectorArray = vector.toArray
      var i = 0
      val len = vectorArray.length
      while (i < len) {
        vectorArray(i) = vectorArray(i) * math.sqrt(xVariance(i)) + xMean(i)
        i += 1
      }
    }

    val y = (0 until nPoints).map { idx =>
      val xArray = x(idx).toArray
      val margins = Array.ofDim[Double](nClasses)
      val probs = Array.ofDim[Double](nClasses)

      for (i <- 0 until nClasses - 1) {
        for (j <- 0 until xDim) margins(i + 1) += weights(i * xWithInterceptsDim + j) * xArray(j)
        if (addIntercept) margins(i + 1) += weights((i + 1) * xWithInterceptsDim - 1)
      }
      // Preventing the overflow when we compute the probability
      val maxMargin = margins.max
      if (maxMargin > 0) for (i <- 0 until nClasses) margins(i) -= maxMargin

      // Computing the probabilities for each class from the margins.
      val norm = {
        var temp = 0.0
        for (i <- 0 until nClasses) {
          probs(i) = math.exp(margins(i))
          temp += probs(i)
        }
        temp
      }
      for (i <- 0 until nClasses) probs(i) /= norm

      // Compute the cumulative probability so we can generate a random number and assign a label.
      for (i <- 1 until nClasses) probs(i) += probs(i - 1)
      val p = rnd.nextDouble()
      var y = 0
      breakable {
        for (i <- 0 until nClasses) {
          if (p < probs(i)) {
            y = i
            break
          }
        }
      }
      y
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), x(i)))
    testData
  }

}

// Runs local Logistic Regression
class LocalLogisticRegressionSpec extends AbstractLocalLogisticRegressionSpec("Local", 1)

// Runs default Spark Logistic Regression
class DefaultLogisticRegressionSpec extends AbstractLocalLogisticRegressionSpec("Default", 2)

abstract class AbstractLocalLogisticRegressionSpec(name: String, partitions: Int)
  extends FlatSpec with GivenWhenThen with ShouldMatchers with TestSparkContext {

  import LocalLogisticRegressionSpec._

  private val eps: Double = 1e-5

  lazy val dataset = sqlContext
    .createDataFrame(generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42)).repartition(partitions)

  lazy val binaryDataset = {
    val nPoints = 10000
    val weights = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

    sqlContext.createDataFrame(
      generateMultinomialLogisticInput(weights, xMean, xVariance, true, nPoints, 42)).repartition(partitions)
  }

  s"$name LogisticRegression" should "test logistic regression: Predictor, Classifier methods" in {
    val lr = new LocalLogisticRegression

    val model = lr.fit(dataset)
    assert(model.numClasses === 2)

    val results = model.transform(dataset)

    // Compare rawPrediction with probability
    results.select("rawPrediction", "probability").collect().foreach {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
    }

    // Compare prediction with probability
    results.select("prediction", "probability").collect().foreach {
      case Row(pred: Double, prob: Vector) =>
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }
  }

  it should "test binary logistic regression with intercept with L1 regularization" in {
    val trainer1 = (new LocalLogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true)
    val trainer2 = (new LocalLogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept) -0.05627428
       data.V2       .
       data.V3       .
       data.V4     -0.04325749
       data.V5     -0.02481551
     */
    val interceptR1 = -0.05627428
    val weightsR1 = Vectors.dense(0.0, 0.0, -0.04325749, -0.02481551)

    assert(model1.intercept ~== interceptR1 relTol 1E-2)
    assert(model1.weights ~= weightsR1 absTol 2E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)  0.3722152
       data.V2       .
       data.V3       .
       data.V4     -0.1665453
       data.V5       .
     */
    val interceptR2 = 0.3722152
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.1665453, 0.0)

    assert(model2.intercept ~== interceptR2 relTol 1E-2)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }

}
