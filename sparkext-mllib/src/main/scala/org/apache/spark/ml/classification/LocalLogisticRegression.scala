package org.apache.spark.ml.classification

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Logging, SparkException}

import scala.collection.mutable

/**
 * Local version of [[LogisticRegression]]
 * When DataFrame is too small that it can easily fit into single node it doesn't make sense
 * to build model using RDD, it can be built on single node. Essentially using Spark
 * as distributed Executor
 */
class LocalLogisticRegression(override val uid: String)
  extends ProbabilisticClassifier[Vector, LocalLogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams with Logging {

  def this() = this(Identifiable.randomUID("locallogreg"))

  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setDefault(elasticNetParam -> 0.0)

  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  override def getThreshold: Double = super.getThreshold

  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  override def getThresholds: Array[Double] = super.getThresholds

  private def trainLocal(instances: Array[(Double, Vector)]): (LogisticRegressionModel, Array[Double]) = {

    val (summarizer, labelSummarizer) =
      instances.foldLeft((new MultivariateOnlineSummarizer, new MultiClassSummarizer)) {
        case ((summarizer: MultivariateOnlineSummarizer, labelSummarizer: MultiClassSummarizer),
          (label: Double, features: Vector)) =>
          (summarizer.add(features), labelSummarizer.add(label))
      }

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numClasses = histogram.length
    val numFeatures = summarizer.mean.size

    if (numInvalid != 0) {
      val msg = s"Classification labels should be in {0 to ${numClasses - 1} " +
        s"Found $numInvalid invalid labels."
      logError(msg)
      throw new SparkException(msg)
    }

    if (numClasses > 2) {
      val msg = s"Currently, LogisticRegression with ElasticNet in ML package only supports " +
        s"binary classification. Found $numClasses in the input dataset."
      logError(msg)
      throw new SparkException(msg)
    }

    val featuresMean = summarizer.mean.toArray
    val featuresStd = summarizer.variance.toArray.map(math.sqrt)

    val regParamL1 = $(elasticNetParam) * $(regParam)
    val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

    val costFun = new LocalLogisticCostFun(instances, numClasses, $(fitIntercept), $(standardization),
      featuresStd, featuresMean, regParamL2)

    val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
      new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
    } else {
      def regParamL1Fun = (index: Int) => {
        // Remove the L1 penalization on the intercept
        if (index == numFeatures) {
          0.0
        } else {
          if ($(standardization)) {
            regParamL1
          } else {
            // If `standardization` is false, we still standardize the data
            // to improve the rate of convergence; as a result, we have to
            // perform this reverse standardization by penalizing each component
            // differently to get effectively the same objective function when
            // the training dataset is not standardized.
            if (featuresStd(index) != 0.0) regParamL1 / featuresStd(index) else 0.0
          }
        }
      }
      new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
    }

    val initialWeightsWithIntercept =
      Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures)

    if ($(fitIntercept)) {
      /*
         For binary logistic regression, when we initialize the weights as zeros,
         it will converge faster if we initialize the intercept such that
         it follows the distribution of the labels.

         {{{
         P(0) = 1 / (1 + \exp(b)), and
         P(1) = \exp(b) / (1 + \exp(b))
         }}}, hence
         {{{
         b = \log{P(1) / P(0)} = \log{count_1 / count_0}
         }}}
       */
      initialWeightsWithIntercept.toArray(numFeatures)
        = math.log(histogram(1).toDouble / histogram(0).toDouble)
    }

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialWeightsWithIntercept.toBreeze.toDenseVector)

    val (weights, intercept, objectiveHistory) = {
      /*
         Note that in Logistic Regression, the objective history (loss + regularization)
         is log-likelihood which is invariance under feature standardization. As a result,
         the objective history from optimizer is the same as the one in the original space.
       */
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        arrayBuilder += state.adjustedValue
      }

      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        logError(msg)
        throw new SparkException(msg)
      }

      /*
         The weights are trained in the scaled space; we're converting them back to
         the original space.
         Note that the intercept in scaled space and original space is the same;
         as a result, no scaling is needed.
       */
      val rawWeights = state.x.toArray.clone()
      var i = 0
      while (i < numFeatures) {
        rawWeights(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
        i += 1
      }

      if ($(fitIntercept)) {
        (Vectors.dense(rawWeights.dropRight(1)).compressed, rawWeights.last, arrayBuilder.result())
      } else {
        (Vectors.dense(rawWeights).compressed, 0.0, arrayBuilder.result())
      }
    }

    val model = copyValues(new LogisticRegressionModel(uid, weights, intercept))

    (model, objectiveHistory)
  }

  override protected def train(dataset: DataFrame): LogisticRegressionModel = {

    if (dataset.rdd.partitions.length == 1) {
      log.info(s"Build LogisticRegression in local mode")

      val (model, objectiveHistory) = extractLabeledPoints(dataset).map {
        case LabeledPoint(label: Double, features: Vector) => (label, features)
      }.mapPartitions { instances =>
        Seq(trainLocal(instances.toArray)).toIterator
      }.first()

      val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
        model.transform(dataset),
        $(probabilityCol),
        $(labelCol),
        objectiveHistory)
      model.setSummary(logRegSummary)

    } else {
      log.info(s"Fallback to distributed LogisticRegression")

      val that = classOf[LogisticRegression].getConstructor(classOf[String]).newInstance(uid)
      val logisticRegression = copyValues(that)
      // Scala Reflection magic to call protected train method
      val ru = scala.reflect.runtime.universe
      import ru._
      val m = ru.runtimeMirror(logisticRegression.getClass.getClassLoader)
      val im = m.reflect(logisticRegression)
      val trainMethod = typeOf[LogisticRegression].declaration(newTermName("train")).asMethod
      val mm = im.reflectMethod(trainMethod)
      mm.apply(dataset).asInstanceOf[LogisticRegressionModel]
    }
  }

  override def copy(extra: ParamMap): LocalLogisticRegression = defaultCopy(extra)
}

/**
 * Local version of [[LogisticCostFun]]
 */
private class LocalLogisticCostFun(
  data: Array[(Double, Vector)],
  numClasses: Int,
  fitIntercept: Boolean,
  standardization: Boolean,
  featuresStd: Array[Double],
  featuresMean: Array[Double],
  regParamL2: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    val numFeatures = featuresStd.length
    val w = Vectors.fromBreeze(weights)

    val logisticAggregator = data.foldLeft(new LogisticAggregator(w, numClasses, fitIntercept,
      featuresStd, featuresMean)) {
      case (aggregator, (label, features)) => aggregator.add(label, features)
    }

    val totalGradientArray = logisticAggregator.gradient.toArray

    // regVal is the sum of weight squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      w.foreachActive { (index, value) =>
        // If `fitIntercept` is true, the last term which is intercept doesn't
        // contribute to the regularization.
        if (index != numFeatures) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardization) {
              totalGradientArray(index) += regParamL2 * value
              value * value
            } else {
              if (featuresStd(index) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(index) * featuresStd(index))
                totalGradientArray(index) += regParamL2 * temp
                value * temp
              } else {
                0.0
              }
            }
          }
        }
      }
      0.5 * regParamL2 * sum
    }

    (logisticAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}

