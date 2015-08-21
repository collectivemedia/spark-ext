package org.apache.spark.ml.feature

import breeze.linalg.DenseVector
import breeze.optimize.{ApproximateGradientFunction, DiffFunction, LBFGS}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, BinaryAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, NumericType, StructType}


private[feature] trait BinningBase extends Params with HasInputCol with HasOutputCol

class OptimalBinning(override val uid: String) extends Estimator[Binning] with BinningBase with SplitOptimizer {

  def this() = this(Identifiable.randomUID("optimalBinning"))

  val numBins: Param[Int] = new Param[Int](this, "numBins", "Number of bins",
    ParamValidators.gt(2))

  val sampleSize: Param[Int] = new Param[Int](this, "sampleSize", "Size of a sample used for split optimizer",
    ParamValidators.gt(1000))

  def getNumBins: Int = $(numBins)

  def getSampleSize: Int = $(sampleSize)

  def setNumBins(value: Int): this.type = set(numBins, value)

  def setSampleSize(value: Int): this.type = set(sampleSize, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(
    numBins    -> 5,
    sampleSize -> 10000
  )

  override def fit(dataset: DataFrame): Binning = {
    transformSchema(dataset.schema, logging = true)

    val notNulls = dataset.filter(col($(inputCol)).isNotNull)
    val inputSize = notNulls.count()
    val fraction = if ($(sampleSize) >= inputSize) 1.0D else $(sampleSize).toDouble / inputSize
    val sample = notNulls.select(col($(inputCol)).cast(DoubleType)).sample(withReplacement = false, fraction)

    val x = sample.collect().map(_.getDouble(0))

    log.debug(s"Collected sample size of: ${x.length}")

    // Doesn't make any sense to do binning if no enough sample points available
    require(x.length > ${numBins} * 10,
      s"Number of sample points for binning is too small")

    // Find optimal split with -Inf, +Inf bounds
    val splits = Double.NegativeInfinity +: optimalSplit(x, $(numBins) - 1) :+ Double.PositiveInfinity
    val bins = splits.sliding(2).map(bin => s"[${bin.mkString(", ")})").toArray
    log.debug(s"Calculated optimal split. Bins: ${bins.mkString(", ")}")

    copyValues(new Binning(uid).setSplits(splits).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    inputDataType match {
      case _: NumericType =>
      case other =>
        throw new IllegalArgumentException(s"Data type $other is not supported.")
    }
    // Names of bins are not available at this point
    val attrGroup = new AttributeGroup($(outputCol), $(numBins))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): Estimator[Binning] = defaultCopy(extra)
}


/**
 * Based on [[org.apache.spark.ml.feature.Bucketizer Bucketizer]], except that
 * instead of [[org.apache.spark.ml.attribute.NominalAttribute NominalAttribute]] it
 * outputs [[org.apache.spark.ml.attribute.AttributeGroup AttributeGroup]] column
 */
final class Binning(override val uid: String)
  extends Model[Binning] with BinningBase {

  def this() = this(Identifiable.randomUID("binning"))

  val splits: DoubleArrayParam = new DoubleArrayParam(this, "splits",
    "Split points for mapping continuous features into bins. With n+1 splits, there are n " +
      "bins. A bin defined by splits x,y holds values in the range [x,y) except the last " +
      "bin, which also includes y. The splits should be strictly increasing. " +
      "Values at -inf, inf must be explicitly provided to cover all Double values; " +
      "otherwise, values outside the splits specified will be treated as errors.",
    Bucketizer.checkSplits)

  def getSplits: Array[Double] = $(splits)

  def setSplits(value: Array[Double]): this.type = set(splits, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val numBins = ${splits}.length - 1
    val t = udf { feature: Double =>
      val binIdx = Bucketizer.binarySearchForBuckets($(splits), feature).toInt
      Vectors.sparse(numBins, Seq((binIdx, 1.0)))
    }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol)).cast(DoubleType)).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    inputDataType match {
      case _: NumericType =>
      case other =>
        throw new IllegalArgumentException(s"Data type $other is not supported.")
    }
    val bins = $(splits).sliding(2).map(bin => s"[${bin.mkString(", ")})").toArray
    val attrs: Array[Attribute] = bins.map(bin => new BinaryAttribute(Some(bin)))
    val attrGroup = new AttributeGroup($(outputCol), attrs)
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): Binning = {
    defaultCopy[Binning](extra).setParent(parent)
  }
}

/**
 * Compute optimal split to have the same number of points in each bucket/bin
 */
trait SplitOptimizer {

  protected def fromDiff(diff: Array[Double]): Array[Double] = {
    diff.scanLeft(0D)((acc, v) => acc + v).drop(1)
  }

  protected def toDiff(values: Array[Double]): Array[Double] = {

    if (values.isEmpty) {
      Array.empty
    } else if (values.length == 1) {
      values
    } else {
      val diff = values.sliding(2) map {
        case s if s.length == 2 => s(1) - s(0)
        case s => sys.error(s"Unexpected sliding window: $s")
      }
      (values.head +: diff.toSeq).toArray
    }
  }

  protected def quantiles(x: Array[Double])(percentiles: Array[Double]): Array[Double] = {
    val as = x.sorted
    percentiles.map({ p =>
      val i = p * (as.length - 1)
      val lb = i.toInt
      val ub = math.ceil(i).toInt
      val w = i - lb
      val quantile = as(lb) * (1 - w) + as(ub) * w
      quantile
    })(collection.breakOut)
  }

  /**
   * Mean squared error from ideal split
   */
  protected def error(counts: Array[Int]): Double = {
    val sum = counts.sum
    val bins = counts.length
    counts.map(_ - (sum / bins)).map(math.pow(_, 2)).sum / bins
  }

  protected class OptimalSplitTargetFunction(
    x: Array[Double],
    splits: Int
  ) extends DiffFunction[DenseVector[Double]] {

    // Calculate starting point based on quantile split
    val init: DenseVector[Double] = {
      val percentile = (1 to splits) map (_.toDouble / (splits + 1))
      DenseVector.apply(toDiff(quantiles(x)(percentile.toArray)))
    }

    // Target minimization function
    private val targetFunction: DenseVector[Double] => Double =
      p => error(counts(p))

    def counts(p: DenseVector[Double]): Array[Int] = {
      val splits = Double.NegativeInfinity +: fromDiff(p.toArray) :+ Double.PositiveInfinity

      val count = splits.sliding(2) map {
        case split if split.length == 2 =>
          val low = split(0)
          val high = split(1)
          val filter = (v: Double) => v >= low && v < high
          x.count(filter)

        case split => sys.error(s"Unexpected split: $split")
      }

      count.toArray
    }

    private val gradient = new ApproximateGradientFunction(targetFunction)

    def calculate(p: DenseVector[Double]): (Double, DenseVector[Double]) = {
      (targetFunction(p), gradient.gradientAt(p))
    }
  }

  /**
   * Compute optimal split values to get uniformly
   * distributed number of points in each bin
   *
   * @param x input data that needs to be splitted
   * @param splits number of splits
   * @param maxIter max iterations for LBFGS optimizer
   * @param m memory parameter for LBFGS optimizer
   * @return
   */
  def optimalSplit(
    x: Array[Double],
    splits: Int,
    maxIter: Int = 100,
    m: Int = 3
  ): Array[Double] = {

    // Binning requires at least 3 splits
    require(splits >= 3, s"Target splits should greater or equal 3")

    val lbfgs = new LBFGS[DenseVector[Double]](maxIter, m)
    val f = new OptimalSplitTargetFunction(x, splits)

    fromDiff(lbfgs.minimize(f, f.init).toArray)
  }

}
