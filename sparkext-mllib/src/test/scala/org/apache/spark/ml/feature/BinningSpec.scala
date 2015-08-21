package org.apache.spark.ml.feature

import java.util.UUID

import com.collective.TestSparkContext
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, GivenWhenThen, ShouldMatchers}

class BinningSpec extends FlatSpec with GivenWhenThen with ShouldMatchers with TestSparkContext {

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("num_days", IntegerType),
    StructField("ctr", DoubleType),
    StructField("actions", DoubleType)
  ))

  val N = 1000

  def cookieId = UUID.randomUUID().toString

  val users = sqlContext.createDataFrame(sc.parallelize((1 to N).map { i =>
    Row(cookieId, i, math.random, if (math.random > 0.5) 10 * math.random else null)
  }), schema)

  "Optimal Binning" should "compute binning for ctr" in {
    val optimalBinning = new OptimalBinning()
      .setInputCol("ctr")
      .setOutputCol("ctr_bin")
      .setNumBins(5)

    val binning = optimalBinning.fit(users)

    assert(binning.getSplits.length == 6)
    binning.getSplits(1) should be(0.20 +- 0.5)
    binning.getSplits(2) should be(0.40 +- 0.5)
    binning.getSplits(3) should be(0.60 +- 0.5)
    binning.getSplits(4) should be(0.80 +- 0.5)

    val binned = binning.transform(users).collect()
    assert(binned.length == N)
  }

  "Binning" should "bin DoubleType column" in {
    val binning = new Binning()
      .setInputCol("ctr")
      .setOutputCol("ctr_bin")
      .setSplits(Array(0.0, 0.25, 0.5, 0.75, 1.0))

    def validate(ctr: Double, bin: Vector) = {
      assert(bin.size == 4)
      assert(bin.toSparse.indices.length == 1)
      assert(ctr match {
        case v if v >= 0.0 && v < 0.25 => bin.toSparse.indices.head == 0
        case v if v >= 0.25 && v < 0.50 => bin.toSparse.indices.head == 1
        case v if v >= 0.50 && v < 0.75 => bin.toSparse.indices.head == 2
        case v if v >= 0.75 && v < 1.0 => bin.toSparse.indices.head == 3

      })
    }

    val binned = binning.transform(users)
    binned.collect().foreach { case Row(_, _, ctr: Double, _, bin: SparseVector) =>
      validate(ctr, bin)
    }
  }

  it should "bin IntegerType column" in {
    val binning = new Binning()
      .setInputCol("num_days")
      .setOutputCol("num_days_bin")
      .setSplits(Array(0.0, 400, 800, 1000))

    def validate(numDays: Int, bin: Vector) = {
      assert(bin.size == 3)
      assert(bin.toSparse.indices.length == 1)
      assert(numDays match {
        case v if v >= 0 && v < 400 => bin.toSparse.indices.head == 0
        case v if v >= 400 && v < 800 => bin.toSparse.indices.head == 1
        case v if v >= 800 && v <= 1000 => bin.toSparse.indices.head == 2
      })
    }

    val binned = binning.transform(users)

    binned.collect().foreach { case Row(_, numDays: Int, _, _, bin: SparseVector) =>
      validate(numDays, bin)
    }
  }

  it should "fail to bin StringType column" in {
    val binning = new Binning()
      .setInputCol("cookie_id")
      .setOutputCol("cookie_id_bins")
      .setSplits(Array(0.0, 400, 800, 1000))

    intercept[IllegalArgumentException] {
      binning.transform(users)
    }
  }

  it should "bin column with nulls" in {
    val binning = new Binning()
      .setInputCol("actions")
      .setOutputCol("actions_bins")
      .setSplits(Array(0.0, 4.0, 8.0, 10.0))

    binning.transform(users)
  }

}
