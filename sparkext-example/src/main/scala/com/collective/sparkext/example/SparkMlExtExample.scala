package com.collective.sparkext.example

import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, GatherEncoder, S2CellTransformer, Gather}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.evaluation.BinaryModelMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._

object SparkMlExtExample extends App with Sites with Geo with Response {

  import sqlContext.implicits._

  turnOffLogging()

  println(s"Run Spark ML Ext Example application")

  println(s"Sites data frame size = ${sitesDf.count()}")
  println(s"Geo data frame size = ${geoDf.count()}")
  println(s"Response data frame size = ${responseDf.count()} ")

  // Gather site visitation log
  val gatherSites = new Gather()
    .setPrimaryKeyCols(Sites.cookie)
    .setKeyCol(Sites.site)
    .setValueCol(Sites.impressions)
    .setOutputCol("sites")

  // Transform lat/lon into S2 Cell Id
  val s2Transformer = new S2CellTransformer()
    .setLevel(5)
    .setCellCol("s2_cell")

  // Gather S2 CellId log
  val gatherS2Cells = new Gather()
    .setPrimaryKeyCols(Geo.cookie)
    .setKeyCol("s2_cell")
    .setValueCol(Geo.impressions)
    .setOutputCol("s2_cells")

  // Gather raw data into wide rows
  val gatheredSites = gatherSites.transform(sitesDf)
  val gatheredCells = gatherS2Cells.transform(s2Transformer.transform(geoDf))

  // Assemble input dataset
  val dataset = responseDf.as("response")
    .join(gatheredSites, responseDf(Response.cookie) === gatheredSites(Sites.cookie))
    .join(gatheredCells, responseDf(Response.cookie) === gatheredCells(Sites.cookie))
    .select(
      $"response.*",
      $"sites",
      $"s2_cells"
    ).cache()

  println(s"Input dataset size = ${dataset.count()}")

  dataset.show(10)

  // Split dataset into test/train sets
  val trainPct = 0.1
  val Array(trainSet, testSet) = dataset.randomSplit(Array(1 - trainPct, trainPct))

  // Setup ML Pipeline stages

  // Encode site data
  val encodeSites = new GatherEncoder()
    .setInputCol("sites")
    .setOutputCol("sites_f")
    .setKeyCol(Sites.site)
    .setValueCol(Sites.impressions)

  // Encode S2 Cell data
  val encodeS2Cells = new GatherEncoder()
    .setInputCol("s2_cells")
    .setOutputCol("s2_cells_f")
    .setKeyCol("s2_cell")
    .setValueCol(Geo.impressions)
    .setCover(0.95)

  // Assemble feature vectors together
  val assemble = new VectorAssembler()
    .setInputCols(Array("sites_f", "s2_cells_f"))
    .setOutputCol("features")

  // Extract features label information
  val dummyPipeline = new Pipeline()
    .setStages(Array(encodeSites, encodeS2Cells, assemble))
  val out = dummyPipeline.fit(dataset).transform(dataset)
  val attrGroup = AttributeGroup.fromStructField(out.schema("features"))

  val attributes = attrGroup.attributes.get
  println(s"Num features = ${attributes.length}")
  attributes.zipWithIndex.foreach { case (attr, idx) =>
    println(s" - $idx = $attr")
  }

  // Build logistic regression using featurized statistics
  val lr = new LogisticRegression()
    .setFeaturesCol("features")
    .setLabelCol(Response.response)
    .setProbabilityCol("probability")

  // Define pipeline with 4 stages
  val pipeline = new Pipeline()
    .setStages(Array(encodeSites, encodeS2Cells, assemble, lr))

  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol(Response.response)

  val crossValidator = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)

  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.elasticNetParam, Array(0.1, 0.5))
    .build()

  crossValidator.setEstimatorParamMaps(paramGrid)
  crossValidator.setNumFolds(2)

  println(s"Train model on train set")
  val cvModel = crossValidator.fit(trainSet)

  println(s"Score test set")
  val testScores = cvModel.transform(testSet)

  val scoreAndLabels = testScores
    .select(col("probability"), col(Response.response))
    .map { case Row(probability: DenseVector, label: Double) =>
    val predictedActionProbability = probability(1)
    (predictedActionProbability, label)
  }

  println("Evaluate model")
  val metrics = new BinaryModelMetrics(scoreAndLabels)
  val auc = metrics.areaUnderROC()

  println(s"Model AUC: $auc")

  private def turnOffLogging(): Unit = {
    Logger.getRootLogger.removeAllAppenders()
    Logger.getRootLogger.addAppender(new NullAppender())
  }
}

trait Sites extends InMemorySparkContext {

  object Sites {
    val cookie = "cookie"
    val site = "site"
    val impressions = "impressions"

    val schema = StructType(Array(
      StructField(cookie, StringType),
      StructField(site, StringType),
      StructField(impressions, IntegerType)
    ))
  }

  lazy val sitesDf: DataFrame = {
    val lines = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/sites.csv")).getLines()
    val rows = lines.map(_.split(",")).drop(1) collect {
      case Array(cookie, site, impressions) => Row(cookie, site, impressions.toInt)
    }
    val rdd = sc.parallelize(rows.toSeq)
    sqlContext.createDataFrame(rdd, Sites.schema)
  }

}

trait Geo extends InMemorySparkContext {

  object Geo {
    val cookie = "cookie"
    val lat = "lat"
    val lon = "lon"
    val impressions = "impressions"

    val schema = StructType(Array(
      StructField(cookie, StringType),
      StructField(lat, DoubleType),
      StructField(lon, DoubleType),
      StructField(impressions, IntegerType)
    ))
  }

  lazy val geoDf: DataFrame = {
    val lines = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/geo.csv")).getLines()
    val rows = lines.map(_.split(",")).drop(1) collect {
      case Array(cookie, lat, lon, impressions) => Row(cookie, lat.toDouble, lon.toDouble, impressions.toInt)
    }
    val rdd = sc.parallelize(rows.toSeq)
    sqlContext.createDataFrame(rdd, Geo.schema)
  }

}

trait Response extends InMemorySparkContext {

  object Response {
    val cookie = "cookie"
    val response = "response"

    val schema = StructType(Array(
      StructField(cookie, StringType),
      StructField(response, DoubleType)
    ))
  }

  lazy val responseDf: DataFrame = {
    val lines = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/response.csv")).getLines()
    val rows = lines.map(_.split(",")).drop(1) collect {
      case Array(cookie, response) => Row(cookie, response.toDouble)
    }
    val rdd = sc.parallelize(rows.toSeq)
    sqlContext.createDataFrame(rdd, Response.schema)
  }

}

