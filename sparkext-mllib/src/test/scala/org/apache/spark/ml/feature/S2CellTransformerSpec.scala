package org.apache.spark.ml.feature

import com.collective.TestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, GivenWhenThen}


class S2CellTransformerSpec extends FlatSpec with GivenWhenThen with TestSparkContext {

  val schema = StructType(Seq(
    StructField("city", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType)
  ))

  val cities = sqlContext.createDataFrame(sc.parallelize(Seq(
    Row("New York", 40.7142700, -74.0059700),
    Row("London", 51.50722, -0.12750),
    Row("Princeton", 40.3487200, -74.6590500)
  )), schema)

  def cellMap(rows: Array[Row]): Map[String, String] = {
    rows.map { case Row(city: String, _, _, cell: String) => city -> cell }.toMap
  }

  "S2 Cell Transformer" should "compute S2 Cell Id for level = 6" in {
    Given("S2 Cell Transformer with level = 6")
    val s2CellTransformer = new S2CellTransformer().setLevel(6)
    val transformed = s2CellTransformer.transform(cities)
    val cells = cellMap(transformed.collect())
    Then("New York should be in the same cell with Princeton")
    assert(cells("New York") == cells("Princeton"))
  }

  it should "compute S2 Cell Id for level = 12" in {
    Given("S2 Cell Transformer with level = 12")
    val s2CellTransformer = new S2CellTransformer().setLevel(12)
    val transformed = s2CellTransformer.transform(cities)
    val cells = cellMap(transformed.collect())
    Then("all cities should in it's onw cell")
    assert(cells.values.toSet.size == 3)
  }

}
