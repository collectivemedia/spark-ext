package org.apache.spark.ml.feature

import com.collective.TestSparkContext
import org.apache.spark.ml.attribute.{NominalAttribute, Attribute}
import org.scalatest.FlatSpec

class StringToShortIndexerSpec extends FlatSpec with TestSparkContext {

  "StringToShortIndexer" should "assign correct index for columns" in {
    val data = sc.parallelize(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")), 2)
    val df = sqlContext.createDataFrame(data).toDF("id", "label")
    val indexer = new StringToShortIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)

    val transformed = indexer.transform(df)
    val attr = Attribute.fromStructField(transformed.schema("labelIndex"))
      .asInstanceOf[NominalAttribute]
    assert(attr.values.get === Array("a", "c", "b"))
    val output = transformed.select("id", "labelIndex").map { r =>
      (r.getInt(0), r.getShort(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 0), (1, 2), (2, 1), (3, 0), (4, 0), (5, 1))
    assert(output === expected)
  }

}
