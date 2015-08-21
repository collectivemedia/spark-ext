# Spark Ext

Spark ML transformers, estimator, Spark SQL aggregations, etc that are missing in Apache Spark

## Spark SQL

``` scala
val schema = StructType(Seq(
  StructField("cookie_id", StringType),
  StructField("site", StringType),
  StructField("impressions", LongType)
))

val impressionLog = sqlContext.createDataFrame(sc.parallelize(Seq(
  Row("cookie_1", "google.com", 10L),
  Row("cookie_2", "cnn.com", 14L),
  ...
)), schema)
 ```
    
### CollectArray

Aggregation function that collects all values from a column

``` scala
import org.apache.spark.sql.ext.functions._

// collects all sites for cookie (with duplicates)
impressionLog
      .groupBy(col("cookie_id"))
      .agg(collectArray(col("site")))
```

## Spark ML

## S2 Geometry CellId transformer

Gets Google S2 Geometry CellId from decimal `lat` and `lon`

``` scala
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
 
  val s2CellTransformer = new S2CellTransformer().setLevel(6)
  s2CellTransformer.transform(cities)
```

