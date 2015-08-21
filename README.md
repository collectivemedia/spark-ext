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

## Optimal Binning

Continuous features may need to be transformed to binary format using binning to account for nonlinearity. In general, 
binning attempts to break a set of ordered values into evenly distributed groups, such that each group 
contains approximately the same number of values from the sample.

## Gather

Inspired by R 'tidyr' and 'reshape2' packages. Convert 'long' DataFrame with values
for each category into 'wide' DataFrame, applying aggregation function if single
category has multiple values

cookie_id | site_id | impressions
----------|---------|-------------
 cookieAA |   123   | 10
 cookieAA |   123   | 5
 cookieAA |   456   | 20
 
gathered using 'sum' aggregate
 
cookie_id | output_col
----------|-------------
cookieAA  | [{ site_id: 123, impressions: 15.0 }, { site_id: 456, impressions: 20.0 }]
