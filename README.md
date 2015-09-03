# Spark Ext

[![Build Status](https://travis-ci.org/collectivemedia/spark-ext.svg?branch=master)](https://travis-ci.org/collectivemedia/spark-ext)

Spark ML transformers, estimator, Spark SQL aggregations, etc that are missing in Apache Spark

## Where to get it

``` scala
resolvers += "Collective Media Bintray" at "https://dl.bintray.com/collectivemedia/releases"
```

And use following library dependencies:

```
libraryDependencies +=  "com.collective.sparkext" %% "sparkext-sql" % "0.0.8"
libraryDependencies +=  "com.collective.sparkext" %% "sparkext-mllib" % "0.0.8"
```

## Testing

    sbt test
    
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
    
#### CollectArray

Aggregation function that collects all values from a column

``` scala
import org.apache.spark.sql.ext.functions._

// collects all sites for cookie (with duplicates)
impressionLog
      .groupBy(col("cookie_id"))
      .agg(collectArray(col("site")))
```

## Spark ML

#### S2 Geometry CellId transformer

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

#### Optimal Binning

Continuous features may need to be transformed to binary format using binning to account for nonlinearity. In general, 
binning attempts to break a set of ordered values into evenly distributed groups, such that each group 
contains approximately the same number of values from the sample.

#### Gather

Inspired by R `tidyr` and `reshape2` packages. Convert `long` `DataFrame` with values
for each key into `wide` `DataFrame`, applying aggregation function if single
key has multiple values

cookie_id | site_id | impressions
----------|---------|-------------
 cookieAA |   123   | 10
 cookieAA |   123   | 5
 cookieAA |   456   | 20
 
``` scala
val gather = new Gather()
      .setPrimaryKeyCols("cookie_id")
      .setKeyCol("site_id")
      .setValueCol("impressions")
      .setOutputCol("sites")
val gathered = gather.transform(siteLog)      
```

cookie_id | sites
----------|-------------
cookieAA  | [{ site_id: 123, impressions: 15.0 }, { site_id: 456, impressions: 20.0 }]

#### Gather Encoder

Encode categorical key-value pairs using dummy variables. 

 cookie_id | sites
 ----------|------------------------------------------------------------------------
 cookieAA  | [{ site_id: 1, impressions: 15.0 }, { site_id: 2, impressions: 20.0 }]
 cookieBB  | [{ site_id: 2, impressions: 7.0 }, { site_id: 3, impressions: 5.0 }]

transformed into

 cookie_id | site_features
 ----------|------------------------
 cookieAA  | [ 15.0 , 20.0 , 0   ]
 cookieBB  | [ 0.0  ,  7.0 , 5.0 ]

Optionally apply dimensionality reduction using `top` transformation:
 - Top coverage, is selecting categorical values by computing the count of distinct users for each value,
   sorting the values in descending order by the count of users, and choosing the top values from the resulting
   list such that the sum of the distinct user counts over these values covers c percent of all users,
   for example, selecting top sites covering 99% of users.
