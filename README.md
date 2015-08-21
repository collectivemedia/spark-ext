# Spark Ext

Spark ML transformers, estimator, Spark SQL aggregations, etc that are missing in Apache Spark

## Spark SQL

    import org.apache.spark.sql.ext.functions._
    
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

### CollectArray

Aggregation function that collects all values from a column

    // Get all sites for cookie (with duplicates)
    impressionLog
          .groupBy(col("cookie_id"))
          .agg(collectArray(col("site")))
