import sbt._


object Dependency {

  object V {

    val Spark              = "v1.6.0-rc1"
    val S2Geometry         = "1.0"

    val ScalaTest          = "2.2.4"

  }

  val sparkSql            = "org.apache.spark"           %% "spark-sql"      % V.Spark
  val sparkMLLib          = "org.apache.spark"           %% "spark-mllib"    % V.Spark

  val s2Geometry          = "com.google.common.geometry"  % "s2-geometry"    % V.S2Geometry intransitive()

  object Test {

    val scalaTest         = "org.scalatest"              %% "scalatest"      % V.ScalaTest  % "test"

  }

}
