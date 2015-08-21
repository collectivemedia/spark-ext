import sbt._


object Dependency {

  object V {

    val Spark              = "1.5.0-SNAPSHOT"

    val ScalaTest          = "2.2.4"

  }

  val sparkSql            = "org.apache.spark"          %% "spark-sql"                   % V.Spark % "provided"
  val sparkMLLib          = "org.apache.spark"          %% "spark-mllib"                 % V.Spark % "provided"

  object Test {

    val scalaTest         = "org.scalatest"             %% "scalatest"                   % V.ScalaTest  % "test"

  }

}
