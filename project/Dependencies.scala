
object Dependencies {

  import Dependency._

  val sparkExtSql =
    Seq(
        sparkSql
      , Test.scalaTest
    )

  val sparkExtMllib =
    Seq(
        sparkMLLib
      , s2Geometry
      , Test.scalaTest
    )

  val sparkExtTest =
    Seq(
        sparkSql
      , Test.scalaTest
    )

}
