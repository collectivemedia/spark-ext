
object Dependencies {

  import Dependency._

  val sparkExtSql =
    Seq(
        sparkSql % "provided"
      , Test.scalaTest
    )

  val sparkExtMllib =
    Seq(
        sparkMLLib % "provided"
      , s2Geometry
      , Test.scalaTest
    )

  val sparkExtTest =
    Seq(
        sparkSql % "provided"
      , Test.scalaTest
    )

  val sparkExtExample =
    Seq(
      sparkMLLib
    )

}
