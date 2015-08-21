
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
      , Test.scalaTest
    )

  val sparkExtTest =
    Seq(
        sparkSql
      , Test.scalaTest
    )

}
