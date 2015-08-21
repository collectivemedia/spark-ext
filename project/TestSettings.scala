import sbt.Keys._
import sbt._
import org.scalastyle.sbt.ScalastylePlugin


object TestSettings {

  private[this] lazy val checkScalastyle = taskKey[Unit]("checkScalastyle")

  def testSettings: Seq[Def.Setting[_]] = Seq(
    fork in Test := true,

    // Run Scalastyle as a part of tests
    checkScalastyle := ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
    test in Test <<= (test in Test) dependsOn checkScalastyle,

    // Disable logging in all tests
    javaOptions in Test += "-Dlog4j.configuration=log4j-turned-off.properties",

    // Generate JUnit test reports
    testOptions in Test <+= (target in Test) map {
      t => Tests.Argument(TestFrameworks.ScalaTest, "-u", (t / "test-reports").toString)
    }
  )

}
