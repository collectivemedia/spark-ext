name in ThisBuild := "spark-ext"

organization in ThisBuild := "com.collective"

scalaVersion in ThisBuild := "2.10.5"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
  //"-Ywarn-unused-import" // turn on whenever you want to discover unused imports, 2.11+
  //"-Xfuture"
  //"-Xlint",
  //"-Ywarn-value-discard"
)

// compiler optimization (2.11+ only)
// still experimental
// scalacOptions       += "-Ybackend:o3"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalastyleFailOnError in ThisBuild := true

maxErrors in ThisBuild := 5

shellPrompt in ThisBuild := ShellPrompt.buildShellPrompt

resolvers in ThisBuild ++= Seq(
  // Maven local
  Resolver.mavenLocal,
  // Cloudera
  "Cloudera"             at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  // Typesafe
  Resolver.typesafeRepo("releases"),
  Resolver.typesafeRepo("snapshots"),
  Resolver.typesafeIvyRepo("releases"),
  Resolver.typesafeIvyRepo("snapshots"),
  // Sonatype
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  // Scalaz
  "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases",
  // Apache
  "Apache Releases"      at "http://repository.apache.org/content/repositories/releases/",
  "Apache Snapshots"     at "http://repository.apache.org/content/repositories/snapshots",
  // Conjars
  "Conjars"              at "http://conjars.org/repo"
)


// Spark Ext Project

def SparkExtProject(path: String) =
  Project(path, file(path))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(TestSettings.testSettings: _*)
    .settings(bintrayRepository := "releases")
    .settings(bintrayOrganization := Some("collectivemedia"))

// Aggregate all projects & disable publishing root project

lazy val root = Project("spark-ext", file(".")).
  settings(publish :=()).
  settings(publishLocal :=()).
  settings(unidocSettings: _*).
  aggregate(sparkextSql, sparkextMllib, sparkextTest)

// Spark Ext Projects

lazy val sparkextSql =
  SparkExtProject("sparkext-sql")
    .dependsOn(sparkextTest % "test->test")

lazy val sparkextMllib =
  SparkExtProject("sparkext-mllib")
    .dependsOn(sparkextSql)
    .dependsOn(sparkextTest % "test->test")

lazy val sparkextTest =
  SparkExtProject("sparkext-test")
    .settings(publish :=())
    .settings(publishLocal :=())
