import com.typesafe.sbt.SbtGit.{GitKeys => git}

name in ThisBuild := "spark-ext"

organization in ThisBuild := "com.collective.sparkext"

scalaVersion in ThisBuild := "2.10.6"

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
  "Scalaz Bintray Repo"  at "https://dl.bintray.com/scalaz/releases",
  // Apache
  "Apache Releases"      at "https://repository.apache.org/content/repositories/releases/",
  "Apache Staging"       at "https://repository.apache.org/content/repositories/staging",
  "Apache Snapshots"     at "https://repository.apache.org/content/repositories/snapshots",
  // Conjars
  "Conjars"              at "http://conjars.org/repo",
  // Collective
  "Collective Bintray"   at "https://dl.bintray.com/collectivemedia/releases"
)


// Spark Ext Project

def SparkExtProject(path: String) =
  Project(path, file(path))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(TestSettings.testSettings: _*)
    .settings(bintrayRepository := "releases")
    .settings(bintrayOrganization := Some("collectivemedia"))
    .settings(
      apiMappings ++= {
        val cp: Seq[Attributed[File]] = (fullClasspath in Test).value
        def findManagedDependency(organization: String, name: String): File = {
          cp.foreach(println)
          ( for {
            entry <- cp
            module <- entry.get(moduleID.key)
            if module.organization == organization
            if module.name.startsWith(name)
            jarFile = entry.data
          } yield jarFile
          ).head
        }
        Map(
          findManagedDependency("org.apache.spark", "spark") -> url("https://spark.apache.org/docs/latest/api/scala/")
        )
      }
    )

// Aggregate all projects & disable publishing root project

lazy val root = Project("spark-ext", file(".")).
  settings(publish :=()).
  settings(publishLocal :=()).
  settings(unidocSettings: _*).
  settings(unidocSettings: _*).
  settings(site.settings ++ ghpages.settings: _*).
  settings(
    autoAPIMappings := true,
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "/"),
    git.gitRemoteRepo := "git@github.com:collectivemedia/spark-ext.git"
  ).
  aggregate(sparkextSql, sparkextMllib, sparkextTest, sparkextExample)

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

lazy val sparkextExample =
  SparkExtProject("sparkext-example")
    .dependsOn(sparkextMllib)
    .settings(publish :=())
    .settings(publishLocal :=())

