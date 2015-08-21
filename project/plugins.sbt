libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.5"

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

// Dependency graph
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.5")

// Check Scala style
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

// Publish unified documentation to site
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.2")

// Publish to bintray
addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

// Publish unidoc to Github pages
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.7.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.2")
