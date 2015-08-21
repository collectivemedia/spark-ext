import sbt._

object VersionScheme {

  object Keys {

    val isRelease = Def.settingKey[Boolean]("True if this is a release")

    val versionPrefix = Def.settingKey[String](
      "Prefix of the version string")

  }

}
