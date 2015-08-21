import sbt._

object GitHelper {

  def headSha(): String = Process("git rev-parse --short HEAD").!!.stripLineEnd

}
