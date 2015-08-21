import sbt._

import scala.language.postfixOps

object ShellPrompt {

  object devnull extends ProcessLogger {
    def info (s: => String): Unit = {}
    def error (s: => String): Unit = {}
    def buffer[T] (f: => T): T = f
  }

  val current = """\*\s+([\w-/]+)""".r

  def gitBranches = "git branch --no-color" lines_! devnull mkString

  val buildShellPrompt = {
    (state: State) => {
      val currBranch =
        current findFirstMatchIn gitBranches map (_ group(1)) getOrElse "-"
      val currProject = Project.extract (state).currentProject.id
      "%s:%s> ".format (
        currProject, currBranch
      )
    }
  }
}
