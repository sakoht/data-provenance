// This configures SbtBuildInfo for the current project for provenance tracking.
// It presumes a single-module root project.  See the other examples for multi-module.

// Setup:
// - put this file into the root of your project, next-to build.sbt, possibly named buildinfo.sbt
// - put this into your project/plugins.sbt: `addSbtPlugin("com.eed3si9n" %  "sbt-buildinfo" % "0.7.0")``
// - edit the `buildInfoPackage` to refer to a package owned exclusively by your project

buildInfoPackage := "com.cibo.provenance.example1" 

buildInfoObject := "BuildInfo"

libraryDependencies ++= Seq(
    "com.cibo" %% "provenance" % "0.2-SNAPSHOT" withSources(),
)

// This presumes the project uses git for source control.
// The sbt git plugins do not work w/ sbt 1.0 yet.  This suffices.
import scala.sys.process._
val gitBranch: String          = ("git status" #| "head -n 1").!!.replace("On branch ", "").stripSuffix("\n")
val gitCommitAuthor: String    = "git log -1 --pretty=%aN".!!.stripSuffix("\n")
val gitCommitDate: String      = "git log -1 --pretty=%aI".!!.stripSuffix("\n")
val gitDescribe: String        = "git log -1 --pretty=%B".!!.stripSuffix("\n")
val gitHeadRev: String         = "git rev-parse HEAD".!!.stripSuffix("\n")
val gitRepoClean: String       = if ("git status".!!.split("\n").length == 3) "true" else "false"

buildInfoKeys := Seq[BuildInfoKey](
    name, version, scalaVersion, sbtVersion,
    BuildInfoKey("gitBranch", gitBranch),
    BuildInfoKey("gitRepoClean", gitRepoClean),
    BuildInfoKey("gitHeadRev", gitHeadRev),
    BuildInfoKey("gitCommitAuthor", gitCommitAuthor),
    BuildInfoKey("gitCommitDate", gitCommitDate),
    BuildInfoKey("gitDescribe", gitDescribe)
)

buildInfoOptions ++= Seq(
  BuildInfoOption.ToJson,
  BuildInfoOption.BuildTime,
  BuildInfoOption.Traits("com.cibo.provenance.GitBuildInfo")
)

enablePlugins(BuildInfoPlugin)

