// This configures SbtBuildInfo for the current project for provenance tracking.
// It presumes the project uses git for source control.
// - Ensure that project/plugins has: addSbtPlugin("com.eed3si9n"     %  "sbt-buildinfo"         % "0.7.0")
// - For a single-module sbt, put this file in the root.
// - For a multi-module sbt, paste this content into your root, and include buildInfoSettings as necessary.

// The sbt git plugins do not work w/ sbt 1.0 yet.  This suffices.
import scala.sys.process._
val gitBranch: String          = ("git status" #| "head -n 1").!!.replace("On branch ", "").stripSuffix("\n")
val gitCommitAuthor: String    = "git log -1 --pretty=%aN".!!.stripSuffix("\n")
val gitCommitDate: String      = "git log -1 --pretty=%aI".!!.stripSuffix("\n")
val gitDescribe: String        = "git log -1 --pretty=%B".!!.stripSuffix("\n")
val gitHeadRev: String         = "git rev-parse HEAD".!!.stripSuffix("\n")
val gitRepoClean: String       = if ("git status".!!.split("\n").length == 3) "true" else "false"

// The settings:
lazy val buildInfoSettings: Seq[Setting[_]] = Seq(
  buildInfoPackage := "com.cibo.provenance.examples",             // Set this to match a package in your project.
  buildInfoObject := "BuildInfo",                                 // This name is arbitrary.
  libraryDependencies ++= Seq(
    "com.cibo" %% "provenance" % "0.1-SNAPSHOT" withSources(),
  ),
  buildInfoKeys := Seq[BuildInfoKey](
    name, version, scalaVersion, sbtVersion,
    BuildInfoKey("gitBranch", gitBranch),
    BuildInfoKey("gitRepoClean", gitRepoClean),
    BuildInfoKey("gitHeadRev", gitHeadRev),
    BuildInfoKey("gitCommitAuthor", gitCommitAuthor),
    BuildInfoKey("gitCommitDate", gitCommitDate),
    BuildInfoKey("gitDescribe", gitDescribe)
  ),
  buildInfoOptions ++=
    Seq(
      BuildInfoOption.ToJson,
      BuildInfoOption.BuildTime,
      BuildInfoOption.Traits("com.cibo.provenance.GitBuildInfo")
    )
)

// If you have a single-module project, this will apply settings to the root.
sbt.internal.DslEntry.fromSettingsDef(buildInfoSettings)
enablePlugins(BuildInfoPlugin)

// If you have a multi-module project, put this file into project/buildmeta.sbt,
// then, in your module(s):
//  .settings(buildInfoSettings)
//  .enablePlugins(BuildInfoPlugin)

// This content should be in project/plugins.sbt (or project/project/plugins.sbt for multi-module).
//  addSbtPlugin("com.eed3si9n"     %  "sbt-buildinfo"         % "0.7.0")

