
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

buildInfoPackage := "com.cibo.provenance.examples"
buildInfoObject := "BuildInfo"

buildInfoOptions ++=
  Seq(
    BuildInfoOption.ToJson,
    BuildInfoOption.BuildTime,
    BuildInfoOption.Traits("com.cibo.provenance.GitBuildInfo")
  )

enablePlugins(BuildInfoPlugin)

