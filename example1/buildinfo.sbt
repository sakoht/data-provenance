import uk.gov.hmrc.gitstamp._
import GitStampPlugin._

gitStampSettings
val gitInfo = GitStamp.gitStamp

buildInfoKeys := Seq[BuildInfoKey](
  name, version, scalaVersion, sbtVersion,
  BuildInfoKey("gitBranch", gitInfo("Git-Branch")),
  BuildInfoKey("gitRepoClean", gitInfo("Git-Repo-Is-Clean")),
  BuildInfoKey("gitHeadRev", gitInfo("Git-Head-Rev")),
  BuildInfoKey("gitCommitAuthor", gitInfo("Git-Commit-Author")),
  BuildInfoKey("gitCommitDate", gitInfo("Git-Commit-Date")),
  BuildInfoKey("gitDescribe", gitInfo("Git-Describe"))
)

buildInfoPackage := "com.cibo.provenance.examples"

buildInfoOptions ++=
  Seq(
    BuildInfoOption.ToJson,
    BuildInfoOption.BuildTime,
    BuildInfoOption.Traits("com.cibo.provenance.GitBuildInfo")
  )

enablePlugins(BuildInfoPlugin)

