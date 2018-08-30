import scala.sys.process._

buildInfoPackage := "com.cibo.provenance.example1" 
buildInfoObject := "BuildInfo"

buildInfoKeys := Seq[BuildInfoKey](
  name,
  version,
  scalaVersion,
  sbtVersion,
  "gitBranch"             -> git.gitCurrentBranch.value,
  "gitUncommittedChanges"  -> git.gitUncommittedChanges.value,
  "gitHash"               -> git.gitHeadCommit.value.get,
  "gitMessage"            -> git.gitHeadMessage.value.get,
  "gitCommitAuthor"       -> "git log -1 --pretty=%aN".!!.stripSuffix("\n"),
  "gitCommitDate"         -> git.gitHeadCommitDate.value.get,
  "gitHashShort"          -> git.gitHeadCommit.value.get.substring(0,8),
)

buildInfoOptions ++= Seq(
  BuildInfoOption.ToJson,
  BuildInfoOption.BuildTime,
  BuildInfoOption.Traits("com.cibo.provenance.BuildInfoGit")
)

enablePlugins(BuildInfoPlugin)

