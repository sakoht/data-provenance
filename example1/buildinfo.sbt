import scala.sys.process._

// The sbt-git-stamp plugin does the same thing but does not work on sbt 1.0,
// and requires 4 repos all compiled with 2.10 macros. :(

val gitBranch         = ("git status" #| "head -n 1" !!).replace("On branch ", "")
val gitRepoClean      = (if (("git status" !!).split("\n").size == 3) "true" else "false") 
val gitHeadRev        = "git rev-parse HEAD" !!
val gitCommitAuthor   = "git log -1 --pretty=%ad" !!
val gitCommitDate     = "git log -1 --pretty=%aI" !!
val gitDescribe       = "git log -1 --pretty=%B" !!

buildInfoKeys := Seq[BuildInfoKey](
  name, version, scalaVersion, sbtVersion,
  BuildInfoKey("gitBranch", gitBranch),
  BuildInfoKey("gitRepoClean", gitRepoClean),
  BuildInfoKey("gitHeadRev", gitHeadRev),
  BuildInfoKey("gitCommitAuthor", gitCommitAuthor),
  BuildInfoKey("gitCommitDate", gitCommitDate),
  BuildInfoKey("gitDescribe",  gitDescribe)
)

buildInfoPackage := "com.cibo.provenance.examples"

buildInfoOptions ++=
  Seq(
    BuildInfoOption.ToJson,
    BuildInfoOption.BuildTime,
    BuildInfoOption.Traits("com.cibo.provenance.GitBuildInfo")
  )

enablePlugins(BuildInfoPlugin)

