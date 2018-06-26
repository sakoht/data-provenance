package com.cibo.provenance

import io.circe.{Decoder, Encoder}

/**
  * SbtBuildInfo uses this as the base trait for projects using `git`.
  * A custom object extending this trait will be created and inserted into the build.
  *
  * When that object is serialized and later re-loaded, it comes back a a GitBuildInfoSaved (below).
  */
trait BuildInfoGit extends BuildInfo with Serializable {
  def gitBranch: String
  def gitRepoClean: String
  def gitHeadRev: String
  def gitCommitAuthor: String
  def gitCommitDate: String
  def gitDescribe: String

  def commitId: String = gitHeadRev

  def codec = BuildInfoGit.codec
  def toEncoded = codec.encoder.apply(this)
}

/**
  * A re-loaded instance of a saved GitBuildInfo.
  *
  * @param name
  * @param version
  * @param scalaVersion
  * @param sbtVersion
  * @param builtAtString
  * @param builtAtMillis
  * @param gitBranch
  * @param gitRepoClean
  * @param gitHeadRev
  * @param gitCommitAuthor
  * @param gitCommitDate
  * @param gitDescribe
  */
case class BuildInfoGitSaved(
  name: String,
  version: String,
  scalaVersion: String,
  sbtVersion: String,
  builtAtString: String,
  builtAtMillis: Long,
  gitBranch: String,
  gitRepoClean: String,
  gitHeadRev: String,
  gitCommitAuthor: String,
  gitCommitDate: String,
  gitDescribe: String
) extends BuildInfoGit


object BuildInfoGit {
  private val encoder: Encoder[BuildInfoGit] =
    Encoder.forProduct15(
      "vcs", "name", "version", "scalaVersion", "sbtVersion", "builtAtString", "builtAtMillis", "commitId", "buildId",
      "gitBranch", "gitRepoClean", "gitHeadRev", "gitCommitAuthor", "gitCommitDate", "gitDescribe"
    ) {
      bi =>
        Tuple15(
          "git", bi.name, bi.version, bi.scalaVersion, bi.sbtVersion, bi.builtAtString, bi.builtAtMillis,
          bi.commitId, bi.buildId, bi.gitBranch, bi.gitRepoClean, bi.gitHeadRev, bi.gitCommitAuthor,
          bi.gitCommitDate, bi.gitDescribe
        )
    }

  private val decoder: Decoder[BuildInfoGit] =
    Decoder.forProduct15(
      "vcs", "name", "version", "scalaVersion", "sbtVersion", "builtAtString", "builtAtMillis", "commitId", "buildId",
      "gitBranch", "gitRepoClean", "gitHeadRev", "gitCommitAuthor", "gitCommitDate", "gitDescribe"
    ) {
      (
        vcs: String, name: String, version: String, scalaVersion: String, sbtVersion: String,
        builtAtString: String, builtAtMillis: Long, commitId: String, buildId: String,
        gitBranch: String, gitRepoClean: String, gitHeadRev: String,
        gitCommitAuthor: String, gitCommitDate: String, gitDescribe: String
      ) =>
        BuildInfoGitSaved(
          name, version, scalaVersion, sbtVersion, builtAtString, builtAtMillis,
          gitBranch, gitRepoClean, gitHeadRev, gitCommitAuthor, gitCommitDate, gitDescribe
        )
    }

  implicit val codec: Codec[BuildInfoGit] = Codec(encoder, decoder)
}

