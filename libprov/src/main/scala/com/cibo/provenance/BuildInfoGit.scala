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
  def gitUncommittedChanges: Boolean
  def gitHash: String
  def gitHashShort: String
  def gitCommitAuthor: String
  def gitCommitDate: String
  def gitMessage: String

  def commitId: String = gitHash

  def codec: Codec[BuildInfoGit] = BuildInfoGit.codec
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
  * @param gitUncommittedChanges
  * @param gitHash
  * @param gitHashShort
  * @param gitCommitAuthor
  * @param gitCommitDate
  * @param gitMessage
  */
case class BuildInfoGitSaved(
  name: String,
  version: String,
  scalaVersion: String,
  sbtVersion: String,
  builtAtString: String,
  builtAtMillis: Long,
  gitBranch: String,
  gitUncommittedChanges: Boolean,
  gitHash: String,
  gitHashShort: String,
  gitCommitAuthor: String,
  gitCommitDate: String,
  gitMessage: String
) extends BuildInfoGit


object BuildInfoGit {
  private val encoder: Encoder[BuildInfoGit] =
    Encoder.forProduct16(
      "vcs", "name", "version", "scalaVersion", "sbtVersion", "builtAtString", "builtAtMillis", "commitId", "buildId",
      "gitBranch", "gitUncommittedChanges", "gitHash", "gitHashShort", "gitCommitAuthor", "gitCommitDate", "gitMessage"
    ) {
      bi =>
        Tuple16(
          "git", bi.name, bi.version, bi.scalaVersion, bi.sbtVersion, bi.builtAtString, bi.builtAtMillis,
          bi.commitId, bi.buildId, bi.gitBranch, bi.gitUncommittedChanges, bi.gitHash, bi.gitHashShort, bi.gitCommitAuthor,
          bi.gitCommitDate, bi.gitMessage
        )
    }

  private val decoder: Decoder[BuildInfoGit] =
    Decoder.forProduct16(
      "vcs", "name", "version", "scalaVersion", "sbtVersion", "builtAtString", "builtAtMillis", "commitId", "buildId",
      "gitBranch", "gitUncommittedChanges", "gitHash", "gitHashShort", "gitCommitAuthor", "gitCommitDate", "gitMessage"
    ) {
      (
        vcs: String, name: String, version: String, scalaVersion: String, sbtVersion: String,
        builtAtString: String, builtAtMillis: Long, commitId: String, buildId: String,
        gitBranch: String, gitUncommittedChanges: Boolean, gitHash: String, gitHashShort: String,
        gitCommitAuthor: String, gitCommitDate: String, gitMessage: String
      ) =>
        BuildInfoGitSaved(
          name, version, scalaVersion, sbtVersion, builtAtString, builtAtMillis,
          gitBranch, gitUncommittedChanges, gitHash, gitHashShort, gitCommitAuthor, gitCommitDate, gitMessage
        )
    }

  implicit val codec: Codec[BuildInfoGit] = Codec(encoder, decoder)
}

