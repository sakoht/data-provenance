package com.cibo.provenance

import com.cibo.provenance.tracker.ResultTracker

/**
  * Created by ssmith on 10/22/17.
  *
  * Apps using this library generate a BuildInfo object at compile time that extends this trait.
  */

trait BuildInfo extends Serializable {
  def name: String
  def version: String
  def scalaVersion: String
  def sbtVersion: String

  def builtAtString: String
  def builtAtMillis: Long

  def commitId: String
  def buildId: String = builtAtString.replace(":",".").replace(" ",".").replace("-",".")

  override def toString: String = f"BuildInfo($commitId@$buildId)"

  lazy val abbreviate: BuildInfoBrief = BuildInfoBrief(commitId, buildId)
}

trait GitBuildInfo extends BuildInfo with Serializable {
  def gitBranch: String
  def gitRepoClean: String
  def gitHeadRev: String
  def gitCommitAuthor: String
  def gitCommitDate: String
  def gitDescribe: String

  def commitId: String = gitHeadRev
}

object NoBuildInfo extends BuildInfo with Serializable {
  // This is used by objects of UnknownProvenance as a placeholder for when BuildInfo does not apply.
  lazy val name: String = "-"
  lazy val version: String = "-"
  lazy val scalaVersion: String = "-"
  lazy val sbtVersion: String = "-"

  lazy val builtAtString: String = "-"
  lazy val builtAtMillis: Long = 0L

  lazy val commitId: String = "-"
  override lazy val buildId: String = "-"
}

case class BuildInfoBrief(commitId: String, override val buildId: String) extends BuildInfo {
  lazy val impl: BuildInfo = NoBuildInfo

  def name: String = impl.name
  def version: String = impl.version
  def scalaVersion: String = impl.scalaVersion
  def sbtVersion: String = impl.sbtVersion

  def builtAtString: String = impl.builtAtString
  def builtAtMillis: Long = impl.builtAtMillis

  override lazy val abbreviate: BuildInfoBrief = this
}
