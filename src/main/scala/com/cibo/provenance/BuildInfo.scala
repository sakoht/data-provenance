package com.cibo.provenance

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

  def commitId: String
  def buildId: String
}

trait GitBuildInfo extends BuildInfo with Serializable {
  def gitBranch: String
  def gitRepoClean: String
  def gitHeadRev: String
  def gitCommitAuthor: String
  def gitCommitDate: String
  def gitDescribe: String

  def commitId: String = gitHeadRev
  def buildId: String = "DUMMY-BUILD"
}

object NoBuildInfo extends BuildInfo with Serializable {
  // This is used by objects of UnknownProvenance as a placeholder for when BuildInfo does not apply.
  def name: String = "-"
  def version: String = "-"
  def scalaVersion: String = "-"
  def sbtVersion: String = "-"

  def commitId: String = "-"
  def buildId: String = "-"
}
