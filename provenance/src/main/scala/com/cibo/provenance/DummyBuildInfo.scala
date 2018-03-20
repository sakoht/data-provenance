package com.cibo.provenance

import java.time.Instant

/**
  * Created by ssmith on 10/23/17.
  *
  * Apps will have a BuildInfo that captures the source control and build information.
  * This is used as a placeholder in test cases for this library, and for library users.
  *
  */
object DummyBuildInfo extends GitBuildInfo {
  def name: String = "DUMMY-NAME"
  def version: String = "DUMMY-VERSION"
  def scalaVersion: String = "DUMMY-SCALA-VERSION"
  def sbtVersion: String = "DUMMY-SBT-VERSION"

  lazy val startTime: Instant = Instant.parse("1955-11-12T22:04:00.000Z") // we don't need roads

  def gitBranch: String = "DUMMY-BRANCH"
  def gitRepoClean: String = "true"
  def gitHeadRev: String = "DUMMY-COMMIT"
  def gitCommitAuthor: String = "DUMMY-AUTHOR"
  def gitCommitDate: String = "1919-01-01T00:01:02.000Z"
  def gitDescribe: String = "DUMMY-DESCRIBE"

  lazy val builtAtString: String = startTime.toString
  lazy val builtAtMillis: Long = startTime.toEpochMilli
}


