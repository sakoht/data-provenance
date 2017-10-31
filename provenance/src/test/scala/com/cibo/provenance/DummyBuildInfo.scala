package com.cibo.provenance

import java.time.Instant

/**
  * Created by ssmith on 10/23/17.
  *
  * Apps will have a BuildInfo that captures the source control and build information.
  * This library does not.  For tests, we use this placeholder.
  *
  */

trait DummyBuildInfo extends BuildInfo {
  def name: String = "DUMMY-NAME"
  def version: String = "DUMMY-VERSION"
  def scalaVersion: String = "DUMMY-SCALA-VERSION"
  def sbtVersion: String = "DUMMY-SBT-VERSION"

  private lazy val startTime: Instant = Instant.now

  lazy val builtAtString: String = startTime.toString
  lazy val builtAtMillis: Long = startTime.toEpochMilli

  lazy val commitId: String = "DUMMY-COMMIT"
}

object DummyBuildInfo extends DummyBuildInfo

