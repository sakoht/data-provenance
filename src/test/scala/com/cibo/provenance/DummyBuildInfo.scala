package com.cibo.provenance

/**
  * Created by ssmith on 10/23/17.
  *
  * Apps will have a BuildInfo that captures the source control and build information.
  * This library does not.  For tests, we use this placeholder.
  *
  */

object DummyBuildInfo extends BuildInfo {
  def name: String = "DUMMY-NAME"
  def version: String = "DUMMY-VERSION"
  def scalaVersion: String = "DUMMY-SCALA-VERSION"
  def sbtVersion: String = "DUMMY-SBT-VERSION"

  lazy val builtAtString: String = "DUMMY-BUILT-AT"
  lazy val builtAtMillis: Long = 0L

  def commitId: String = "DUMMY-COMMIT"
  override def buildId: String = "DUMMY-BUILD"
}
