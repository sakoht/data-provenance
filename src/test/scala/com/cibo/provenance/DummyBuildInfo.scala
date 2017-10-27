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

  def commitId: String = "DUMMY-COMMIT"
  def buildId: String = "DUMMY-BUILD"
}
