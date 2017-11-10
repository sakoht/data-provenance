package com.cibo.provenance

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.tracker.{ResultTracker, ResultTrackerSimple}

case class TestResultTrackerSimple(testSubdirName: String, buildInfo: BuildInfo = DummyBuildInfo)
  extends ResultTrackerSimple(SyncablePath(TestUtils.testOutputBaseDir + "/" + testSubdirName))(buildInfo) {

  import java.io.File
  import org.apache.commons.io.FileUtils

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  val testDataDir: String = f"$testOutputBaseDir/$testSubdirName"

  def deletePreviousOutputs() =
    FileUtils.deleteDirectory(new File(testDataDir))

  def diffVsExpected =
    TestUtils.diffOutputSubdir(testSubdirName)

  def apply(block: (TestResultTrackerSimple) => Unit) = {
    implicit val rt: ResultTracker = this
    deletePreviousOutputs()
    block(this)
    diffVsExpected
  }
}