package com.cibo.provenance

import org.scalatest.{FunSpec, Matchers}

class DevelopmentVersionSpec extends FunSpec with Matchers {

  import java.io.File

  import org.apache.commons.io.FileUtils
  import com.cibo.io.s3.SyncablePath

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  implicit val buildInfo: BuildInfo = DummyBuildInfo


  describe("Functions with a version with the 'dev' flag.") {

    object myFunc extends Function1WithProvenance[Int, String] {

      val currentVersion: Version = Version("1.0", dev = true) // <---

      def impl(n: Int): String = "hello"
    }

    it("should use produce with the DevVersion suffix") {
      val testSubdir = "dev-version"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))

      val result1 = myFunc(123).resolve
      result1.call.getVersionValue(rt).dev shouldBe true
      result1.call.getVersionValue(rt).id.endsWith(DevVersion.suffix)

      val result2 = myFunc(456).resolve
      result2.call.getVersionValue.dev shouldBe true
      result2.call.getVersionValue.id.endsWith(DevVersion.suffix)
    }
  }
}
