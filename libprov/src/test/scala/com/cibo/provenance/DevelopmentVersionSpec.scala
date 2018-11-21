package com.cibo.provenance

import org.scalatest.{FunSpec, Matchers}
import com.cibo.aws.AWSClient.Implicits.s3SyncClient
import com.cibo.io.s3.SyncablePathBaseDir.Implicits.default

class DevelopmentVersionSpec extends FunSpec with Matchers {

  import com.cibo.io.s3.SyncablePath

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  implicit val buildInfo: BuildInfo = BuildInfoDummy


  describe("Functions with a version with the 'dev' flag.") {

    object myFunc extends Function1WithProvenance[Int, String] {

      val currentVersion: Version = Version("1.0", dev = true) // <---

      def impl(n: Int): String = "hello"
    }

    it("should use produce with the DevVersion suffix") {
      val testSubdir = "dev-version"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt: ResultTracker = ResultTrackerForSelfTest(testDataDir)

      val result1 = myFunc(123).resolve
      result1.call.versionValue(rt).isDev shouldBe true
      result1.call.versionValue(rt).id.endsWith(Version.devSuffix)

      val result2 = myFunc(456).resolve
      result2.call.versionValue.isDev shouldBe true
      result2.call.versionValue.id.endsWith(Version.devSuffix)
    }
  }
}
