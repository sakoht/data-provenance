package com.cibo.provenance

/**
  * Created by ssmith on 9/20/17.
  */

import org.scalatest.{FunSpec, Matchers}


class ResultTrackerSimpleSpec extends FunSpec with Matchers {
  import java.io.File
  import org.apache.commons.io.FileUtils

  import com.cibo.io.s3.SyncablePath
  import com.cibo.provenance.tracker.{ResultTracker, ResultTrackerNone, ResultTrackerSimple}

  val baseTestDir: String = f"/tmp/" + sys.env.getOrElse("USER", "anonymous") + "/rt"

  // This dummy build info is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = DummyBuildInfo

  describe("The simple ResultTracker") {

    it("has primitives save and reload correctly.") {
      val testDataDir = f"$baseTestDir/reload1"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))

      val obj1: Int = 999
      val id = rt.saveValue(obj1)
      val obj2 = rt.loadValue[Int](id)
      obj2 shouldEqual obj1
    }

    it("has signatures save and reload correctly.") {
      val testDataDir = f"$baseTestDir/reload2"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))

      val obj1: Add.Call = Add(1, 2)
      val id = rt.saveValue(obj1)
      val obj2 = rt.loadValue[Add.Call](id)
      obj2 shouldEqual obj1
    }

    it("lets a result save and be re-loaded by its call signature.") {
      val testDataDir = f"$baseTestDir/reload3"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))
      
      // Create a result that is not tracked.
      val s1: Add.Call = Add(1, 2)
      val r2 = s1.run(ResultTrackerNone())

      // Save the result explicitly.
      val idIgnored = rt.saveResult(r2)

      // Use the signature itself to re-load, ignoring the saved ID.
      val r2b = rt.loadResultForCallOption(s1).get
      r2b.getProvenanceValue shouldEqual r2.getProvenanceValue
      r2b.getOutputValue shouldEqual r2.getOutputValue
    }

    it("ensures functions do not re-run") {

      val testDataDir = f"$baseTestDir/rerun1"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))
      
      Add.runCount = 0

      val rc0 = Add.runCount
      rc0 shouldBe 0

      val s1 = Add(1, 2)
      val r1 = s1.resolve
      val rc1 = Add.runCount
      rc1 shouldBe 1

      val r1b = s1.resolve
      val rc1b = Add.runCount
      rc1b shouldBe 1
    }

    it("ensures functions do not re-run when called with the same inputs") {
      val testDataDir = f"$baseTestDir/rerun2"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))

      Add.runCount = 0

      Add.runCount = 0
      val s1 = Add(Add(1,2), Add(3,4))
      val r1 = s1.resolve
      val rc1 = Add.runCount
      rc1 shouldBe 3

      Add.runCount = 0
      val s2 = Add(Add(1,2), Add(3,4))
      val r2 = s2.resolve
      val rc2 = Add.runCount
      rc2 shouldBe 0 // unchanged
    }

    it("should skip calls where the call has been made before with the same input values") {
      val testDataDir = f"$baseTestDir/rerun3"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))

      Add.runCount = 0

      val c1 = Add(Add(Add(1,2), Add(3,4)), 6)
      val r1 = c1.resolve
      r1.getOutputValue shouldBe 16

      val rc1 = Add.runCount
      rc1 shouldBe 4                                    // all 4 calls occur

      Add.runCount = 0

      val c2 = Add(Add(Add(1,2), Add(2,5)), 6)          // replace Add(3,4) w/ Add(2,5) ...
      val r2 = c2.resolve
      r2.getOutputValue shouldBe 16

      val rc2 = Add.runCount

      r2.getOutputValue shouldBe r1.getOutputValue      // same output value
      r2.getProvenanceValue.unresolve shouldBe c2       // correct provenance
      rc2 shouldBe 1                                    // only ONE of the four calls has to occur

      Add.runCount = 0

      val s3 = Add(Add(Add(1,2), Add(1,6)), 7)          // 1+6 == 3+4
      val r3 = s3.resolve
      r3.getOutputValue shouldBe 17

      val rc3 = Add.runCount

      r3.getOutputValue shouldBe r1.getOutputValue + 1 // same value
      r3.getProvenanceValue.unresolve shouldBe s3
      rc3 shouldBe 2                                   // only TWO of the four operations actually run: Add(3+5) and the final +7
    }

    it("ensures functions method calls return expected values (breakdown)") {
      val testDataDir = f"$baseTestDir/breakdown"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))

      Add.runCount = 0
      val s1 = Add(1, 2)
      val r1 = s1.resolve
      val rc1 = Add.runCount
      rc1 shouldBe 1

      Add.runCount = 0
      val s2 = Add(3, 4)
      val r2  = s2.resolve
      val rc2 = Add.runCount
      rc2 shouldBe 1

      Add.runCount = 0
      val s4 = Add(r1, s2)
      val r4 = s4.resolve // does not re-run s2
      val rc4 = Add.runCount
      rc4 shouldBe 1

      Add.runCount = 0
      val s3 = Add(s1, s2)
      val r3 = s3.resolve // does not run anything: same input values with same provenance
      val rc3 = Add.runCount
      rc3 shouldBe 0

      Add.runCount = 0
      val s2b = Add(2, 5)
      val r2b = s2b.resolve
      val rc5 = Add.runCount
      rc5 shouldBe 1

      Add.runCount = 0
      val s3b = Add(s1, s2b)
      val r3b = s3b.resolve // does not run anything: different provenance but same final input values
      val rc6 = Add.runCount
      rc6 shouldBe 0
      r3b.getOutputValue shouldEqual r3.getOutputValue
      r3b.getProvenanceValue.unresolve shouldEqual s3b
    }
  }

  describe("Across different commits and builds") {
    val build1 = BuildInfoBrief("commit1", "build1")
    val build2 = BuildInfoBrief("commit1", "build2")
    val build3 = BuildInfoBrief("commit2", "build3")

    it("results should be found from a previous run") {
      val testDataDir = f"$baseTestDir/collision"
      FileUtils.deleteDirectory(new File(testDataDir))

      {
        implicit val rt1 = ResultTrackerSimple(SyncablePath(testDataDir))(build1)
        val r1 = Add(1, 1).resolve
        r1.getOutputBuildInfoBrief shouldEqual build1
      }

      {
        implicit val rt2 = ResultTrackerSimple(SyncablePath(testDataDir))(build2)
        val r2 = Add(1, 1).resolve
        r2.getOutputBuildInfoBrief shouldEqual build1 // still build1
      }

      {
        implicit val rt3 = ResultTrackerSimple(SyncablePath(testDataDir))(build3)
        val r3 = Add(1, 1).resolve
        r3.getOutputBuildInfoBrief == build1 // still build1
      }

      FileUtils.deleteDirectory(new File(testDataDir))

      {
        implicit val rt2 = ResultTrackerSimple(SyncablePath(testDataDir))(build2)
        val r2 = Add(1, 1).resolve
        r2.getOutputBuildInfoBrief == build2 // now build 2!
      }

      FileUtils.deleteDirectory(new File(testDataDir))

      {
        implicit val rt3 = ResultTrackerSimple(SyncablePath(testDataDir))(build2)
        val r3 = Add(1, 1).resolve
        r3.getOutputBuildInfoBrief == build3 // now build 3!
      }
    }

    it("should detect inconsistent output for the same commit/build") {
      val testDataDir = f"$baseTestDir/same-build-inconsistency"
      FileUtils.deleteDirectory(new File(testDataDir))

      val call = Add(1, 1)

      {
        implicit val rt1 = ResultTrackerSimple(SyncablePath(testDataDir))(build1)
        val r1 = call.resolve
        r1.getOutputValue shouldEqual 2
        r1.getOutputBuildInfoBrief shouldEqual build1
      }

      {
        implicit val rt2 = ResultTrackerSimple(SyncablePath(testDataDir))(build2)

        val r2 = call.resolve                             // The resolver finds a previous result
        r2.getOutputBuildInfoBrief shouldEqual build1     // from the last build
        r2.getOutputValue shouldEqual 2                   // and has the correct output.

        val r3 = call.newResult(3)(build1)                // Make a fake result.
        r3.getOutputBuildInfoBrief shouldEqual build1     // On the same build.
        r3.getOutputValue shouldEqual 3                   // That has an inconsistent value for 1+1
        rt2.saveResult(r3)                                // And save it.

        intercept[com.cibo.provenance.InconsistentVersionException] {
          // Detect the collision on load.
          // We will eventually flag the bad commit and detect further attempts to use it.
          call.resolve
        }
      }
    }

    it("should detect inconsistent output for the same declared version across commit/builds") {
      val testDataDir = f"$baseTestDir/cross-build-inconsistency"
      FileUtils.deleteDirectory(new File(testDataDir))

      val call = Add(1, 1)

      {
        implicit val rt1: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))(build1)
        val r1 = call.resolve
        r1.getOutputValue shouldEqual 2
        r1.getOutputBuildInfoBrief shouldEqual build1
      }

      {
        implicit val rt2: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))(build2)

        val r4 = call.newResult(4)(build2)            // Make a fake result.
        r4.getOutputBuildInfoBrief shouldEqual build2 // On a new commit and build.
        r4.getOutputValue shouldEqual 4               // That has an inconsistent value for 1+1
        rt2.saveResult(r4)                            // And save it.

        intercept[com.cibo.provenance.InconsistentVersionException] {
          // For now we complain.
          // Eventually we flag the newer commit as inconsistent, and resolve will load the original value.
          // If the original value was wrong, the version can/should be bumped.
          call.resolve
        }
      }
    }
  }
}

object Add extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")

  // NOTE: This public var is reset during tests, and is a cheat to peek-inside whether or no impl(),
  // which is encapsulated, actually runs.
  var runCount: Int = 0

  def impl(a: Int, b: Int): Int = {
    runCount += 1
    a + b
  }
}

object Multiply extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")

  def impl(a: Int, b: Int): Int = a * b
}
