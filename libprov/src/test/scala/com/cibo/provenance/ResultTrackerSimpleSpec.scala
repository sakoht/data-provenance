package com.cibo.provenance

/**
  * Created by ssmith on 9/20/17.
  */


import com.cibo.provenance.exceptions.InconsistentVersionException
import com.typesafe.scalalogging.LazyLogging
import org.scalatest._

import com.cibo.io.s3.S3SyncablePath

import scala.concurrent.ExecutionContext

class ResultTrackerForSelfTestSpec extends FunSpec with Matchers with LazyLogging {
  
  // This is the root for test output.
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  // This dummy BuildInfo is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  // This is called at the end of each test to regression-test the low-level storage.
  // We test only the manifests since the SHA1 in the name is the digest of the file,
  // or the file is a key path, with empty content.

  describe("The simple ResultTracker") {

    it("has primitives save and reload correctly without access to real type information.") {
      val testSubdir = "reload0"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      val obj1: Int = 888
      val id = rt.saveOutputValue(obj1)

      rt.loadValueOption("scala.Int", id) match {
        case Some(i) =>
          i shouldEqual obj1
        case None =>
          val o2 = rt.loadValueOption("scala.Int", id)
          o2.get
      }

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("has primitives save and reload correctly when the type is known.") {
      val testSubdir = "reload1"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      val obj1: Int = 999
      val id = rt.saveOutputValue(obj1)
      val obj2 = rt.loadValue[Int](id)
      obj2 shouldEqual obj1

      TestUtils.diffOutputSubdir(testSubdir)
    }

    /*

    // When we save a call in normal situations, we break it up into N JSON objects,
    // each referencing each other by ID.
    // As such, we con't support saving a call as a regular data object.
    // If we updated the ValueWithProvenance{En,De}coder this could be made to work again.

    ignore("has calls save and reload correctly when used as regular data") {
      val testSubdir = "reload2"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      
      implicit val rt = ResultTrackerForTest(testDataDir)
      rt.wipe

      val obj1: Add.Call = Add(1, 2)
      val id = rt.saveOutputValue(obj1)

      val obj2pair: (Add.Call, Codec[Add.Call]) = rt.loadValueWithCodec[Add.Call](id)
      val obj2v: Add.Call = obj2pair._1
      obj2v shouldEqual obj1
      val obj2c: Codec[Add.Call] = obj2pair._2

      val obj2 = rt.loadValue[Add.Call](id)
      obj2 shouldEqual obj1

      TestUtils.diffOutputSubdir(testSubdir)
    }

    */

    it("lets a result save and be re-loaded by its call signature.") {
      val testSubdir = "reload3"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe
      
      // Create a result that is not tracked.
      val s1: Add.Call = Add(1, 2)
      val r2 = s1.run(ResultTrackerNone())

      // Save the result explicitly.
      val idIgnored = FunctionCallResultWithKnownProvenanceSerializable.save(r2)

      // Use the signature itself to re-load, ignoring the saved ID.
      val r2b = rt.loadResultByCallOption(s1).get
      r2b.call.unresolve shouldEqual r2.call.unresolve
      r2b.output shouldEqual r2.output

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("ensures functions do not re-run") {
      val testSubdir = "rerun1"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe
      
      Add.runCount = 0

      val rc0 = Add.runCount
      rc0 shouldBe 0

      val s1 = Add(1, 2)
      val r1 = s1.resolve
      val rc1 = Add.runCount
      rc1 shouldBe 1 * rt.countUnderlying

      val r1b = s1.resolve
      val rc1b = Add.runCount
      rc1b shouldBe 1 * rt.countUnderlying

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("ensures functions do not re-run when called with the same inputs") {
      val testSubdir = "rerun2"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      Add.runCount = 0

      // Ensure the build has _not_ been saved yet.
      rt.loadBuildInfoOption(BuildInfoDummy.commitId, BuildInfoDummy.buildId) shouldEqual None

      Add.runCount = 0
      val s1 = Add(Add(1,2), Add(3,4))
      val r1 = s1.resolve
      val rc1 = Add.runCount
      rc1 shouldBe 3 * rt.countUnderlying

      // Verify we saved the build, and got the BuildInfo in a fully-fleshed-out subclass.
      val reloadedDummy: BuildInfoGit =
        rt.loadBuildInfoOption(BuildInfoDummy.commitId, BuildInfoDummy.buildId).get.asInstanceOf[BuildInfoGit]

      // These are logically equal, though the original was a raw object from SbtBuildInfo,
      // and the reload is a case class with the same fields.
      reloadedDummy.toString shouldEqual BuildInfoDummy.toString

      Add.runCount = 0
      val s2 = Add(Add(1,2), Add(3,4))
      val r2 = s2.resolve
      val rc2 = Add.runCount
      rc2 shouldBe 0 // unchanged

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("should skip calls where the call has been made before with the same input values") {
      val testSubdir = "rerun3"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      Add.runCount = 0

      val c1 = Add(Add(Add(1,2), Add(3,4)), 6)
      val r1 = c1.resolve
      r1.output shouldBe 16

      val rc1 = Add.runCount
      rc1 shouldBe 4 * rt.countUnderlying               // all 4 calls occur

      Add.runCount = 0

      val c2 = Add(Add(Add(1,2), Add(2,5)), 6)          // replace Add(3,4) w/ Add(2,5) ...
      val r2 = c2.resolve
      r2.output shouldBe 16

      val rc2 = Add.runCount

      r2.output shouldBe r1.output                      // same output value
      r2.call.unresolve shouldBe c2                     // correct provenance
      rc2 shouldBe 1  * rt.countUnderlying              // only ONE of the four calls has to occur

      Add.runCount = 0

      val s3 = Add(Add(Add(1,2), Add(1,6)), 7)          // 1+6 == 3+4
      val r3 = s3.resolve
      r3.output shouldBe 17

      val rc3 = Add.runCount

      r3.output shouldBe r1.output + 1                  // same value
      r3.call.unresolve shouldBe s3
      rc3 shouldBe 2  * rt.countUnderlying              // only TWO of the four operations actually run: Add(3+5) and the final +7

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("ensures functions method calls return expected values (breakdown)") {
      val testSubdir = "breakdown"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      Add.runCount = 0
      val s1 = Add(1, 2)
      val r1 = s1.resolve
      val rc1 = Add.runCount
      rc1 shouldBe 1 * rt.countUnderlying

      Add.runCount = 0
      val s2 = Add(3, 4)
      val r2  = s2.resolve
      val rc2 = Add.runCount
      rc2 shouldBe 1 * rt.countUnderlying

      Add.runCount = 0
      val s4 = Add(r1, s2)
      val r4 = s4.resolve                               // does not re-run s2
      val rc4 = Add.runCount
      rc4 shouldBe 1 * rt.countUnderlying

      Add.runCount = 0
      val s3 = Add(s1, s2)
      val r3 = s3.resolve // does not run anything: same input values with same provenance
      val rc3 = Add.runCount
      rc3 shouldBe 0

      Add.runCount = 0
      val s2b = Add(2, 5)
      val r2b = s2b.resolve
      val rc5 = Add.runCount
      rc5 shouldBe 1 * rt.countUnderlying

      Add.runCount = 0
      val s3b = Add(s1, s2b)
      val r3b = s3b.resolve // does not run anything: different provenance but same final input values
      val rc6 = Add.runCount
      rc6 shouldBe 0
      r3b.output shouldEqual r3.output
      r3b.call.unresolve shouldEqual s3b

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }

  describe("Across different commits and builds") {
    val build1 = BuildInfoBrief("commit1", "build1")
    val build2 = BuildInfoBrief("commit1", "build2")
    val build3 = BuildInfoBrief("commit2", "build3")

    it("results should be found from a previous run") {
      val testSubdir = "collision"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      val rt = ResultTrackerForSelfTest(testDataDir)  // not used below, just wipe
      rt.wipe

      {
        implicit val rt1 = ResultTrackerForSelfTest(testDataDir)(build1)
        val r1 = Add(1, 1).resolve
        r1.outputBuildInfoBrief shouldEqual build1
      }

      {
        implicit val rt2 = ResultTrackerForSelfTest(testDataDir)(build2)
        val r2 = Add(1, 1).resolve
        r2.outputBuildInfoBrief shouldEqual build1 // still build1
      }

      {
        implicit val rt3 = ResultTrackerForSelfTest(testDataDir)(build3)
        val r3 = Add(1, 1).resolve
        r3.outputBuildInfoBrief == build1 // still build1
      }

      

      {
        implicit val rt2 = ResultTrackerForSelfTest(testDataDir)(build2)
        val r2 = Add(1, 1).resolve
        r2.outputBuildInfoBrief == build2 // now build 2!
      }

      

      {
        implicit val rt3 = ResultTrackerForSelfTest(testDataDir)(build2)
        val r3 = Add(1, 1).resolve
        r3.outputBuildInfoBrief == build3 // now build 3!
      }

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("should detect inconsistent output for the same commit/build") {
      val testSubdir = "same-build-inconsistency"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      val rt = ResultTrackerForSelfTest(testDataDir) // not used below, just to wipe
      rt.wipe

      val call = Add(1, 1)

      {
        implicit val rt1 = ResultTrackerForSelfTest(testDataDir)(build1)
        val r1 = call.resolve
        r1.output shouldEqual 2
        r1.outputBuildInfoBrief shouldEqual build1
      }

      {
        implicit val rt2 = ResultTrackerForSelfTest(testDataDir)(build2)

        val r2 = call.resolve                             // The resolver finds a previous result
        r2.outputBuildInfoBrief shouldEqual build1     // from the last build
        r2.output shouldEqual 2                           // and has the correct output.

        val r3 = call.resolveInputs.newResult(3)(build1)  // Make a fake result.
        r3.outputBuildInfoBrief shouldEqual build1        // On the same build.
        r3.output shouldEqual 3                           // That has an inconsistent value for 1+1

        intercept[InconsistentVersionException] {
          // Detect the collision on load.
          // We will eventually flag the bad commit and detect further attempts to use it.
          FunctionCallResultWithKnownProvenanceSerializable.save(r3)       // And save it.
          call.resolve
        }
      }

      TestUtils.diffOutputSubdir(testSubdir)
    }

    it("should detect inconsistent output for the same declared version across commit/builds") {
      val testSubdir = f"cross-build-inconsistency"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      val call = Add(1, 1)

      {
        implicit val rt1: ResultTracker = ResultTrackerForSelfTest(testDataDir)(build1)
        val r1 = call.resolve
        r1.output shouldEqual 2
        r1.outputBuildInfoBrief shouldEqual build1
      }

      {
        implicit val rt2: ResultTracker = ResultTrackerForSelfTest(testDataDir)(build2)

        val r4 = call.resolveInputs.newResult(4)(build2)    // Make a fake result.
        r4.outputBuildInfoBrief shouldEqual build2          // On a new commit and build.
        r4.output shouldEqual 4                             // That has an inconsistent value for 1+1

        intercept[InconsistentVersionException] {
          // For now we complain.
          // Eventually we flag the newer commit as inconsistent, and resolve will load the original value.
          // If the original value was wrong, the version can/should be bumped.
          FunctionCallResultWithKnownProvenanceSerializable.save(r4)         // And save it.
          call.resolve
        }
      }

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }

  describe("The S3Store path parsing") {
    implicit val ec: ExecutionContext = ExecutionContext.global

    it("works with a good path") {
      KVStore("s3://mybucket/mypath")
    }

    it("fails with a double slash if after the s3://") {
      intercept[RuntimeException] {
        KVStore("s3:///mybucket/mypath")
        //                                          ^
      }
    }

    it("fails with a double slash in the middle") {
      intercept[RuntimeException] {
        KVStore("s3://mybucket//mypath")
        //                                                   ^
      }
    }

    it("fails with a trailing slash") {
      intercept[RuntimeException] {
        KVStore("s3://mybucket/mypath/")
        //                                                         ^
      }
    }
  }
}

object Add extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")

  // NOTE: This public var is reset during tests, and is a cheat to peek-inside whether or no impl(),
  // which is encapsulated, actually runs.
  @transient
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
