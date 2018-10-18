package com.cibo.provenance

import java.io.File

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.testsupport.{ResultTrackerForTest, ResultTrackerForTestFactory}
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}

class TestTheTesterSpec extends FunSpec with Matchers {

  implicit val bi: BuildInfo = BuildInfoDummy

  describe("A ResultTrackerForTest") {

    implicit val bi: BuildInfo = BuildInfoDummy
    implicit val rt: ResultTrackerForTest = MyFakeAppResultTrackerUT.apply("test-the-tester")

    it("when code produces the same answer as the reference answer, everything passes") {
      rt.clean()
      MyTool1(2, 3).resolve.output shouldBe 5  // in the reference data
      MyTool1(6, 4).resolve.output shouldBe 10 // in the reference data
      rt.check()
    }

    it("when the code produces an inconsistent answer, throw an error") {
      rt.clean()
      // consistent with the reference data
      MyTool1(2, 3).resolve.output shouldBe 5

      intercept[com.cibo.provenance.exceptions.InconsistentVersionException] {
        // Use a back-door on the function to make it produce an inconsistent answer.
        MyTool1.backDoorInfluenceResults = 1000
        MyTool1(6, 4).resolve
      }
    }

    it("when there are no errors, but there is new data, a special exception is thrown") {
      rt.clean()
      MyTool1(2, 3).resolve.output shouldBe 5  // in the reference data
      MyTool1(6, 4).resolve.output shouldBe 10 // in the reference data
      MyTool1(7, 7).resolve.output shouldBe 14 // NOT in the reference

      intercept[ResultTrackerForTest.UnstagedReferenceDataException] {
        rt.check()
      }
    }

    it("staging the new data as reference data prevents the exception") {
      rt.clean()
      MyTool1(2, 3).resolve.output shouldBe 5  // in the reference data
      MyTool1(6, 4).resolve.output shouldBe 10 // in the reference data
      MyTool1(7, 7).resolve.output shouldBe 14 // NOT in the reference

      // It still fails the check.
      intercept[ResultTrackerForTest.UnstagedReferenceDataException] {
        rt.check()
      }

      // Make a copy of the tracker, and give it a copy of the reference data on local disk we can mutate.
      val rtb = rt.copy(
        referencePath =
          SyncablePath(
            rt.outputPath.path.replaceAll("result-trackers-for-test-cases", "result-trackers-for-test-cases-dummyref")
          )
      )
      FileUtils.deleteDirectory(rtb.referencePath.toFile)
      FileUtils.copyDirectory(rt.referencePath.toFile, rtb.referencePath.toFile)

      // It still fails the check.
      intercept[ResultTrackerForTest.UnstagedReferenceDataException] {
        rtb.check()
      }

      // A "push" to reference storage stages the data.
      rtb.push()

      // Then the check passes.
      rtb.check()
    }
  }
}

/**
  * A factory for making result trackers.
  * We only need one, but want to test the
  */
object MyFakeAppResultTrackerUT extends ResultTrackerForTestFactory(
  outputRoot = SyncablePath(s"/tmp/${sys.env.getOrElse("USER","anonymous")}/result-trackers-for-test-cases/libprov"),
  referenceRoot = SyncablePath(
    new File("libprov/src/test/resources/provenance-data-by-test").getAbsolutePath
  )
)(BuildInfoDummy)

object MyTool1 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  var backDoorInfluenceResults: Int = 0
  def impl(a: Int, b: Int): Int = a + b + backDoorInfluenceResults
}


