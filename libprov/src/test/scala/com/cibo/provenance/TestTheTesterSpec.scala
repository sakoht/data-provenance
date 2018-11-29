package com.cibo.provenance

import java.io.File

import com.cibo.provenance.testsupport.{ResultTrackerForTest, ResultTrackerForTestFactory}
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}

class TestTheTesterSpec extends FunSpec with Matchers {

  implicit val bi: BuildInfo = BuildInfoDummy

  describe("A ResultTrackerForTest") {

    implicit val bi: BuildInfo = BuildInfoDummy

    implicit val rt: ResultTrackerForTest = MyFakeAppResultTrackerUT.apply("test-the-tester")

    it("When code produces the same answer as the reference answer, everything passes.") {
      rt.clean()
      MyTool1(2, 3).resolve.output shouldBe 5   // in the reference data
      MyTool1(6, 4).resolve.output shouldBe 10  // in the reference data
      rt.checkForUnstagedResults()
    }

    it("If code ever produces an inconsistent answer for the same params & version, throw an exception") {
      rt.clean()
      intercept[com.cibo.provenance.exceptions.InconsistentVersionException] {
        // Use a back-door in MyTool2IsBad to give make the tool give an inconsistent answer.
        // This is a bad tool, incorporating a mutable var into impl().
        // It doesn't always demonstrate its inconsistency flaw in the other scenarios.
        // The purpose of this test is to report on a bad/inconsistent tool.
        // This throws an exception because it sees that the reference data had 6+4=10, but it will get 910.
        MyTool2IsBad.backDoorInfluenceResults = 900
        MyTool2IsBad(6, 4).resolve
      }
    }

    it("When there are no errors, but we are testing new parameters with no reference answer, " +
      "a special exception is thrown, telling us that data need to be staged, and seeting that data up.") {
      rt.clean()
      MyTool1(2, 3).resolve.output shouldBe 5  // in the reference data
      MyTool1(6, 4).resolve.output shouldBe 10 // in the reference data
      MyTool1(7, 7).resolve.output shouldBe 14 // NOT in the reference data

      intercept[ResultTrackerForTest.UnstagedReferenceDataException] {
        rt.checkForUnstagedResults()
      }
    }

    it("Simulate staging new data reference data, and see the error go away.") {
      rt.clean()
      MyTool1(2, 3).resolve.output shouldBe 5  // in the reference data
      MyTool1(6, 4).resolve.output shouldBe 10 // in the reference data
      MyTool1(7, 7).resolve.output shouldBe 14 // NOT in the reference data

      // It still fails the check.
      intercept[ResultTrackerForTest.UnstagedReferenceDataException] {
        rt.checkForUnstagedResults()
      }

      // Make a copy of the tracker, and give it a copy of the reference data on local disk we can mutate.
      val rtb = rt.copy(
        referencePath =
          rt.outputPath.replaceAll("result-trackers-for-test-cases", "result-trackers-for-test-cases-dummyref")
      )
      FileUtils.deleteDirectory(new File(rtb.referencePath))
      FileUtils.copyDirectory(new File(rt.referencePath), new File(rtb.referencePath))

      // It still fails the check.
      intercept[ResultTrackerForTest.UnstagedReferenceDataException] {
        rtb.checkForUnstagedResults()
      }

      // A "push" to reference storage stages the data.
      rtb.push()

      // Then the check passes.
      rtb.checkForUnstagedResults()
    }
  }
}

/**
  * A factory for making result trackers.
  * We only need one, but want to test the
  */
object MyFakeAppResultTrackerUT extends ResultTrackerForTestFactory(
  outputRoot = s"/tmp/${sys.env.getOrElse("USER","anonymous")}/result-trackers-for-test-cases/libprov",
  referenceRoot =
    new File(TestUtils.testResourcesDir + "/provenance-data-by-test").getAbsolutePath
)(BuildInfoDummy)

object MyTool1 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int): Int = a + b
}

object MyTool2IsBad extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  var backDoorInfluenceResults: Int = 0
  def impl(a: Int, b: Int): Int = a + b + backDoorInfluenceResults
}

