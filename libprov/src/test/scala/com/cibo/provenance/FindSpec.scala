package com.cibo.provenance

import org.scalatest.{FunSpec, Matchers}

class FindSpec extends FunSpec with Matchers {

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("finding data") {

    val testSubdir = f"find"
    val testDataDir = f"$testOutputBaseDir/$testSubdir"
    implicit val rt = ResultTrackerForSelfTest(testDataDir)
    rt.wipe()

    val loaf1 = BreadLoaf(5)
    val loaf2Call = addSlices(loaf1, 10)
    val loaf3Call = addSlices(loaf2Call, 6)

    // Resolve the call first so that it saves.
    val loaf3Result = loaf3Call.resolve.normalize

    // Pull the result out individually for comparisons below.
    val loaf2result = rt.loadResultByCallOption(loaf2Call).get.normalize

    it("returns correct versions") {
      addSlices.findVersions.toSet shouldBe Set(Version("0.1"))
    }

    it("works to load fully vivified calls") {
      addSlices.findCalls.toSet shouldEqual Set(loaf2Call, loaf3Call)
    }

    it("works to load fully vivified results") {
      addSlices.findResults.map(_.normalize).toSet shouldEqual Set(loaf2result, loaf3Result)
    }

    it("works to load raw call data") {
      val raw = addSlices.findCallData.toSet
      val vivified = raw.map(_.loadAs[addSlices.Call])
      val normalized = vivified.map(_.normalize)
      normalized shouldEqual Set(loaf2Call, loaf3Call)
    }

    it("works to load raw results") {
      val raw = addSlices.findResultData.toSet
      val vivified = raw.map(_.loadAs[addSlices.Result])
      val normalized = vivified.map(_.normalize)
      normalized shouldEqual Set(loaf2result, loaf3Result)
    }

    it("works when querying for results by output") {
      rt.a.findResultsByOutput(loaf1).toSet shouldEqual Set.empty

      val result2 = loaf2Call.resolve
      rt.findResultsByOutput(result2.output).toSet shouldEqual Set(result2)

      val result3 = loaf3Call.resolve
      rt.findResultsByOutput(result3.output).toSet shouldEqual Set(result3)
    }
  }
}

case class BreadLoaf(slices: Int)

object BreadLoaf {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit val encoder: Encoder[BreadLoaf] = deriveEncoder[BreadLoaf]
  implicit val decoder: Decoder[BreadLoaf] = deriveDecoder[BreadLoaf]
}

object addSlices extends Function2WithProvenance[BreadLoaf, Int, BreadLoaf] {
  val currentVersion: Version = Version("0.1")

  def impl(cheese: BreadLoaf, count: Int): BreadLoaf = {
    cheese.copy(slices = cheese.slices + count)
  }
}



