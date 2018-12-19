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
    val loaf2 = addSlices(loaf1, 10)
    val loaf3 = addSlices(loaf2, 6)

    // Resolve the call first so that it saves.
    val loaf3result = loaf3.resolve.normalize

    // Pull the result out individually for comparisons below.
    val loaf2result = rt.loadResultByCallOption(loaf2).get.normalize

    it("returns correct versions") {
      addSlices.findVersions.toSet shouldBe Set(Version("0.1"))
    }

    it("works to load fully vivified calls") {
      addSlices.findCalls.toSet shouldEqual Set(loaf2, loaf3)
    }

    it("works to load fully vivified results") {
      addSlices.findResults.map(_.normalize).toSet shouldEqual Set(loaf2result, loaf3result)
    }

    it("works to load raw call data") {
      val raw = addSlices.findCallData.toSet
      val vivified = raw.map(_.loadAs[addSlices.Call])
      val normalized = vivified.map(_.normalize)
      normalized shouldEqual Set(loaf2, loaf3)
    }

    it("works to load raw results") {
      val raw = addSlices.findResultData.toSet
      val vivified = raw.map(_.loadAs[addSlices.Result])
      val normalized = vivified.map(_.normalize)
      normalized shouldEqual Set(loaf2result, loaf3result)
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



