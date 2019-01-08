package com.cibo.provenance

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FunSpec, Matchers}

class FindSpec extends FunSpec with Matchers with LazyLogging {

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("The API to list ResultTracker contents") {

    val testSubdir = f"find-api"
    val testDataDir = f"$testOutputBaseDir/$testSubdir"
    implicit val rt = ResultTrackerForSelfTest(testDataDir)
    rt.wipe()

    val stick1 = BreadStick(5, 0)
    val stick2Call = bakeIt(stick1, 10)
    val stick3Call = bakeIt(stick2Call, 6)

    // Resolve the call first so that it saves.
    val stick3Result = stick3Call.resolve.normalize

    // Pull the result out individually for comparisons below.
    val stick2Result = rt.loadResultByCallOption(stick2Call).get.normalize

    it("returns correct versions") {
      bakeIt.findVersions.toSet shouldBe Set(Version("0.1"))
    }

    it("works to load fully vivified calls") {
      bakeIt.findCalls.toSet shouldEqual Set(stick2Call, stick3Call)
    }

    it("works to load fully vivified results") {
      bakeIt.findResults.map(_.normalize).toSet shouldEqual Set(stick2Result, stick3Result)
    }

    it("works to load raw call data") {
      val raw = bakeIt.findCallData.toSet
      val vivified = raw.map(_.loadAs[bakeIt.Call])
      val normalized = vivified.map(_.normalize)
      normalized shouldEqual Set(stick2Call, stick3Call)
    }

    it("works to load raw results") {
      val raw = bakeIt.findResultData.toSet
      val vivified = raw.map(_.loadAs[bakeIt.Result])
      val normalized = vivified.map(_.normalize)
      normalized shouldEqual Set(stick2Result, stick3Result)
    }

    it("works when querying for results by output") {
      rt.a.findResultsByOutput(stick1).toSet shouldEqual Set.empty

      val result2 = stick2Call.resolve
      rt.findResultsByOutput(result2.output).toSet shouldEqual Set(result2)

      val result3 = stick3Call.resolve
      rt.findResultsByOutput(result3.output).toSet shouldEqual Set(result3)
    }

    it("produces expected data in storage") {
      TestUtils.diffOutputSubdir(testSubdir)
    }
  }

  describe("The API to find where a result is used as an input") {
    val testSubdir = "find-uses"
    val testDataDir = f"$testOutputBaseDir/$testSubdir"
    implicit val rt = ResultTrackerForSelfTest(testDataDir)
    rt.wipe()

    val stick1 = UnknownProvenance(BreadStick(10, 0))
    val stick1R = stick1.resolve
    logger.info(f"stick1 result ID ${stick1R.provenanceId} for value ${stick1R.output} with digest ${stick1R.outputId}")

    val eatable1 = isEatable(stick1)
    val eatable1R = eatable1.resolve

    val stick2 = bakeIt(stick1, 3)
    val stick2R = stick2.resolve
    logger.info(f"stick2 result ID ${stick2R.provenanceId} for value ${stick2R.output} with digest ${stick2R.outputId}")

    val eatable2 = isEatable(stick2)
    val eatable2R = eatable2.resolve

    val stick3 = bakeIt(stick2, 2)
    val stick3R = stick3.resolve
    logger.info(f"stick3 result ID ${stick3R.provenanceId} for value ${stick3R.output} with digest ${stick3R.outputId}")

    val stick1Uses = rt.findUsesOfResult(stick1R)
    val stick2Uses = rt.findUsesOfResult(stick2R)
    val stick3Uses = rt.findUsesOfResult(stick3R)

    it("correctly finds no uses for things that are not used as inputs") {
      stick3Uses shouldEqual Iterable.empty
    }

    it("works to look up uses of result from a regular call") {
      stick2Uses.map(_.load.normalize).toSet shouldEqual Set(eatable2R.normalize, stick3R.normalize)
    }

    it("works to look up uses of a result with unknown provenance") {
      stick1Uses.map(_.load.normalize).toSet shouldEqual Set(eatable1R.normalize, stick2R.normalize)
    }

    it("works on raw values") {
      rt.findUsesOfValue(stick1R.output).toSet shouldEqual stick1Uses.toSet
      rt.findUsesOfValue(stick2R.output).toSet shouldEqual stick2Uses.toSet
      rt.findUsesOfValue(stick3R.output).toSet shouldEqual Set.empty
    }

    it("produces expected data in storage") {
      TestUtils.diffOutputSubdir(testSubdir)
    }
  }
}

case class BreadStick(length: Int, crispiness: Int)

object BreadStick {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit val encoder: Encoder[BreadStick] = deriveEncoder[BreadStick]
  implicit val decoder: Decoder[BreadStick] = deriveDecoder[BreadStick]
}

object bakeIt extends Function2WithProvenance[BreadStick, Int, BreadStick] {
  val currentVersion = Version("0.1")
  def impl(stick: BreadStick, heat: Int): BreadStick = {
    stick.copy(crispiness = stick.crispiness + (heat * 2))
  }
}

object isEatable extends Function1WithProvenance[BreadStick, Boolean] {
  val currentVersion = Version("0.1")
  def impl(stick: BreadStick): Boolean = stick.crispiness >= 2 && stick.crispiness <= 10
}
