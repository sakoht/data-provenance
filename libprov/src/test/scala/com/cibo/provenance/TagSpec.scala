package com.cibo.provenance

import org.scalatest._
import io.circe.generic.auto._
import com.cibo.provenance.Util._

class TagSpec extends FunSpec with Matchers {

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("tags") {
    it("work when made w/o a result tracker") {
      val f1: Fish = Fish(10, 0)
      val t1: tag[Fish]#Call = tag(f1, "fish tag 1")

      val testSubdir = "tag1"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe()

      t1.save
    }

    it("works when made w/ a result tracker") {
      val testSubdir = "tag2"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe()

      val f1: Fish            = Fish(10, 0)
      val t1: tag[Fish]#Call  = tag(f1, "fish tag 1")

      val f2: fryEm.Call      = fryEm(t1, 6)
      val t2: tag[Fish]#Call  = tag(f2, "fish tag 2")

      val f3 = f2.resolve

      val tags = rt.findResultData("com.cibo.provenance.Util$.tag[com.cibo.provenance.Fish]")
      tags.size shouldBe 2
    }
  }
}

case class Fish(length: Int, crispiness: Int)

object fryEm extends Function2WithProvenance[Fish, Int, Fish] {
  val currentVersion = Version("0.1")
  def impl(unfriedFish: Fish, heat: Int): Fish = {
    unfriedFish.copy(crispiness = unfriedFish.crispiness + (heat * 2))
  }
}
