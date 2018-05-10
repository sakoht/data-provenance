package com.cibo.provenance

import org.scalatest.{FunSpec, Matchers}

/**
  * Created by ssmith on 9/20/17.
  */

class ResultTrackerNoneSpec extends FunSpec with Matchers {

  describe("The null ResultTracker") {

    implicit val buildInfo: BuildInfo = BuildInfoDummy
    implicit val noTracking: ResultTrackerNone = ResultTrackerNone()

    it("causes functions re-run every time to produce results.") {
      AddTwoInts.runCount = 0

      val cnt0 = AddTwoInts.runCount
      cnt0 shouldBe 0

      val c1 = AddTwoInts(1, 2)
      val r1 = c1.run
      val cnt1 = AddTwoInts.runCount
      cnt1 shouldBe 1

      val c2 = AddTwoInts(3, 4)
      val r2  = c2.run
      val cnt2 = AddTwoInts.runCount
      cnt2 shouldBe 2

      val c3 = AddTwoInts(c1, c2)
      val r3 = c3.run
      val cnt3 = AddTwoInts.runCount
      cnt3 shouldBe 5 // re-runs s1 and s2, then runs s3

      val c4 = AddTwoInts(r1, c2)
      val r4 = c4.run
      val cnt4 = AddTwoInts.runCount // as above, but also re-runs s2 again, and s4
      cnt4 shouldBe 7
    }
  }
}


object AddTwoInts extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")

  // NOTE: This public var is reset during tests, and is a cheat to peek-inside whether or no impl(),
  // which is encapsulated, actually runs.
  var runCount: Int = 0

  def impl(a: Int, b: Int): Int = {
    runCount += 1
    a + b
  }
}

