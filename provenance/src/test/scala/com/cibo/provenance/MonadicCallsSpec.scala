package com.cibo.provenance

import org.scalatest.{FunSpec, Matchers}

/**
  * Created by ssmith on 11/07/17.
  */

class MonadicCallsSpec extends FunSpec with Matchers {
  import java.io.File
  import org.apache.commons.io.FileUtils

  import com.cibo.io.s3.SyncablePath
  import com.cibo.provenance.tracker._
  import com.cibo.provenance.monaidcs._

  val baseTestDir: String = f"/tmp/" + sys.env.getOrElse("USER", "anonymous") + "/rt"

  // This dummy build info is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = DummyBuildInfo

  describe("Calls that return sequence") {

    it("never run when selecting an element, and only run once after resolving some element") {
      val testDataDir = f"$baseTestDir/mappable-calls-are-dry"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))
      MakeDummyOutputList.runCount = 0

      val myCall = MakeDummyOutputList()
      MakeDummyOutputList.runCount shouldBe 0

      val i0: ApplyWithProvenance[Double]#Call = myCall(0)
      val i1: ApplyWithProvenance[Double]#Call = myCall(1)
      val i2: ApplyWithProvenance[Double]#Call = myCall(2)
      val i3: ApplyWithProvenance[Double]#Call = myCall(3)

      // Still hasn't run...
      MakeDummyOutputList.runCount shouldBe 0

      i0.resolve.output shouldBe 1.1
      i1.resolve.output shouldBe 2.2
      i2.resolve.output shouldBe 3.3
      i3.resolve.output shouldBe 4.4

      // Only ever ran once.
      MakeDummyOutputList.runCount shouldBe 1
    }

    it("can map") {
      val testDataDir = f"$baseTestDir/monadic-calls"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))

      val a: MakeDummyOutputList.Call = MakeDummyOutputList()
      val b: MapWithProvenance[Double, Double]#Call = a.map(MyIncrement)
      val c = b.map(MyIncrement)
    }
  }

  describe("Results that return a sequence") {

    it("can 'scatter', giving individual results with provenance") {
      val testDataDir = f"$baseTestDir/mappable-results-scatter"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))

      MakeDummyOutputList.runCount = 0

      val myResult = MakeDummyOutputList().resolve
      MakeDummyOutputList.runCount shouldBe 1

      val seqOfResults: Seq[ApplyWithProvenance[Double]#Result] = myResult.scatter
      MakeDummyOutputList.runCount shouldBe 1

      val r0: ApplyWithProvenance[Double]#Result = seqOfResults(0)
      val r1: ApplyWithProvenance[Double]#Result = seqOfResults(1)
      val r2: ApplyWithProvenance[Double]#Result = seqOfResults(2)
      val r3: ApplyWithProvenance[Double]#Result = seqOfResults(3)

      MakeDummyOutputList.runCount shouldBe 1

      r0.output shouldBe 1.1
      r1.output shouldBe 2.2
      r2.output shouldBe 3.3
      r3.output shouldBe 4.4

      MakeDummyOutputList.runCount shouldBe 1
    }

    it("can map") {
      val testDataDir = f"$baseTestDir/mappable-results-map"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))
      MakeDummyOutputList.runCount = 0

      val myResult1: MakeDummyOutputList.Result = MakeDummyOutputList().resolve
      MakeDummyOutputList.runCount shouldBe 1

      MyIncrement.runCount = 0

      val myResult2: MapWithProvenance[Double, Double]#Call = myResult1.map(MyIncrement)
      MakeDummyOutputList.runCount shouldBe 1
      MyIncrement.runCount shouldBe 0

      val r2 = myResult2(2)
      MakeDummyOutputList.runCount shouldBe 1
      MyIncrement.runCount shouldBe 0

      r2.resolve.output shouldBe 4.3
      MakeDummyOutputList.runCount shouldBe 1
      MyIncrement.runCount shouldBe 4             // ideally 1, if we can make the system be lazy on the full map

      myResult2(2).resolve.output shouldBe 4.3
      MakeDummyOutputList.runCount shouldBe 1     // no change
      MyIncrement.runCount shouldBe 4             // no change, again 1 if we can get lazy

      myResult2(0).resolve.output shouldBe 2.1
      myResult2(1).resolve.output shouldBe 3.2
      myResult2(2).resolve.output shouldBe 4.3    // already done
      myResult2(3).resolve.output shouldBe 5.4

      MakeDummyOutputList.runCount shouldBe 1     // no change
      MyIncrement.runCount shouldBe 4             // no repeat calls
    }
  }
}

object MakeDummyOutputList extends Function0WithProvenance[Seq[Double]] {
  val currentVersion = Version("0.0")

  // Warning: This mutable counter lets the test cheat and see if thins are really re-running or not.
  var runCount: Int = 0

  def impl = {
    runCount += 1
    Seq(1.1, 2.2, 3.3, 4.4)
  }
}

object MyIncrement extends Function1WithProvenance[Double, Double] {
  val currentVersion = Version("0.0")

  // Warning: This mutable counter lets the test cheat and see if thins are really re-running or not.
  var runCount: Int = 0

  def impl(x: Double) = {
    runCount += 1
    x + 1.0
  }

}

object CountList extends Function1WithProvenance[Int, Seq[Double]] {
  val currentVersion = NoVersion
  def impl(in: Seq[Double]) = {
    println(in)
    in.size
  }
}



