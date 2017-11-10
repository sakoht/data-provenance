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

  val outputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = DummyBuildInfo

  describe("Calls that return a SUBCLASS of Seq[A]") {
    it("work ...*partly*") {
      implicit val rt: ResultTracker = ResultTrackerNone()

      val s: FunctionCallWithProvenance[Seq[Int]] = UnknownProvenance(Seq(11, 22, 33))
      val l: FunctionCallWithProvenance[List[Int]] = UnknownProvenance(List(11, 22, 33))
      val v: FunctionCallWithProvenance[Vector[Int]] = UnknownProvenance(Vector(11, 22, 33))
      val a: FunctionCallWithProvenance[Array[Int]] = UnknownProvenance(Array(11, 22, 33))

      s.resolve.output shouldBe Seq(11, 22, 33)
      l.resolve.output shouldBe Vector(11, 22, 33)
      v.resolve.output shouldBe Vector(11, 22, 33)
      a.resolve.output shouldBe Array(11, 22, 33)

      // This normal scala automatically works on Seq, and also List, Vector and Array.
      def foo(s: Seq[Int]) = {
        s.length
      }
      foo(s.resolve.output)
      foo(l.resolve.output)
      foo(v.resolve.output)
      foo(a.resolve.output)

      // For s, a FunctionCallWithProvenance[Seq[Int]], the implicit methods .map, .apply, etc.
      // are available, because FunctionCallWithProvenance[O] has O <: Seq[_].
      s.apply(2)              // .apply is defined in the implicit class MappableCall.
      s.apply2(2)
      s.indices
      s.indices2
      s.map(MyIncrement)      // .map is also.
      s(2)                    // This is just apply w/ sugar.


      // But none of these hit the MappableCall implicit class.  They all fail to compile.
      l.apply2(2)
      //l.apply(2)
      //l.map(MyIncrement)
      //l(2)

      v.apply2(2)
      //v.apply(2)
      //v.map(MyIncrement)
      //v(2)

      //a.apply(2)
      //a.map(MyIncrement)
      //a(2)
    }
  }

  describe("Calls that return sequence") {

    it("never run when selecting an element, and only run once after resolving some element") {
      val subDir = "mappable-calls-are-dry"
      val testDataDir = f"$outputBaseDir/$subDir"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))
      MakeDummyOutputList.runCount = 0

      val myCall = MakeDummyOutputList()
      MakeDummyOutputList.runCount shouldBe 0

      val i0: ApplyWithProvenance[Int]#Call = myCall(0)
      val i1: ApplyWithProvenance[Int]#Call = myCall(1)
      val i2: ApplyWithProvenance[Int]#Call = myCall(2)
      val i3: ApplyWithProvenance[Int]#Call = myCall(3)

      // Still hasn't run...
      MakeDummyOutputList.runCount shouldBe 0

      i0.resolve.output shouldBe 11
      i1.resolve.output shouldBe 22
      i2.resolve.output shouldBe 33
      i3.resolve.output shouldBe 44

      // Only ever ran once.
      MakeDummyOutputList.runCount shouldBe 1

      TestUtils.diffOutputSubdir(subDir)
    }

    it("can map") {
      val subDir = "monadic-calls"
      val testDataDir = f"$outputBaseDir/$subDir"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))

      val a = MakeDummyOutputList() // (11, 22, 33, 44)
      val b = a.map(MyIncrement)    // (12, 23, 34, 45)
      val c = b.map(MyIncrement)    // (13, 24, 35, 46)
      val d = c.map(MyIncrement)    // (14, 25, 36, 47)
      val e = SumValues(d)          // 122

      c(3).resolve.output shouldBe 46
      MyIncrement.runCount shouldBe 8   // we ran two of the three map calls

      d(0).resolve.output shouldBe 14
      MyIncrement.runCount shouldBe 12  // all three map calls have run

      a.resolve.output.sum shouldBe 110
      b.resolve.output.sum shouldBe 114
      c.resolve.output.sum shouldBe 118
      d.resolve.output.sum shouldBe 122
      e.resolve.output shouldBe 122

      MyIncrement.runCount shouldBe 12  // no more work has happened, just lookups

      TestUtils.diffOutputSubdir(subDir)
    }
  }

  describe("Results that return a sequence") {

    it("can 'scatter', giving individual results with provenance") {
      val subDir = "mappable-results-scatter"
      val testDataDir = f"$outputBaseDir/$subDir"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))

      MakeDummyOutputList.runCount = 0

      val myResult = MakeDummyOutputList().resolve
      MakeDummyOutputList.runCount shouldBe 1

      val seqOfResults: Seq[ApplyWithProvenance[Int]#Result] = myResult.scatter
      MakeDummyOutputList.runCount shouldBe 1

      val r0: ApplyWithProvenance[Int]#Result = seqOfResults(0)
      val r1: ApplyWithProvenance[Int]#Result = seqOfResults(1)
      val r2: ApplyWithProvenance[Int]#Result = seqOfResults(2)
      val r3: ApplyWithProvenance[Int]#Result = seqOfResults(3)

      MakeDummyOutputList.runCount shouldBe 1

      r0.output shouldBe 11
      r1.output shouldBe 22
      r2.output shouldBe 33
      r3.output shouldBe 44

      MakeDummyOutputList.runCount shouldBe 1

      TestUtils.diffOutputSubdir(subDir)
    }

    it("can map") {
      val subDir = "mappable-results-map"
      val testDataDir = f"$outputBaseDir/$subDir"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt: ResultTracker = ResultTrackerSimple(SyncablePath(testDataDir))
      MakeDummyOutputList.runCount = 0

      val myResult1: MakeDummyOutputList.Result = MakeDummyOutputList().resolve
      MakeDummyOutputList.runCount shouldBe 1

      MyIncrement.runCount = 0

      val myResult2: MapWithProvenance[Int, Int]#Call = myResult1.map(MyIncrement)
      MakeDummyOutputList.runCount shouldBe 1
      MyIncrement.runCount shouldBe 0

      val r2 = myResult2(2)
      MakeDummyOutputList.runCount shouldBe 1
      MyIncrement.runCount shouldBe 0

      r2.resolve.output shouldBe 34
      MakeDummyOutputList.runCount shouldBe 1
      MyIncrement.runCount shouldBe 4             // ideally 1, if we can make the system be lazy on the full map

      myResult2(2).resolve.output shouldBe 34
      MakeDummyOutputList.runCount shouldBe 1     // no change
      MyIncrement.runCount shouldBe 4             // no change, again 1 if we can get lazy

      myResult2(0).resolve.output shouldBe 12
      myResult2(1).resolve.output shouldBe 23
      myResult2(2).resolve.output shouldBe 34    // already done
      myResult2(3).resolve.output shouldBe 45

      MakeDummyOutputList.runCount shouldBe 1     // no change
      MyIncrement.runCount shouldBe 4             // no repeat calls

      TestUtils.diffOutputSubdir(subDir)
    }
  }
}

object MakeDummyOutputList extends Function0WithProvenance[Seq[Int]] {
  val currentVersion = Version("0.0")
  var runCount: Int = 0 // warning: var
  def impl = {
    runCount += 1
    Seq(11, 22, 33, 44)
  }
}

object MyIncrement extends Function1WithProvenance[Int, Int] {
  val currentVersion = Version("0.0")
  var runCount: Int = 0 // warning: var
  def impl(x: Int) = {
    runCount += 1
    x + 1
  }
}

object CountList extends Function1WithProvenance[Int, Seq[Int]] {
  val currentVersion: Version = NoVersion
  def impl(in: Seq[Int]) = {
    println(in)
    in.size
  }
}

object SumValues extends Function1WithProvenance[Int, Seq[Int]] {
  val currentVersion: Version = NoVersion
  def impl(in: Seq[Int]) = in.sum
}



