package com.cibo.provenance

import com.cibo.provenance.FunctionCallWithProvenance.TraversableCallExt
import org.scalatest.{FunSpec, Matchers}



/**
  * Created by ssmith on 11/07/17.
  */


class MonadicCallsSpec extends FunSpec with Matchers {
    import com.cibo.provenance.monadics._

  val outputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("Calls and results that return a Traversable") {

    it("handle granularity shifts") {
      val subDir = "monadic-calls"
      val testDataDir = f"$outputBaseDir/$subDir"
      
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      val a: MakeDummyOutputList.Call = MakeDummyOutputList() // (11, 22, 33, 44)
      val b = a.map(MyIncrement)    // (12, 23, 34, 45)
      val c = b.map(MyIncrement)    // (13, 24, 35, 46)
      val d = c.map(MyIncrement)    // (14, 25, 36, 47)
      val e = SumValues(d)          // 122

      c(3).resolve.output shouldBe 46
      MyIncrement.runCount shouldBe 8 * rt.countUnderlying // we ran two of the three map calls

      d(0).resolve.output shouldBe 14
      MyIncrement.runCount shouldBe 12 * rt.countUnderlying // all three map calls have run

      a.resolve.output.sum shouldBe 110
      b.resolve.output.sum shouldBe 114
      c.resolve.output.sum shouldBe 118
      d.resolve.output.sum shouldBe 122
      e.resolve.output shouldBe 122

      MyIncrement.runCount shouldBe 12 * rt.countUnderlying // no more work has happened, just lookups

      TestUtils.diffOutputSubdir(subDir)
    }

    it("adds methods `map`, `apply`, `indices` and `scatter` that are recognized by the compiler for `List`, `Vector` and `List`") {
      implicit val rt: ResultTracker = ResultTrackerNone()

      val s: FunctionCallWithProvenance[List[Int]] = UnknownProvenance(List(11, 22, 33))
      val l: FunctionCallWithProvenance[List[Int]] = UnknownProvenance(List(11, 22, 33))
      val v: FunctionCallWithProvenance[Vector[Int]] = UnknownProvenance(Vector(11, 22, 33))

      s.resolve.output shouldBe List(11, 22, 33)
      l.resolve.output shouldBe List(11, 22, 33)
      v.resolve.output shouldBe Vector(11, 22, 33)

      // Note: this only tests whether the methods are recognized by the compiler.
      // Other tests go into detail about results.

      s.apply(2)
      s.indices
      s.map(MyIncrement)
      s(2)
      s.scatter

      l.apply(2)
      l.indices
      l.map(MyIncrement)
      l(2)
      l.scatter

      v.apply(2)
      v.indices
      v.map(MyIncrement)
      v(2)
      v.scatter
    }

    it("should never run when selecting an element with apply(), and only run once after resolving some element") {
      val subDir = "mappable-calls-are-dry"
      val testDataDir = f"$outputBaseDir/$subDir"

      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      MakeDummyOutputList.runCount = 0

      val myCall: FunctionCallWithProvenance[List[Int]] = MakeDummyOutputList()
      MakeDummyOutputList.runCount shouldBe 0

      val i0: ApplyWithProvenance[List, Int]#Call = myCall(0)
      val i1: ApplyWithProvenance[List, Int]#Call = myCall(1)
      val i2: ApplyWithProvenance[List, Int]#Call = myCall(2)
      val i3: ApplyWithProvenance[List, Int]#Call = myCall(3)

      // Still hasn't run...
      MakeDummyOutputList.runCount shouldBe 0

      i0.resolve.output shouldBe 11
      i1.resolve.output shouldBe 22
      i2.resolve.output shouldBe 33
      i3.resolve.output shouldBe 44

      // Only ever ran once.
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying

      TestUtils.diffOutputSubdir(subDir)
    }

    it("should map over calls and results with symmetrical results") {
      implicit val rt: ResultTracker = ResultTrackerNone()

      val call1: FunctionCallWithProvenance[Vector[Int]] = UnknownProvenance(Vector(11, 22, 33))
      val result1: FunctionCallResultWithProvenance[Vector[Int]] = call1.resolve

      val call2a: MapWithProvenance[Vector, Int, Int]#Call = call1.map(MyIncrement)
      val call2b: MapWithProvenance[Vector, Int, Int]#Call = result1.map(MyIncrement)
      call2a.unresolve.toString shouldBe "MapWithProvenance(raw(Vector(11, 22, 33)),raw(com.cibo.provenance.MyIncrement@v0.0))"
      call2b.unresolve.toString shouldBe "MapWithProvenance(raw(Vector(11, 22, 33)),raw(com.cibo.provenance.MyIncrement@v0.0))"

      val result2 = call2a.resolve

      result2.indices.resolve.output shouldBe (0 to 2)
      result2(2).resolve.output shouldBe 34
    }

    it("maps efficiently") {
      val subDir = "mappable-results-map"
      val testDataDir = f"$outputBaseDir/$subDir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      MakeDummyOutputList.runCount = 0

      val myResult1: MakeDummyOutputList.Result = MakeDummyOutputList().resolve
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying

      MyIncrement.runCount = 0

      val myResult2: MapWithProvenance[List, Int, Int]#Call = myResult1.map(MyIncrement)
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying
      MyIncrement.runCount shouldBe 0

      val r2 = myResult2(2)
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying
      MyIncrement.runCount shouldBe 0

      r2.resolve.output shouldBe 34
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying
      MyIncrement.runCount shouldBe 4 * rt.countUnderlying // ideally 1, if we can make the system be lazy on the full map

      myResult2(2).resolve.output shouldBe 34
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying // no change
      MyIncrement.runCount shouldBe 4 * rt.countUnderlying // no change, again 1 if we can get lazy

      myResult2(0).resolve.output shouldBe 12
      myResult2(1).resolve.output shouldBe 23
      myResult2(2).resolve.output shouldBe 34 // already done
      myResult2(3).resolve.output shouldBe 45

      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying // no change
      MyIncrement.runCount shouldBe 4 * rt.countUnderlying // no repeat calls

      TestUtils.diffOutputSubdir(subDir)
    }

    it("should `scatter` and retain the higher kind of the Traversable") {
      implicit val rt: ResultTracker = ResultTrackerNone()

      val i1 = UnknownProvenance(Vector(1, 2, 3))
      val m1 = i1.map(MyIncrement)
      val m2 = m1.map(MyIncrement)

      val s1: Vector[FunctionCallWithProvenance[Int]] = m2.scatter

      val s2: Vector[FunctionCallResultWithProvenance[Int]] = m2.resolve.scatter

      s1.map(_.resolve.output) shouldBe Vector(3, 4, 5)
      s2.map(_.output) shouldBe Vector(3, 4, 5)
    }

    it("handles `scatter` with efficient pass-through to the underling implementation") {
      val subDir = "mappable-results-scatter"
      val testDataDir = f"$outputBaseDir/$subDir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      MakeDummyOutputList.runCount = 0

      val myResult = MakeDummyOutputList().resolve
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying

      val seqOfResults = myResult.scatter
      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying

      val r0 = seqOfResults(0)
      val r1 = seqOfResults(1)
      val r2 = seqOfResults(2)
      val r3 = seqOfResults(3)

      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying

      r0.output shouldBe 11
      r1.output shouldBe 22
      r2.output shouldBe 33
      r3.output shouldBe 44

      MakeDummyOutputList.runCount shouldBe 1 * rt.countUnderlying

      TestUtils.diffOutputSubdir(subDir)
    }
  }

  describe("gathering of Traversables") {

    it("works implicitly, and keeps history") {
      val subDir = "gather"
      val testDataDir = f"$outputBaseDir/$subDir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      // Make a list with a variety of tracked objects, all returning an Int.
      // Each has different classes, different states, and different history depth.
      val lst1 = List[ValueWithProvenance[Int]](
        UnknownProvenance[Int](123),
        UnknownProvenance[Int](456).resolve,
        MyIncrement(100),
        MyIncrement(MyIncrement(200)).resolve
      )

      // The code wants a ValueWithProvenance[List[Int]]
      // But we are giving it a List[ValueWithProvenance[Int]], each with independent history.
      // This should implicitly convert to the correct shape.
      val sum = SumValues(lst1)

      // Be sure it worked.
      sum.resolve.output shouldEqual 882

      // Dig through the history and be sure we can reach the depths...

      // Be sure the original history is individually trackable.
      val inputForSum = sum.i1.asInstanceOf[GatherWithProvenance[List, Int]#Call]
      val gatheredInputs = inputForSum.gatheredInputs.asInstanceOf[List[ValueWithProvenance[Int]]]

      // the last value in the list is a MyIncrement.Result
      val lastInput: ValueWithProvenance[Int] = gatheredInputs.last
      lastInput.asInstanceOf[FunctionCallResultWithProvenance[Int]]
      lastInput.asInstanceOf[MyIncrement.Result]

      // the nested MyIncrement input was a MyIncrement.Call, but it was replaced with a result.
      val lastInputInput: ValueWithProvenance[_] = lastInput.resolve.call.inputs.head
      lastInputInput.asInstanceOf[ValueWithProvenance[Int]]
      lastInputInput.asInstanceOf[MyIncrement.Result]

      // the input to that is a constant 200
      val lastInputInputInput: ValueWithProvenance[_] = lastInputInput.resolve.call.inputs.head
      lastInputInputInput.asInstanceOf[ValueWithProvenance[Int]]
      lastInputInputInput.asInstanceOf[UnknownProvenanceValue[Int]]

      // Be sure everything can reveal its intermediate values
      lastInputInputInput.resolve.output shouldBe 200           // UnknownProvenance(200)
      lastInputInput.resolve.output shouldBe 201                // MyIncrement(UnknownProvenance(200))
      lastInput.resolve.output shouldBe 202                     // MyIncrement(MyIncrement(UnknownProvenance(200)))
    }
  }

  describe("Options") {
    it("work") {
      implicit val rt = ResultTrackerNone()

      val s1: Option[String] = Some("hello")
      val n1: Option[String] = None

      val s1p = UnknownProvenance(s1)
      val n1p = UnknownProvenance(n1)

      s1p.isEmpty.resolve.output shouldBe false
      n1p.isEmpty.resolve.output shouldBe true

      s1p.nonEmpty.resolve.output shouldBe true
      n1p.nonEmpty.resolve.output shouldBe false

      s1p.get.resolve.output shouldBe "hello"

      intercept[Exception] {
        n1p.get.resolve
      }

      val s2p = s1p.map(AppendSuffix)
      s2p.resolve.output.get shouldBe "hello-mysuffix"
    }
  }
}

object MakeDummyOutputList extends Function0WithProvenance[List[Int]] {
  val currentVersion = Version("0.0")

  @transient
  var runCount: Int = 0 // warning: var

  def impl = {
    runCount += 1
    List(11, 22, 33, 44)
  }
}

object MyIncrement extends Function1WithProvenance[Int, Int] {
  val currentVersion = Version("0.0")

  @transient
  var runCount: Int = 0 // warning: var

  def impl(x: Int) = {
    runCount += 1
    x + 1
  }
}

object CountList extends Function1WithProvenance[List[Int], Int] {
  val currentVersion: Version = Version("0.1")
  def impl(in: List[Int]) = {
    println(in)
    in.size
  }
}

object SumValues extends Function1WithProvenance[List[Int], Int] {
  val currentVersion: Version = Version("0.1")
  def impl(in: List[Int]) = in.sum
}

object AppendSuffix extends Function1WithProvenance[String, String] {
  val currentVersion: Version = Version("0.1")
  val suffix = "-mysuffix"
  def impl(prefix: String): String = prefix + suffix
}



