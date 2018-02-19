package com.cibo.provenance

import java.io.File

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.monadics.MapWithProvenance
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by ssmith on 10/26/17.
  */
class InflateDeflateSpec extends FunSpec with Matchers {
  val outputBaseDir: String = TestUtils.testOutputBaseDir

  describe("Deflation and inflation") {

    it("work on simple provenance.") {

      val testDataDir = f"$outputBaseDir/deflate-simple"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val i1: mult2.Call = mult2(2, 2)
      val _ = i1.resolve

      val d1 = i1.deflate
      val i2 = d1.inflate
      i2 shouldEqual i1
    }

    it ("partially or fully inflate") {
      val testDataDir = f"$outputBaseDir/deflate-reinflate"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val i1: mult2.Call = mult2(2, 2)
      val _ = i1.resolve

      val d1 = i1.deflate
      val i2 = d1.inflate

      val i2b = d1.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[Int]]
      val i2co = rt.loadInflatedCallWithDeflatedInputsOption[Int](i2b.functionName, i2b.functionVersion, i2b.inflatedCallDigest)
      val i2c = i2co.get
      val i2ci = i2c.inflateInputs
      i2ci shouldEqual i2
    }

    it("works on nested provenance") {
      val testDataDir = f"$outputBaseDir/deflate-nested"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val s1 = add2(2, 2)
      val s2 = add2(5, 7)
      val s3 = mult2(s1, s2)

      val s4a: mult2.Call = mult2(s3, 2)

      val s4b: mult2.Call = mult2(mult2(add2(2,2), add2(5,7)), 2)
      s4b shouldEqual s4a

      // Round-trip through deflation/inflation loses some type information.
      val s4bDeflated: FunctionCallWithProvenanceDeflated[Int] = rt.saveCall(s4b)
      val s4bInflated: FunctionCallWithProvenance[Int] = s4bDeflated.inflate

      // But the inflated object is of the correct type, where the code is prepared to recognize it.
      val s4c: mult2.Call = s4bInflated match {
        case i1withTypeKnown: mult2.Call => i1withTypeKnown
        case _ => throw new RuntimeException("Re-inflated object does not match expectred class.")
      }
      s4c shouldEqual s4b
    }

    it("work on functions.") {
      val testDataDir = f"$outputBaseDir/deflate-functions"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val inflatedCall1 = UnknownProvenance(mult2)
      val inflatedResult1 = inflatedCall1.resolve

      val deflatedCall1 = inflatedCall1.deflate
      val inflatedCall2 = deflatedCall1.inflate
      inflatedCall2 shouldEqual inflatedCall1

      val deflatedResult1: FunctionCallResultWithProvenanceDeflated[mult2.type] = inflatedResult1.deflate
      val inflatedResult2: FunctionCallResultWithProvenance[mult2.type] = deflatedResult1.inflate
      inflatedResult2 shouldEqual inflatedResult1
    }

    it("work on functions that take other functions w/ provenance as parameters") {
      val testDataDir = f"$outputBaseDir/deflate-functions"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val u = UnknownProvenance(List(100, 200, 300))

      // MapWithProvenance is nasty b/c the function is itself an input.
      // It uses binary encoding (wrapped in Circe JSON).
      val inflated1: MapWithProvenance[Int, List, Int]#Call = u.map(incrementInt)
      val inflated1b: MapWithProvenance[Int, List, Int]#Call = u.map(incrementInt)
      //inflated1 shouldBe inflated1b

      val inflated1result = inflated1.resolve
      inflated1result.output shouldBe List(101, 201, 301)

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      // Note: something causes these to be equal as strings but not pass the eq check.
      inflated2.toString shouldEqual inflated1.toString

      rt.hasResultForCall(inflated2) shouldBe true
    }

    it("works with functions that return functions with provenance as output") {
      val testDataDir = f"$outputBaseDir/deflate-functions"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val inflated1: fmaker.Call = fmaker()
      val inflated1result = inflated1.resolve
      inflated1result.output shouldBe incrementInt

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      inflated2 shouldEqual inflated1
    }
  }

}

object listMaker extends Function0WithProvenance[List[Int]] {
  val currentVersion: Version = Version("1.0")
  def impl(): List[Int] = List(100, 200, 300)
}

object add2 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a + b
}

object mult2 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a * b
}

object incrementInt extends Function1WithProvenance[Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int): Int = a + 1
}

object fmaker extends Function0WithProvenance[Function1WithProvenance[Int, Int]] {
  val currentVersion: Version = Version("1.0")
  def impl(): Function1WithProvenance[Int, Int] = incrementInt
}
