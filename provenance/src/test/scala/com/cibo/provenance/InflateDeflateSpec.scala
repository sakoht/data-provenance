package com.cibo.provenance

import java.io.File

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.tracker.ResultTrackerSimple
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

      val s1a = mult2(2, 2)
      val _ = s1a.resolve

      val s1b = s1a.deflate
      val s1c = s1b.inflate
      s1c shouldEqual s1a
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
  }
}

object add2 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a + b
}

object mult2 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a * b
}



