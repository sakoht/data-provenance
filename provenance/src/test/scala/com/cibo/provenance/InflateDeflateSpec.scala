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
  val baseTestDir: String = f"/tmp/" + sys.env.getOrElse("USER", "anonymous") + "/rt"

  describe("Deflation and inflation") {

    it("work on simple provenance.") {

      val testDataDir = f"$baseTestDir/deflate-simple"
      FileUtils.deleteDirectory(new File(testDataDir))
      
      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val s1a = multMe(2, 2)
      val _ = s1a.resolve
      
      val s1b = s1a.deflate
      val s1c = s1b.inflate
      s1c shouldEqual s1a
    }

    it("works on nested provenance") {
      val testDataDir = f"$baseTestDir/deflate-nested"
      FileUtils.deleteDirectory(new File(testDataDir))
      
      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))
      
      val s1 = addMe(2, 2) 
      val s2 = addMe(5, 7)
      val s3 = multMe(s1, s2)
      
      val s4a: multMe.Call = multMe(s3, 2)

      val s4b: multMe.Call = multMe(multMe(addMe(2,2), addMe(5,7)), 2)
      s4b shouldEqual s4a

      // Round-trip through deflation/inflation loses some type information.
      val s4bDeflated: FunctionCallWithProvenanceDeflated[Int] = rt.saveCall(s4b)
      val s4bInflated: FunctionCallWithProvenance[Int] = s4bDeflated.inflate

      // But the inflated object is of the correct type, where the code is prepared to recognize it.
      val s4c: multMe.Call = s4bInflated match {
        case i1withTypeKnown: multMe.Call => i1withTypeKnown
        case _ => throw new RuntimeException("Re-inflated object does not match expectred class.")
      }
      s4c shouldEqual s4b

    }
  }
}

object addMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a + b
}

object multMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a * b
}



