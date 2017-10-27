package com.cibo.provenance

import java.io.File

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance._
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

      implicit val bi: BuildInfo = DummyBuildInfo

      val testDataDir = f"$baseTestDir/deflate1"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val rt = ResultTrackerSimple(SyncablePath(testDataDir))

      val s1a = addMe(2, 2)
      val r1a = s1a.resolve

      val s1b = s1a.deflate
      val s1c = s1b.inflate
      s1c shouldEqual s1a

      /*
      val s2 = addMe(5, 7)
      val s3 = multMe(s1, s2)
      val s4 = multMe(s3, 2)

      val s5 = multMe(multMe(addMe(2,2), addMe(5,7)), 2)
      */

    }
  }
}

object addMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")

  // NOTE: This public var is reset during tests, and is a cheat to peek-inside whether or no impl(),
  // which is encapsulated, actually runs.
  var runCount: Int = 0

  def impl(a: Int, b: Int): Int = {
    runCount += 1
    a + b
  }
}
