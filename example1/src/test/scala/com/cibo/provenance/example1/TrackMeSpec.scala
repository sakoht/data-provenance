package com.cibo.provenance.examples

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by ssmith on 9/20/17.
  */

class TrackMeSpec extends FunSpec with Matchers {

  val baseTestDir: String = f"/tmp/" + sys.env.getOrElse("USER", "anonymous") + "/rt.example1"

  describe("The TrackMe demo app") {
    it("should run without errors") {
      val testDataDir = f"$baseTestDir/trackme1"
      FileUtils.deleteDirectory(new File(testDataDir))

      TrackMe.run(TrackMeParams(dbPath = testDataDir))
      TrackMe.run(TrackMeParams(dbPath = testDataDir))
    }
  }
}


