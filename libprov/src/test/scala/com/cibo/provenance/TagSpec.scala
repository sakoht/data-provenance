package com.cibo.provenance

import org.scalatest._

class TagSpec extends FunSpec with Matchers {

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("tags") {
    val testSubdir = "tags"
    val testDataDir = f"$testOutputBaseDir/$testSubdir"
    implicit val rt = ResultTrackerForSelfTest(testDataDir)
    rt.wipe()

    val c1 = squareInt(2)
    val r1 = c1.resolve

    val c2 = squareInt(c1)
    val r2 = c2.resolve

    val c3 = cubeDouble(1.23)
    val r3 = c3.resolve

    val t1 = c1.addTag("tag 1")
    t1.resolve

    val t2 = c2.addTag("tag 2")
    t2.resolve

    val t3 = c3.addTag("tag 3")
    t3.resolve

    // API on the FunctionWithProvenance

    it("can be listed from a function with provenance") {
      squareInt.findTags.toSet shouldEqual Set(Tag("tag 1"), Tag("tag 2"))
      cubeDouble.findTags.toSet shouldEqual Set(Tag("tag 3"))
    }

    it("can be used to find results via the FunctionWithProvenance whose results they were applied-to") {
      val tagged1 = squareInt.findResultsByTag("tag 1")
      val tagged2 = squareInt.findResultsByTag("tag 2")
      val tagged3 = cubeDouble.findResultsByTag(Tag("tag 3"))

      tagged1.map(_.normalize).toSet shouldEqual Set(r1.normalize)
      tagged2.map(_.normalize).toSet shouldEqual Set(r2.normalize)
      tagged3.map(_.normalize).toSet shouldEqual Set(r3.normalize)
    }

    it("does not find results on the wrong class") {
      cubeDouble.findResultsByTag("tag 1").toList shouldEqual Nil
      squareInt.findResultsByTag("tag 3").toList shouldEqual Nil
    }


    // ResultTracker API

    it("can be listed from a result tracker w/o any other information") {
      rt.findTags.toSet shouldEqual Set(Tag("tag 1"), Tag("tag 2"), Tag("tag 3"))
    }

    it("can be queried from a result tracker for detailed data") {
      rt.findTagApplications.map(_.subject).map(_.load.normalize).toSet shouldEqual Set(r1.normalize, r2.normalize, r3.normalize)
    }

    it("can be listed from a result tracker by output class name") {
      rt.findTagsByOutputType("scala.Int").toSet shouldEqual Set(Tag("tag 1"), Tag("tag 2"))
      rt.findTagsByOutputType("scala.Double").toSet shouldEqual Set(Tag("tag 3"))
    }

    it("can be listed from a result tracker by result function name") {
      rt.findTagsByResultFunctionName("com.cibo.provenance.squareInt").toSet shouldEqual Set(Tag("tag 1"), Tag("tag 2"))
      rt.findTagsByResultFunctionName("com.cibo.provenance.cubeDouble").toSet shouldEqual Set(Tag("tag 3"))
    }

    it("cat be used to query the result tracker for results w/o knowing the specific functions or types") {
      val tagged1 = rt.findByTag("tag 1")
      val tagged2 = rt.findByTag("tag 2")
      val tagged3 = rt.findByTag(Tag("tag 3"))

      tagged1.map(_.load.normalize).toSet shouldEqual Set(r1.normalize)
      tagged2.map(_.load.normalize).toSet shouldEqual Set(r2.normalize)
      tagged3.map(_.load.normalize).toSet shouldEqual Set(r3.normalize)
    }

    it("can be removed") {
      /*
      // Repeat all of the above with tag 2 removed.
      val t2removed = c2.removeTag("tag 2")
      t2removed.resolve

      val tagged1 = squareInt.findResultsByTag("tag 1")
      val tagged2 = squareInt.findResultsByTag("tag 2")
      val tagged3 = cubeDouble.findResultsByTag(Tag("tag 3"))
      tagged1.map(_.normalize).toSet shouldEqual Set(r1.normalize)
      tagged2.map(_.normalize).toSet shouldEqual Set.empty
      tagged3.map(_.normalize).toSet shouldEqual Set(r3.normalize)

      rt.findTags.toSet shouldEqual Set(Tag("tag 1"), Tag("tag 3"))

      rt.findTagApplications.map(_._1).map(_.load.normalize).toSet shouldEqual Set(r1.normalize, r3.normalize)

      rt.findTagsByResultFunctionName("com.cibo.provenance.squareInt").toSet shouldEqual Set(Tag("tag 1"))

      val tagged2b = rt.findByTag("tag 2")
      tagged2b.map(_.load.normalize).toSet shouldEqual Set.empty
      */
    }
  }
}

object squareInt extends Function1WithProvenance[Int, Int] {
  val currentVersion = Version("0.1")
  def impl(x: Int): Int = x * x
}

object cubeDouble extends Function1WithProvenance[Double, Double] {
  val currentVersion = Version("0.1")
  def impl(x: Double): Double = x * x * x
}
