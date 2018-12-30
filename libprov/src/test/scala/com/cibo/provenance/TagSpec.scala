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

    val c4 = cubeDouble(2.34)
    c4.resolve

    val t1 = c1.addTag("tag 1")
    t1.resolve

    val t2 = c2.addTag("tag 2")
    t2.resolve

    val t3 = c3.addTag("tag 3")
    t3.resolve

    // Make some tags and remove them.  These should only appear in history.

    val t4 = c4.addTag("tag 4 to remove from c4")
    t4.resolve
    val t4Removal = c4.removeTag("tag 4 to remove from c4")
    t4Removal.resolve

    // This tag is the 2nd tag on c2.  Adding this to ensure it is removed w/o interference with other tags.
    val t5 = c2.addTag("tag 5 to remove from c2")
    t5.resolve
    val t5Removal = c2.removeTag("tag 5 to remove from c2")
    t5Removal.resolve

    // This is a repeat of the tag on c1, but put onto c3, and removed.
    val t6 = c3.addTag("tag 1")
    t6.resolve
    val t6Removal = c3.removeTag("tag 1")
    t6Removal.resolve

    // API on the FunctionWithProvenance
    it("have the expected counts in storage") {
      rt.findTags.size shouldBe 3
      rt.findTagHistory.size shouldBe 9
    }

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
      rt.findTagApplications.map(_.subjectData).map(_.load.normalize).toSet shouldEqual Set(r1.normalize, r2.normalize, r3.normalize)
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

    it("can be replaced") {
      // Add back "tag 1" to c3, which we added above but then removed from c3 above.
      // It is also on c1, which has not changed.
      val t7 = c3.addTag("tag 1")
      t7.resolve

      rt.findTags.size shouldBe 3
      rt.findTagHistory.size shouldBe 10

      val tagged1b = rt.findByTag("tag 1")
      tagged1b.map(_.load.normalize).toSet shouldEqual Set(r1.normalize, r3.normalize)

      val taggedAll = rt.findTagApplications.filter(_.tag == Tag("tag 1"))
      taggedAll.map(_.subjectData.load.normalize).toSet shouldEqual Set(r1.normalize, r3.normalize)

      val tagsOnF1 = squareInt.findResultsByTag(Tag("tag 1"))
      val tagsOnF2 = cubeDouble.findResultsByTag(Tag("tag 1"))

      tagsOnF1.map(_.normalize).toSet shouldEqual Set(r1.normalize)
      tagsOnF2.map(_.normalize).toSet shouldEqual Set(r3.normalize)

      rt.findTags.toSet shouldEqual Set(Tag("tag 1"), Tag("tag 2"), Tag("tag 3"))
      rt.findTagApplications.map(_.subjectData).map(_.load.normalize).toSet shouldEqual Set(r1.normalize, r2.normalize, r3.normalize)

      rt.findTagsByResultFunctionName("com.cibo.provenance.squareInt").toSet shouldEqual Set(Tag("tag 1"), Tag("tag 2"))
      rt.findTagsByResultFunctionName("com.cibo.provenance.cubeDouble").toSet shouldEqual Set(Tag("tag 1"), Tag("tag 3"))
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
