package com.cibo.provenance

import org.scalatest.{FunSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by ssmith on 5/11/17.
  */


object AddInts extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a + b
}

object MultiplyInts extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("2.2")
  def impl(a: Int, b: Int): Int = a * b
}

object myFunc extends Function2WithProvenance[String, Double, Int] {
  val currentVersion: Version = Version("3.5")
  def impl(d: Double, n: Int): String = (1 to n).map(_ => d.toString).mkString(",")
}

class FunctionWithProvenanceSpec extends FunSpec with Matchers {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  // Use a dummy result tracker that stores nothing, re-runs everything.
  // Set the implicit BuildInfo to the DummyBuildInfo which uses a fake git commit and build ID.
  implicit val rt: ResultTracker = ResultTrackerNone()(currentBuildInfo = DummyBuildInfo)

  describe("functions with provenance") {
    it("should work with raw/primitive inputs") {
      val sig = myFunc(1.23, 3)
      val result = sig.run

      result.output shouldEqual "1.23,1.23,1.23"

      result.provenance shouldEqual sig

      assert(sig.isInstanceOf[myFunc.Call])
      assert(result.isInstanceOf[myFunc.Result])

      result.toString shouldBe "(1.23,1.23,1.23 <- myFunc(raw(1.23),raw(3)))"
    }

    it("should work with input values that are results with provenance") {

      val f1 = AddInts(1, 2)
      val r1 = f1.run
      r1.output shouldEqual 3
      r1.provenance shouldEqual f1

      r1.toString shouldEqual "(3 <- AddInts(raw(1),raw(2)))"

      val f2 = MultiplyInts(2, r1)
      val r2 = f2.run
      r2.output shouldEqual 6
      r2.provenance shouldEqual f2

      r2.toString shouldEqual "(6 <- MultiplyInts(raw(2),(3 <- AddInts(raw(1),raw(2)))))"
      
      r2.provenance.inputTuple._1 shouldEqual UnknownProvenance(2)
      r2.provenance.inputTuple._2 shouldEqual r1

      r2.provenance.inputTuple._1.resolve.output shouldEqual 2
      r2.provenance.inputTuple._1.resolve.provenance shouldEqual UnknownProvenance(2)

      r2.provenance.inputTuple._2.resolve.output shouldEqual 3
      r2.provenance.inputTuple._2.resolve.provenance shouldEqual f1

      r2.provenance.inputTuple._2.resolve.provenance shouldEqual f1

      val f3 = AddInts(r1, r2)
      val r3 = f3.run
      r3.output shouldEqual 9
      r3.provenance.inputTuple._1 shouldEqual r1
      r3.provenance.inputTuple._2 shouldEqual r2

      r3.toString shouldEqual "(9 <- AddInts((3 <- AddInts(raw(1),raw(2))),(6 <- MultiplyInts(raw(2),(3 <- AddInts(raw(1),raw(2)))))))"
    }

    it("should work with nested signatures that are never called until the end") {
      val f1 = AddInts(1, 2)
      val f2 = MultiplyInts(3, 4)
      val f3 = AddInts(f1, f2)

      f3.inputTuple._1 shouldEqual f1
      f3.inputTuple._2 shouldEqual f2

      val r3 = f3.run
      r3.output shouldEqual 15
      r3.provenance shouldEqual f3
      r3.provenance.inputTuple._1 shouldEqual f1
      r3.provenance.inputTuple._2 shouldEqual f2

      r3.toString shouldEqual "(15 <- AddInts(AddInts(raw(1),raw(2)),MultiplyInts(raw(3),raw(4))))"
    }
  }
}



