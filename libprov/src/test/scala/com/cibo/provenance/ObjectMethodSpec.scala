package com.cibo.provenance

import scala.language.existentials
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest._

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.oo._

class ObjectMethodSpec extends FunSpec with Matchers {
  // This is the root for test output.
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  // This dummy BuildInfo is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("method calls") {
    it("work with provenance tracking") {
      val testSubdir = "methods"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt: ResultTrackerForSelfTest = ResultTrackerForSelfTest(testDataDir)
      rt.wipe()

      val obj = Boo.withProvenance(2)
      obj.incrementMe().resolve.output shouldBe 3
      obj.addToMe(90).resolve.output shouldBe 92
      obj.catString(3, "z").resolve.output shouldBe "zzzzz"

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }
}

// test classes

case class Boo(i: Int) {
  def incrementMe: Int = i + 1

  def addToMe(n: Int): Int = i + n

  def catString(n: Int, s: String): String =
    (0 until (i + n)).map(_ => s).mkString("")
}

object Boo extends ObjectCompanion1[Int, Boo](Version("0.1")) { self =>
  implicit val encoder: Encoder[Boo] = deriveEncoder[Boo]
  implicit val decoder: Decoder[Boo] = deriveDecoder[Boo]

  // Add tracking for methods on Boo that we intend to call with tracking.
  val incrementMe = mkMethod0[Int]("incrementMe", Version("0.1"))
  val addToMe = mkMethod1[Int, Int]("addToMe", Version("0.1"))
  val catString = mkMethod2[Int, String, String]("catString", Version("0.1"))

  // Make a type class so these can be called in a syntactically-friendly way.
  implicit class WrappedMethods(obj: ValueWithProvenance[Boo]) {
    val incrementMe = self.incrementMe.wrap(obj)
    val addToMe = self.addToMe.wrap(obj)
    val catString = self.catString.wrap(obj)
  }
}
