package com.cibo.provenance

import scala.language.existentials
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest._

import com.cibo.provenance.oo._
import com.cibo.provenance.implicits._


class ObjectAccessorForProductsSpec extends FunSpec with Matchers {
  // This is the root for test output.
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  // This dummy BuildInfo is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("product accessors") {
    it("work with provenance tracking") {
      val testSubdir = "accessors"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt: ResultTrackerForSelfTest = ResultTrackerForSelfTest(testDataDir)
      rt.wipe()

      val obj = Fizz.withProvenance(123, 9.87)
      obj.a.resolve.output shouldBe 123
      obj.b.resolve.output shouldBe 9.87

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }
}

// test classes

case class Fizz(a: Int, b: Double)

object Fizz extends ObjectCompanion2[Int, Double, Fizz](NoVersion) { outer =>
  implicit def encoder: Encoder[Fizz] = deriveEncoder[Fizz]
  implicit def decoder: Decoder[Fizz] = deriveDecoder[Fizz]

  implicit class FizzWithProvenance(obj: ValueWithProvenance[Fizz]) extends ProductWithProvenance[Fizz](obj) {
    val a = productElement[Int]("a")
    val b = productElement[Double]("b")
  }
}
