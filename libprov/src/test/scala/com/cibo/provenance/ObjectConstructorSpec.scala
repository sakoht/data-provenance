package com.cibo.provenance

import scala.language.existentials
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest._

import com.cibo.provenance.oo._
import com.cibo.provenance.implicits._

class ObjectConstructorSpec extends FunSpec with Matchers {
  // This is the root for test output.
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  // This dummy BuildInfo is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("constructors") {
    it("work with provenance tracking") {
      val testSubdir = "constructor"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt: ResultTrackerForSelfTest = ResultTrackerForSelfTest(testDataDir)
      rt.wipe()

      val foo     = Foo(100, "hello")                 // no tracking
      val barCall = Bar.withProvenance(1.23).resolve  // with tracking
      val bazCall = Baz.withProvenance(foo, barCall)  // with tracking

      val baz: Baz = bazCall.resolve.output
      baz.foo.i shouldBe 100
      baz.bar.f shouldBe 1.23
      baz.buz(3) shouldBe 3 * 100 * 1.23

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }
}

// test classes

case class Foo(i: Int, s: String)

object Foo extends ObjectCompanion2[Int, String, Foo](Version("0.1")) {
  implicit val encoder: Encoder[Foo] = deriveEncoder[Foo]
  implicit val decoder: Decoder[Foo] = deriveDecoder[Foo]
}


case class Bar(f: Double)

object Bar extends ObjectCompanion1[Double, Bar](Version("0.1")) {
  implicit val encoder: Encoder[Bar] = deriveEncoder[Bar]
  implicit val decoder: Decoder[Bar] = deriveDecoder[Bar]
}


case class Baz(foo: Foo, bar: Bar) {
  def buz(n: Int): Double = foo.i * bar.f * n
}

object Baz extends ObjectCompanion2[Foo, Bar, Baz](Version("0.1")) {
  implicit val encoder: Encoder[Baz] = deriveEncoder[Baz]
  implicit val decoder: Decoder[Baz] = deriveDecoder[Baz]
}
