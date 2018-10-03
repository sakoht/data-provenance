package com.cibo.provenance

import com.cibo.io.s3.SyncablePath
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest._


case class Foo(i: Int, s: String) {
  // a zero-parameter method
  def iTimes2() = i * 2
}

case class Bar(f: Double)

case class Baz(foo: Foo, bar: Bar) {
  // a one-parameter method that also uses the input objects
  def buz(n: Int): Double = foo.i * bar.f * n
}

object Foo extends ObjectCompanion2[Int, String, Foo](Version("0.1")) {
  implicit val encoder: Encoder[Foo] = deriveEncoder[Foo]
  implicit val decoder: Decoder[Foo] = deriveDecoder[Foo]
}

object Bar extends ObjectCompanion1[Double, Bar](Version("0.1")) {
  implicit val encoder: Encoder[Bar] = deriveEncoder[Bar]
  implicit val decoder: Decoder[Bar] = deriveDecoder[Bar]
}

object Baz extends ObjectCompanion2[Foo, Bar, Baz](Version("0.1")) {
  implicit val encoder: Encoder[Baz] = deriveEncoder[Baz]
  implicit val decoder: Decoder[Baz] = deriveDecoder[Baz]
}


class ObjectCompanionSpec extends FunSpec with Matchers {
  // Put the classes above in-scope.

  // This is the root for test output.
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir

  // This dummy BuildInfo is used by all ResultTrackers below.
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("constructors") {

    it("works with provenance tracking") {
      val testSubdir = "constructor"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt: ResultTrackerForTest = ResultTrackerForTest(SyncablePath(testDataDir))
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


