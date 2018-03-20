package com.cibo.provenance

import com.cibo.io.s3.SyncablePath
import org.scalatest.{FunSpec, Matchers}

class AbstractOutputSealedSpec extends FunSpec with Matchers {

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = DummyBuildInfo

  describe("functions with an sealed trait return type") {
    it("should work") {
      val testSubdir = f"abstract-outputs-sealed"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val p1 = pickAPet("Kittykitty")
      (
        p1.resolve.output match {
          case _: Dog => "dog"
          case _: Cat => "cat"
          case _ => "other"
        }
      ) shouldBe "cat"

      val p2 = pickAPet("Yippydog")
      (
        p2.resolve.output match {
          case _: Dog => "dog"
          case _: Cat => "cat"
          case _ => "other"
        }
      ) shouldBe "dog"

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }
}

sealed trait PetSealedTrait { def name: String }
case class Cat(name: String) extends PetSealedTrait
case class Dog(name: String) extends PetSealedTrait

object PetSealedTrait {
  import io.circe.generic.semiauto._
  implicit val encoder = deriveEncoder[PetSealedTrait]
  implicit val decoder = deriveDecoder[PetSealedTrait]
}

object pickAPet extends Function1WithProvenance[String, PetSealedTrait] {
  val currentVersion = Version("0.1")
  def impl(name: String): PetSealedTrait = {
    if (name.toLowerCase.toCharArray.head.toInt <= 'm'.toInt) {
      Cat(name)
    } else {
      Dog(name)
    }
  }
}
