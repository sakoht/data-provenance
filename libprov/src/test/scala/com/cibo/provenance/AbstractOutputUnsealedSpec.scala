package com.cibo.provenance

import io.circe.{Decoder, Encoder}

import com.cibo.io.s3.SyncablePath
import org.scalatest.{FunSpec, Matchers}
import com.cibo.aws.AWSClient.Implicits.s3SyncClient
import com.cibo.io.s3.SyncablePathBaseDir.Implicits.default

/*
 * When output is abstract, but the abstract type is a sealed trait,
 * the circe automatically supports serialization, so things just work.
 *
 * This tests the opposite scenario, where the function output is an abstract
 * type (a trait, in this case), with a potentially open-ended number of subclasses.
 *
 * This uses a codec that expects a singe key in the JSON to store/reveal the subclass
 * name.  It further expects that each subclasss to have a companion class with
 * an encoder and decoder available.
 */
class AbstractOutputUnsealedSpec extends FunSpec with Matchers {

  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("functions with an unsealed trait return type") {
    it("should work") {
      val testSubdir = f"abstract-outputs-unsealed"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(SyncablePath(testDataDir))
      rt.wipe

      val p1 = pickAPet2("Kittykitty")
      (
        p1.resolve.output match {
          case _: Dog2 => "dog"
          case _: Cat2 => "cat"
          case _ => "other"
        }
      ) shouldBe "cat"

      val p2 = pickAPet2("Yippydog")
      (
        p2.resolve.output match {
          case _: Dog2 => "dog"
          case _: Cat2 => "cat"
          case _ => "other"
        }
      ) shouldBe "dog"

      TestUtils.diffOutputSubdir(testSubdir)
    }
  }
}

trait PetUnsealedTrait2 { def name: String }
case class Cat2(name: String, meow: Boolean = true) extends PetUnsealedTrait2
case class Dog2(name: String, bark: Boolean = true) extends PetUnsealedTrait2

import io.circe.generic.semiauto._
object Cat2 {
  val encoder = deriveEncoder[Cat2]
  val decoder = deriveDecoder[Cat2]
}
object Dog2 {
  val encoder = deriveEncoder[Dog2]
  val decoder = deriveDecoder[Dog2]
}

object PetUnsealedTrait2 {
  implicit val codec: CirceJsonCodec[PetUnsealedTrait2] = Codec.createAbstractCodec[PetUnsealedTrait2]()
  implicit val encoder: Encoder[PetUnsealedTrait2] = codec.encoder
  implicit val decoder: Decoder[PetUnsealedTrait2] = codec.decoder
}

object pickAPet2 extends Function1WithProvenance[String, PetUnsealedTrait2] {
  val currentVersion = Version("0.1")
  def impl(name: String): PetUnsealedTrait2 = {
    if (name.toLowerCase.toCharArray.head.toInt <= 'm'.toInt) {
      Cat2(name)
    } else {
      Dog2(name)
    }
  }
}
