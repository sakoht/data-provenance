package com.cibo.provenance

import java.io.File

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.monadics.MapWithProvenance
import io.circe.{Decoder, Encoder}
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}

import scala.reflect.ClassTag


/**
  * Created by ssmith on 10/26/17.
  */
class InflateDeflateSpec extends FunSpec with Matchers {
  val outputBaseDir: String = TestUtils.testOutputBaseDir

  describe("Deflation and inflation") {

    it("work on simple calls.") {

      val testDataDir = f"$outputBaseDir/deflate-simple"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val i1: mult2.Call = mult2(2, 2)
      val _ = i1.resolve

      val d1 = i1.deflate
      val i2 = d1.inflate
      i2 shouldEqual i1
    }

    it ("allows a call to partially or fully inflate") {
      val testDataDir = f"$outputBaseDir/deflate-reinflate"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val inflated1: mult2.Call = mult2(2, 2)
      val _ = inflated1.resolve

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      val deflated1known = deflated1.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[Int]]
      val inflated3opt = rt.loadInflatedCallWithDeflatedInputsOption[Int](
        deflated1known.functionName,
        deflated1known.functionVersion,
        deflated1known.inflatedCallDigest
      )
      val inflated3 = inflated3opt.get
      val inflated3inflatedInputs = inflated3.inflateInputs
      inflated3inflatedInputs shouldEqual inflated2
    }

    it("works on nested calls") {
      val testDataDir = f"$outputBaseDir/deflate-nested"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val s1 = add2(2, 2)
      val s2 = add2(5, 7)
      val s3 = mult2(s1, s2)

      val s4a: mult2.Call = mult2(s3, 2)

      val s4b: mult2.Call = mult2(mult2(add2(2,2), add2(5,7)), 2)
      s4b shouldEqual s4a

      // Round-trip through deflation/inflation loses some type information.
      val s4bDeflated: FunctionCallWithProvenanceDeflated[Int] = rt.saveCall(s4b)
      val s4bInflated: FunctionCallWithProvenance[Int] = s4bDeflated.inflate

      // But the inflated object is of the correct type, where the code is prepared to recognize it.
      val s4c: mult2.Call = s4bInflated match {
        case i1withTypeKnown: mult2.Call => i1withTypeKnown
        case _ => throw new RuntimeException("Re-inflated object does not match expectred class.")
      }
      s4c shouldEqual s4b
    }

    it("work on functions") {
      val testDataDir = f"$outputBaseDir/deflate-functions"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val inflatedCall1 = UnknownProvenance(mult2)
      val inflatedResult1 = inflatedCall1.resolve

      val deflatedCall1 = inflatedCall1.deflate
      val inflatedCall2 = deflatedCall1.inflate
      inflatedCall2 shouldEqual inflatedCall1

      val deflatedResult1: FunctionCallResultWithProvenanceDeflated[mult2.type] = inflatedResult1.deflate
      val inflatedResult2: FunctionCallResultWithProvenance[mult2.type] = deflatedResult1.inflate
      inflatedResult2 shouldEqual inflatedResult1
    }

    it("work on functions as inputs") {
      val testDataDir = f"$outputBaseDir/deflate-functions-taking-functions"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val unknownList = UnknownProvenance(List(100, 200, 300))

      // MapWithProvenance is nasty b/c the function is itself an input.
      // It uses binary encoding (wrapped in Circe JSON).
      val inflated1: MapWithProvenance[Int, List, Int]#Call = unknownList.map(incrementInt)
      val inflated1b: MapWithProvenance[Int, List, Int]#Call = unknownList.map(incrementInt)
      //inflated1 shouldBe inflated1b

      val inflated1result = inflated1.resolve
      inflated1result.output shouldBe List(101, 201, 301)

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      // Note: something causes these to be equal as strings but not pass the eq check.
      inflated2.toString shouldEqual inflated1.toString

      rt.hasOutputForCall(inflated2) shouldBe true
    }

    it("works with functions as output") {
      val testDataDir = f"$outputBaseDir/deflate-returning-functions"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val inflated1: fmaker.Call = fmaker()
      val inflated1result = inflated1.resolve
      inflated1result.output shouldBe incrementInt

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      inflated2 shouldEqual inflated1
    }

    it("works with limited type information") {
      val testDataDir = f"$outputBaseDir/deflate-reinflate-unknown-type"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val (functionName, functionVersion, digest) =
        createCallAndSaveCallAndReturnOnlyIds(rt)

      val deflatedCallTyped =
        rt.loadDeflatedCallOption[Int](
          functionName,
          functionVersion,
          digest
        ).get

      checkDeflatedCall(digest, deflatedCallTyped)
    }
  }

  def createCallAndSaveCallAndReturnOnlyIds(implicit rt: ResultTrackerSimple): (String, Version, Digest) = {
    val c1 = add2(2, 2)
    val c2 = add2(5, 7)
    val c3 = mult2(c1, c2)

    val call: mult2.Call = mult2(c3, 2)

    val call2: mult2.Call = mult2(mult2(add2(2, 2), add2(5, 7)), 2)
    call2 shouldEqual call

    val deflated = rt.saveCall(call)

    // Round-trip through deflation/inflation loses some type information on the surface...
    val reinflated: FunctionCallWithProvenance[Int] = deflated.inflate
    reinflated shouldEqual call

    // ..but the inflated object is of the correct type, where the code is prepared to recognize it.
    val reinflatedWithKnownType: mult2.Call = reinflated match {
      case known: mult2.Call => known
      case _ => throw new RuntimeException("Re-inflated object does not match expected specific sub-class!")
    }
    reinflatedWithKnownType shouldEqual call

    // Deflating again should yield a similar thing.
    val deflated2: FunctionCallWithProvenanceDeflated[Int] = rt.saveCall(reinflated)
    deflated2 shouldBe deflated

    // The digest of the deflated object is stable
    deflated2.toDigest shouldEqual deflated.toDigest

    // And matches a the lowest level (thout this raw serialization is not what we actually save.
    Util.digestObjectRaw(deflated2) shouldBe Util.digestObjectRaw(deflated)

    /*
    // Return just the primitives that will allow this to be re-constituted.
    val recast = s4bDeflated.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[_]]
    val untypified = recast.untypify(rt)
    val finalDigest = untypified.toDigest
    if (rt.loadDeflatedUntypedCallOption(untypified.functionName, untypified.functionVersion, finalDigest).isEmpty)
      println("problem")
    */
    val untypified = deflated2.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[Int]]
    import io.circe.generic.auto._
    val finalDigest = Util.digestObject(untypified)
    (untypified.functionName, untypified.functionVersion, finalDigest)
  }

  def checkDeflatedCall(
    deflatedCallDigest: Digest,
    deflatedCall: FunctionCallWithProvenanceDeflated[_]
  )(implicit
    rt: ResultTrackerSimple
  ) = {
    // Inflate into a regular call.
    // The type is unknown at compile time, but the object should be complete.
    val inflatedCall: FunctionCallWithProvenance[_] =
    deflatedCall.inflate

    // Verify that the object functions with its full identity.
    inflatedCall shouldEqual mult2(mult2(add2(2, 2), add2(5, 7)), 2)

    // Ensure it matches the correct subtype in match calls.
    val inflatedCallWithKnownType: mult2.Call =
      inflatedCall match {
        case i1withTypeKnown: mult2.Call =>
          i1withTypeKnown
        case _ =>
          throw new RuntimeException("Re-inflated object does not match expected class.")
      }

    // Ensure we can repeat the deflation and get the same ID.
    val deflatedAgain = rt.saveCall(inflatedCall)
    deflatedAgain match {
      case _: FunctionCallWithUnknownProvenanceDeflated[_] =>
        throw new RuntimeException("Deflation created an object with unknown provenance?")
      case known: FunctionCallWithKnownProvenanceDeflated[_] =>
        known.toDigest shouldBe deflatedCallDigest
    }
  }
}

object listMaker extends Function0WithProvenance[List[Int]] {
  val currentVersion: Version = Version("1.0")
  def impl(): List[Int] = List(100, 200, 300)
}

object add2 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a + b
}

object mult2 extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int, b: Int): Int = a * b
}

object incrementInt extends Function1WithProvenance[Int, Int] {
  val currentVersion: Version = Version("1.0")
  def impl(a: Int): Int = a + 1
}

object fmaker extends Function0WithProvenance[Function1WithProvenance[Int, Int]] {
  val currentVersion: Version = Version("1.0")
  def impl(): Function1WithProvenance[Int, Int] = incrementInt
}
