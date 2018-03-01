package com.cibo.provenance

import java.io.File

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.monadics.MapWithProvenance
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}


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
      val i2 = d1.inflate.inflateInputs
      i2 shouldEqual i1
    }

    it("allows a call to partially or fully inflate") {
      val testDataDir = f"$outputBaseDir/deflate-reinflate"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val inflated1: mult2.Call = mult2(2, 2)
      val _ = inflated1.resolve

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      val deflated1known = deflated1.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[Int]]
      val inflated3opt = rt.loadCallByDigestOption[Int](
        deflated1known.functionName,
        deflated1known.functionVersion,
        deflated1known.inflatedCallDigest
      )
      val inflated3 = inflated3opt.get
      inflated3 shouldEqual inflated2
      inflated3.inflateInputs shouldEqual inflated2.inflateInputs
    }

    it("works on nested calls") {
      val testDataDir = f"$outputBaseDir/deflate-nested"
      FileUtils.deleteDirectory(new File(testDataDir))

      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val c1 = add2(2, 2)
      val c2 = add2(5, 7)
      val c3 = mult2(c1, c2)

      val call: mult2.Call = mult2(c3, 2)

      val call2: mult2.Call = mult2(mult2(add2(2,2), add2(5,7)), 2)
      call2 shouldEqual call.inflateRecurse

      // Round-trip through deflation/inflation loses some type information.
      val deflated: FunctionCallWithProvenanceDeflated[Int] = rt.saveCall(call)
      val reinflated: FunctionCallWithProvenance[Int] = deflated.inflate

      // But the inflated object is of the correct type, where the code is prepared to recognize it.
      val reinflatedWithTypeKnown: mult2.Call = reinflated match {
        case known: mult2.Call => known
        case _ => throw new RuntimeException("Re-inflated object does not match expectred class.")
      }

      val reinflatedDeeply = reinflatedWithTypeKnown.inflateRecurse
      reinflatedDeeply shouldEqual call2
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
      Util.digestObjectRaw(inflated1) shouldEqual Util.digestObjectRaw(inflated1b)

      val inflated1result = inflated1.resolve
      inflated1result.output shouldBe List(101, 201, 301)

      val deflated1 = inflated1.deflate
      val inflated2 = deflated1.inflate

      // Note: something causes these to be equal as strings but not pass the eq check.
      inflated2.unresolve.toString shouldEqual inflated1.unresolve.toString

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
      val testDataDir = f"$outputBaseDir/deflate-reinflate-limited-type"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val (functionName, functionVersion, digest) =
        createCallAndSaveCallAndReturnOnlyIds(rt)

      // For this test, the type Int is specified.
      val deflatedCallTyped: FunctionCallWithProvenanceDeflated[Int] =
        rt.loadCallByDigestDeflatedOption[Int](
          functionName,
          functionVersion,
          digest
        ).get

      checkDeflatedCall(digest, deflatedCallTyped)
    }

    it("works with no type information") {
      val testDataDir = f"$outputBaseDir/deflate-reinflate-unknown-type"
      FileUtils.deleteDirectory(new File(testDataDir))
      implicit val bi: BuildInfo = DummyBuildInfo
      implicit val rt: ResultTrackerSimple = ResultTrackerSimple(SyncablePath(testDataDir))

      val (functionName, functionVersion, digest) =
        createCallAndSaveCallAndReturnOnlyIds(rt)

      // For this test, the type int is NOT specified.
      val deflatedCallUntyped: FunctionCallWithProvenanceDeflated[_] =
        rt.loadCallByDigestDeflatedUntypedOption(
          functionName,
          functionVersion,
          digest
        ).get

      checkDeflatedCall(digest, deflatedCallUntyped)
    }
  }

  def createCallAndSaveCallAndReturnOnlyIds(implicit rt: ResultTrackerSimple): (String, Version, Digest) = {
    val c1 = add2(2, 2)
    val c2 = add2(5, 7)
    val c3 = mult2(c1, c2)

    val call1: mult2.Call = mult2(c3, 2)

    val call2: mult2.Call = mult2(mult2(add2(2, 2), add2(5, 7)), 2)
    call2 shouldEqual call1

    val call = call1

    val d1 = Util.digestObjectRaw(call)
    call.getEncoder
    call.getDecoder
    call.getOutputClassTag
    val d2 = Util.digestObjectRaw(call)

    val deflated = rt.saveCall(call)
    val deflatedDigest = deflated.toDigest
    
    // Note: If there are any lazy vals the second and subsequent saves could be different.
    val deflated2 = rt.saveCall(call)
    //deflated2 shouldEqual deflated
    deflated2.toDigest shouldEqual deflatedDigest

    // Round-trip through deflation/inflation loses some type information on the surface...
    val reinflated: FunctionCallWithProvenance[Int] = deflated.inflate

    // ..but the inflated object is of the correct type, where the code is prepared to recognize it.
    val reinflatedWithKnownType: mult2.Call =
      reinflated match {
        case known: mult2.Call =>
          known
        case other =>
          throw new RuntimeException(f"Re-inflated object does not match expected specific sub-class! $other")
      }

    // Ensure that the reinflated object has the exact same digest still.
    Util.digestObjectRaw(reinflatedWithKnownType) shouldEqual
      deflated.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[_]].inflatedCallDigest

    // Re-inflation should be the same regardless of the level of type awareness.
    val reinflatedDeeply1 = reinflated.inflateRecurse
    reinflatedDeeply1 shouldEqual call
    val reinflatedDeeply2 = reinflatedWithKnownType.inflateRecurse
    reinflatedDeeply2 shouldEqual call

    // Deflating again should yield a similar thing.
    //val deflated3 = rt.saveCall(reinflatedDeeply2)
    //deflated3 shouldEqual deflated
    //deflated3.toDigest shouldEqual deflatedDigest

    // Setting the type should not affect
    val deflated4 = deflated.asInstanceOf[FunctionCallWithKnownProvenanceDeflated[Int]]
    deflated4 shouldEqual deflated
    deflated4.toDigest shouldEqual deflatedDigest

    (deflated4.functionName, deflated4.functionVersion, deflatedDigest)
  }

  def checkDeflatedCall(
    deflatedCallDigest: Digest,
    deflatedCall: FunctionCallWithProvenanceDeflated[_]
  )(implicit
    rt: ResultTrackerSimple
  ): Unit = {
    // Inflate into a regular call.
    // The type is unknown at compile time, but the object should be complete.
    val inflatedCall: FunctionCallWithProvenance[_] =
    deflatedCall.inflate

    // Verify that the object functions with its full identity.
    val inflatedDeeply = inflatedCall.inflateRecurse
    inflatedDeeply shouldEqual mult2(mult2(add2(2, 2), add2(5, 7)), 2)

    // Ensure it matches the correct subtype in match calls.
    inflatedCall match {
      case known: mult2.Call =>
        known
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

object theInt extends Function0WithProvenance[Int] {
  val currentVersion: Version = Version("1.0")
  def impl(): Int = 123
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
