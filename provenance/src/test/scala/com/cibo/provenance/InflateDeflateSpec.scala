package com.cibo.provenance

import com.cibo.io.s3.SyncablePath
import com.cibo.provenance.monadics.MapWithProvenance
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by ssmith on 10/26/17.
  */


class InflateDeflateSpec extends FunSpec with Matchers {
  val outputBaseDir: String = TestUtils.testOutputBaseDir

  describe("Deflation and inflation") {

    it("work on simple calls.") {
      val testDataDir = f"$outputBaseDir/save-simple"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val call1: mult2.Call = mult2(2, 2)
      val _ = call1.resolve

      val saved1 = call1.save
      val call2 = saved1.load
      call2 shouldEqual call1
    }

    it("allows a call to partially or fully load") {
      val testDataDir = f"$outputBaseDir/save-reload"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val call1: mult2.Call = mult2(2, 2)
      val _ = call1.resolve

      val deflated1 = call1.save
      val call2 = deflated1.load

      val deflated2opt = rt.loadCallById(deflated1.id)
      val call3 = deflated2opt.get.load

      call3 shouldEqual call2
      call3.load.loadInputs shouldEqual call2.loadInputs
    }

    it("works on nested calls") {
      val testDataDir = f"$outputBaseDir/save-nested"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val c1 = add2(2, 2)
      val c2 = add2(5, 7)
      val c3 = mult2(c1, c2)

      val call: mult2.Call = mult2(c3, 2)

      val call2: mult2.Call = mult2(mult2(add2(2,2), add2(5,7)), 2)
      call2 shouldEqual call.loadRecurse

      // Round-trip through deflation/inflation loses some type information.
      val deflated: FunctionCallWithProvenanceDeflated[Int] = call.save
      val reloaded: FunctionCallWithProvenance[Int] = deflated.load

      // But the loaded object is of the correct type, where the code is prepared to recognize it.
      val reloadedWithTypeKnown: mult2.Call = reloaded match {
        case known: mult2.Call => known
        case _ => throw new RuntimeException("Re-loaded object does not match expectred class.")
      }

      val reloadedDeeply: mult2.Call = reloadedWithTypeKnown.loadRecurse
      reloadedDeeply shouldEqual call2
    }

    it("work on functions") {
      val testDataDir = f"$outputBaseDir/save-functions"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val u1: UnknownProvenance[mult2.type] = UnknownProvenance(mult2)
      val r1: FunctionCallResultWithProvenance[mult2.type] = u1.resolve

      val savedCall1 = u1.save
      val loadedCall2 = savedCall1.load
      loadedCall2 shouldEqual u1

      val savedResult1: FunctionCallResultWithProvenanceDeflated[_] = r1.save
      val savedResult1b = savedResult1.asInstanceOf[FunctionCallResultWithProvenanceDeflated[mult2.type]]
      val loadedResult2: FunctionCallResultWithProvenance[_] = savedResult1.load
      loadedResult2 shouldEqual r1
    }

    it("work on functions as inputs") {
      val testDataDir = f"$outputBaseDir/save-functions-taking-functions"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val unknownList = UnknownProvenance(List(100, 200, 300))

      // MapWithProvenance is challenging b/c the function is itself an input,
      // and, further, it has type parameters.
      val inc1 = unknownList.map(incrementInt)

      // Ensure it can translate its entire type signature into text.
      inc1.functionName shouldEqual
        "com.cibo.provenance.monadics.MapWithProvenance[scala.collection.immutable.List,scala.Int,scala.Int]"

      // Save the call directly.  This usually happens "under the hood" when resolving.
      val saved1 = inc1.save
      saved1.data match {
        case data: FunctionCallWithKnownProvenanceSerializableWithInputs =>
        case other => throw new RuntimeException("")
      }

      // Creating a duplicate should create an identical blob of saved call data.
      val inc2 = unknownList.map(incrementInt)
      val saved2 = inc2.save
      saved2 shouldEqual saved1

      // Reload the saved call.
      val inc3 = saved1.load

      // Nothing has run.
      rt.hasOutputForCall(inc1) shouldBe false
      rt.hasOutputForCall(inc2) shouldBe false
      rt.hasOutputForCall(inc3) shouldBe false

      // Resolve.
      val result1 = inc1.resolve
      result1.output shouldBe List(101, 201, 301)

      // All instances know they have a result.
      rt.hasOutputForCall(inc1) shouldBe true
      rt.hasOutputForCall(inc2) shouldBe true
      rt.hasOutputForCall(inc3) shouldBe true

      // These are no-ops.
      val result2 = inc2.resolve
      result2.output == result1.output
      val result3 = inc3.resolve
      result3.output == result1.output

      // Note: something causes these to be equal as strings but not pass the eq check.
      inc3.unresolve.toString shouldEqual inc1.unresolve.toString
    }

    it("works with functions as output") {
      val testDataDir = f"$outputBaseDir/save-returning-functions"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val loaded1: fmaker.Call = fmaker()
      val loaded1result = loaded1.resolve
      loaded1result.output shouldBe incrementInt

      val saved1 = loaded1.save
      val loaded2 = saved1.load

      loaded2 shouldEqual loaded1
    }

    it("works with no type information") {
      val testDataDir = f"$outputBaseDir/save-reload-unknown-type"
      implicit val bi: BuildInfo = BuildInfoDummy
      implicit val rt = new ResultTrackerSimple(SyncablePath(testDataDir)) with TestTracking
      rt.wipe

      val digest = createCallAndSaveCallAndReturnOnlyIds(rt)

      // For this test, the type int is NOT specified.
      val savedCallUntyped: FunctionCallWithProvenanceDeflated[_] =
        rt.loadCallById(digest).get

      checkDeflatedCall(digest, savedCallUntyped)
    }
  }

  def createCallAndSaveCallAndReturnOnlyIds(implicit rt: ResultTrackerSimple): Digest = {
    val c1 = add2(2, 2)
    val c2 = add2(5, 7)
    val c3 = mult2(c1, c2)

    val call1: mult2.Call = mult2(c3, 2)

    val call2: mult2.Call = mult2(mult2(add2(2, 2), add2(5, 7)), 2)
    call2 shouldEqual call1

    val saved: FunctionCallWithProvenanceDeflated[Int] = call2.save

    // Repeat saves return only IDs.
    val saved2 = call2.save
    saved2 shouldEqual saved
    saved2.id shouldEqual saved.id

    val call3: FunctionCallWithProvenance[Int] = saved.load

    // ..but the loaded object is of the correct type, where the code is prepared to recognize it.
    call3 match {
      case _: mult2.Call =>
        // good
      case other =>
        throw new RuntimeException(f"Re-loaded object does not match expected specific sub-class! $other")
    }

    // Ensure that the reloaded object has the exact same digest still.
    call3 shouldEqual call2

    // Re-inflation should be the same regardless of the level of type awareness.
    val reloadedDeeply1 = call3.loadRecurse
    reloadedDeeply1 shouldEqual call2

    val reloadedDeeply2 = call3.loadRecurse
    reloadedDeeply2 shouldEqual call2

    // Deflating again should yield a similar thing.
    val saved3 = reloadedDeeply2.save
    saved3 shouldEqual saved

    saved3.id
  }

  def checkDeflatedCall(
    savedCallDigest: Digest,
    savedCall: FunctionCallWithProvenanceDeflated[_]
  )(implicit
    rt: ResultTrackerSimple
  ): Unit = {
    // Inflate into a regular call.
    // The type is unknown at compile time, but the object should be complete.
    val loadedCall: FunctionCallWithProvenance[_] =
    savedCall.load

    // Verify that the object functions with its full identity.
    val loadedDeeply = loadedCall.loadRecurse
    loadedDeeply shouldEqual mult2(mult2(add2(2, 2), add2(5, 7)), 2)

    // Ensure it matches the correct subtype in match calls.
    loadedCall match {
      case known: mult2.Call =>
        known
      case _ =>
        throw new RuntimeException("Re-loaded object does not match expected class.")
    }

    // Ensure we can repeat the deflation and get the same ID.
    val savedAgain = loadedCall.save
    savedAgain.data match {
      case _: FunctionCallWithUnknownProvenanceSerializable =>
        throw new RuntimeException("Deflation created an object with unknown provenance?")
      case known: FunctionCallWithKnownProvenanceSerializable =>
        known.toDigest shouldBe savedCallDigest
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
