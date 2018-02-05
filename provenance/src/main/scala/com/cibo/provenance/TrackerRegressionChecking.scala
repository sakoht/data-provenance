package com.cibo.provenance

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

/**
  * Apply this trait to a ResultTracker used in test cases to check for regression.
  *
  * It will:
  *   - When any call resolves, it will still re-run the code and verify that output matches any saved data.
  *   - Test each saved object by re-loading it to ensure that the serialization will round-trip.
  *
  * Example Test Case:
  *
  *   implicit val bi: BuildInfo = DummyBuildInfo
  *
  *   implicit val rt: ResultTracker =
  *     new ResultTrackerSimple("src/test/resources/rt1") with TrackerRegressionChecking
  *
  *   describe("MyFunc") {
  *
  *     List[FunctionCallWithProvenance](
  *
  *       MyFunc("foo", 111),
  *       MyFunc("bar", 222),
  *       MyFunc("baz", 333)
  *
  *     ).foreach {
  *       call =>
  *         it("$call resolves as expected...") {
  *           call.resolve
  *         }
  *     }
  *   }
  *
  */

trait TrackerRegressionChecking extends ResultTracker with LazyLogging {

  /*
   * Return a FunctionCallResultWithProvenance for a FunctionCallWithProvenance.
   *
   * If a result already exists for the current inputs at this version, it would normally be returned without
   * running.  For the test tracker, however, the logic ALWAYS runs.  In the default case, the result will be
   * identical to any previous result already saved.  When it is not, the data fabric will record the conflict.
   *
   */
  override def resolve[O](f: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    implicit val ct: ClassTag[O] = f.getOutputClassTag

    val callWithInputDigests = f.resolveInputs(rt=this)

    val newResult: FunctionCallResultWithProvenance[O] = callWithInputDigests.run(this)

    // Warn if we are going to save an inconsistent result.
    // This lets us stop in the debugger before saving tainted data to the test fabric.
    loadResultForCallOption[O](callWithInputDigests) match {
      case Some(existingResult) =>
        if (existingResult.output(this) != newResult.output(this)) {
          logger.warn("Saving inconsistent result!")
        }
      case None =>
    }

    // Save the new result.  If it has conflicts this will taint the data fabric for this version.
    saveResult(newResult)

    // Now sanity-check the reload process.
    loadResultForCallOption[O](callWithInputDigests) match {
      case Some(newResultCopy2) =>
        val output1 = newResult.output(this)
        val output2 = newResultCopy2.output(this)
        if (output1 != output2)
          throw new RuntimeException(f"New result output reloads and does not test as equal!: $output1, $output2")

        val digest1 = Util.digestObject(output1)
        val digest2 = Util.digestObject(output2)
        if (digest1 != digest2) {
          throw new RuntimeException(f"New result has inconsistent digests: $digest1, $digest2 for $output1, $output2")
        } else {
          newResult
        }
      case None =>
        throw new RuntimeException(f"Failed to re-load saved result $newResult!")
    }
  }
}
