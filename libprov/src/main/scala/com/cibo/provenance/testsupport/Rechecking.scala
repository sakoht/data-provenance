package com.cibo.provenance.testsupport

import com.cibo.provenance.{FunctionCallResultWithKnownProvenanceSerializable, FunctionCallResultWithProvenance, FunctionCallWithProvenance, ResultTracker}

import scala.concurrent.{ExecutionContext, Future}

/**
  * When this trait is applied to a ResultTracker, every attempt to resolve a call actually runs,
  * rather than just returning the previous result.  If there is a conflict between the expected
  * and new answers, an exception is thrown.
  */
trait Rechecking extends ResultTracker {

  /**
    * The overridden resolve() produces a new result every time, and diffs it vs. any existing result.
    * An exception is thrown if there is a conflict.
    *
    * @tparam O     The output type of the call.
    *
    * @param call:  A `FunctionCallWithProvenance[O]` to resolve (get or create a result)
    * @return       A `FunctionCallResultWithProvenance[O]` that wraps the call output.
    */
  override def resolve[O](call: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    val callWithInputDigests = call.resolveInputs(rt=this)
    val existingResultOption = loadResultByCallOption[O](callWithInputDigests)
    val newResult: FunctionCallResultWithProvenance[O] = callWithInputDigests.run(this)
    resolveInner(existingResultOption, newResult)
  }

  /**
    * The overridden resolveAsync() produces a new result every time, and diffs it vs. any existing result.
    * An exception is thrown if there is a conflict.
    *
    * @tparam O     The output type of the call.
    *
    * @param call:  A `FunctionCallWithProvenance[O]` to resolve (get or create a result)
    * @return       A `FunctionCallResultWithProvenance[O]` that wraps the call output.
    */
  override def resolveAsync[O](call: FunctionCallWithProvenance[O])(implicit ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]] = {
    implicit val rt: ResultTracker = this
    for {
      callWithInputDigests <- call.resolveInputsAsync
      existingResultOption <- loadResultByCallOptionAsync[O](callWithInputDigests)
    } yield {
      val newResult: FunctionCallResultWithProvenance[O] = callWithInputDigests.run(this)
      resolveInner(existingResultOption, newResult)
    }
  }

  private def resolveInner[O](
    existingResultOption: Option[FunctionCallResultWithProvenance[O]],
    newResult: FunctionCallResultWithProvenance[O]
  ): FunctionCallResultWithProvenance[O] = {
    existingResultOption match {
      case Some(existingResult) =>
        // On multiple runs of a test case, there will be previously existing result.
        val existingDigest = existingResult.outputAsVirtualValue.digestOption.get
        val newDigest = newResult.outputAsVirtualValue.resolveDigest.digestOption.get

        if (newDigest == existingDigest) {
          logger.info(f"Call matches previous result: ${newResult.call}")
          existingResult
        } else {
          logger.error(f"Call returns conflicting result!: ${newResult.call}\nOLD: $existingResult\nNEW: $newResult")
          if (saveConflicts) {
            logger.warn(f"Saving conflicted results to storage...")
            FunctionCallResultWithKnownProvenanceSerializable.save(newResult)(this)
          }
          throw new com.cibo.provenance.exceptions.InconsistentVersionException(
            newResult.call.functionName,
            newResult.call.versionValueAlreadyResolved.get,
            Seq(existingResult.outputBuildInfo.commitId, newResult.outputBuildInfo.commitId).distinct,
            Some(newResult.call.getInputGroupValuesDigest(this))
          )
        }
      case None =>
        // This should happen on the first run of a test case.
        logger.warn(f"No comparison result found!  Generated: $newResult")
        FunctionCallResultWithKnownProvenanceSerializable.save(newResult)(this)
        newResult
    }
  }

  /**
    * By default conflicts are saved to the data fabric.  This is appropriate in production when we need
    * to flag a particular version of a function as tainted.
    *
    * During testing, the saved conflict is typically discarded after the version number is bumped or the code is fixed.
    */
  val saveConflicts: Boolean = true
}
