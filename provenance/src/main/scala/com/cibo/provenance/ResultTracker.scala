package com.cibo.provenance


import io.circe.{Decoder, Encoder}
import scala.reflect.ClassTag

/**
  * Created by ssmith on 5/16/17.
  *
  * A ResultTracker maps a FunctionCallWithProvenance to a FunctionCallResultWithProvenance, either by
  * executing the function and storing the result, or by finding previous results.
  *
  * It can be thought of as akin to a database handle, but with responsibility for decision making around data
  * generation, and tracking that generation in detail.
  *
  * It is usually supplied implicitly to methods on other objects that use the provenance library, giving them
  * a database-specific context.
  *
  * The primary entrypoint is the `.resolve(call)` method, which either gets or creates a result for a given call.
  * It also contains methods to otherwise interrogate the storage layer.
  *
  * A ResultTracker might determine the status of functions that are in the process of producing results, or have failed,
  * and decides if/how to move things into that state (run things) indirectly, though that is not part of the
  * base API currently.
  */
trait ResultTracker {

  /**
    * This is the core function of a ResultTracker.  It converts a call which may or may not have
    * a result into a result.
    *
    * First, it checks to see if there is already a result for the exact call
    * in question, and if so returns that result.  If not it checks to see if there has ever been
    * another call with the same inputs on the same version of code, and if so composes
    * a result with the known output, but with a new path through provenance tracking.  If
    * the inputs are entirely new at this version of the code, it actually runs the implementation
    * and saves everything.
    *
    * Note that, when it finds pre-existing outputs with different provenance, the new provenance
    * still creates a result that references the commit/build that actually made the data.  This
    * allows us to retroactively determine when there has been a versioning error in the commit
    * history and correct for it after the fact.
    *
    * @tparam O     The output type of the call.
    *
    * @param call:  A `FunctionCallWithProvenance[O]` to resolve (get or create a result)
    * @return       A `FunctionCallResultWithProvenance[O]` that wraps the call output.
    */
  def resolve[O](call: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    val callWithInputDigests = call.resolveInputs(rt=this)
    loadResultForCallOption[O](callWithInputDigests) match {
      case Some(existingResult) =>
        existingResult
      case None =>
        val newResult = callWithInputDigests.run(this)
        saveResult(newResult)
        newResult
    }
  }

  // abstract interface

  /**
    * The BuildInfo for the current running application.
    * This is used to construct new results for anything the tracker resolves.
    * It is typically supplied at construction time.
    *
    * @return
    */
  def currentAppBuildInfo: BuildInfo

  /**
    * Save one `FunctionCallWithProvenance[O]`, and return the "deflated" version of the call.
    * This happens automatically when a result is saved, but might happen earlier if the call is
    * to be transmitted to another process, or queued by a job distribution system.
    *
    * The deflated version uses IDs to reference its inputs (also deflated, recursively), and
    * as such is a light-weight pointer into the complete history of the call.
    *
    * @param call:  The call to save.
    * @tparam O:    The output type of the call and returned deflated call.
    * @return       The deflated call created during saving.
    */
  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O]

  /**
    * Save one `FunctionCallResultWithProvenance[O]`.  This is automatically called during resolve(),
    * but can be called independently when moving results made from one ResultTracker to another.
    *
    * @param result:  The result to save.
    * @tparam O       The output type of the result.
    * @return         The deflated result.
    */
  def saveResult[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O]

  /**
    * Save a regular input/output value to the storage fabric.
    *
    * @param obj:   The object to save.  It must have a circe Encoder and Decoder implicitly available.
    * @tparam T     The type of data to save.
    * @return       The Digest of the serialized data.  A unique ID usable to re-load later.
    */
  def saveValue[T : ClassTag : Encoder : Decoder](obj: T): Digest


  /**
    * Check storage for a result for a given call.
    *
    * @param call   The call to query for.
    * @tparam O     The type of the output of the call.
    * @return       A boolean flag that is true if a result exists.
    */
  def hasOutputForCall[O](call: FunctionCallWithProvenance[O]): Boolean

  /**
    * Check storage for a given input/output value by SHA1 Digest.
    *
    * @param obj      The object to check for.  It must have a circe encoder/decoder.  It will be digested pre-query.
    * @return         A boolean flag that is true if the value is stored.
    */
  def hasValue[T : ClassTag : Encoder : Decoder](obj: T): Boolean

  /**
    * Check storage for a given input/output value by SHA1 Digest.
    *
    * @param digest   The digest ID of the object to check for.
    * @return         A boolean flag that is true if the value is stored.
    */
  def hasValue(digest: Digest): Boolean

  /**
    * Load a result for a given call.
    *
    * @param call     The call to query for.
    * @tparam O       The outut type of the call.
    * @return         An Option[FunctionCallResultWithProvenance] that has Some if the result exists.
    */
  def loadResultForCallOption[O](call: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]]

  /**
    * Load a previously saved Call if its digest is known.  The call's digest is in used in its deflated surrogate,
    * and in deflated results.
    *
    * Note that, while the returned call is NOT deflated, its inputs are initially, so full reconstitution
    * of the history can happen iteratively.
    *
    * @param functionName       The class of FunctionWithProvenance for which the call is made.
    * @param functionVersion    The version of the function represented in the call.
    * @param digest             The digested value of the serialized call.
    * @tparam O                 The output type of the call.
    * @return                   A FunctionCallWithProvenance[O] that is NOT deflated, but still has deflated inputs.
    */
  def loadCallByDigestOption[O : ClassTag : Encoder : Decoder](
    functionName: String,
    functionVersion: Version,
    digest: Digest
  ): Option[FunctionCallWithProvenance[O]]

  /**
    * Load a previously saved _deflated_ Call if its digest is known.  The deflated call's digest is directly referenced
    * in the storage system since the possibility of inflation is uncertain.  The inflated call is only accessed
    * indirectly and conditionally.
    *
    * This version of this function expects to know the output type O and have implicit access to ClassTag[O],
    * Encoder[O] and Decoder[O].  Switch to the output-type-agnostic version where if this is uncertain.
    *
    * @param functionName       The class of FunctionWithProvenance for which the call is made.
    * @param functionVersion    The version of the function represented in the call.
    * @param digest             The digested value of the serialized call.
    * @return                   A FunctionCallWithProvenance[O] that is deflated, and also has deflated inputs.
    *
    * @tparam O                 The output type of the call.  
    */
  def loadCallByDigestDeflatedOption[O : ClassTag : Encoder : Decoder](
    functionName: String,
    functionVersion: Version,
    digest: Digest
  ): Option[FunctionCallWithProvenanceDeflated[O]]

  /**
    * Load a previously saved _deflated_ Call if its digest is known.  The deflated call's digest is directly referenced
    * in the storage system since the possibility of inflation is uncertain.  The inflated call is only accessed
    * indirectly and conditionally.
    *
    * This type-agnostic version of the loader loads the encoder/decoder information and determines the class tag
    * only via reflection.  It is not used within the API, but can be used by services to bridge between code
    * that is type-agnostic and code that is type-aware.
    *
    * @param functionName       The class of FunctionWithProvenance for which the call is made.
    * @param functionVersion    The version of the function represented in the call.
    * @param digest             The digested value of the serialized call.
    * @return                   A FunctionCallWithProvenance[O] that is deflated (which also has deflated inputs).
    *
    */
  def loadCallByDigestDeflatedUntypedOption(
    functionName: String,
    functionVersion: Version,
    digest: Digest
  ): Option[FunctionCallWithProvenanceDeflated[_]]

  /**
    * Retrieve one value from storage if the specified digest key is found.
    *
    * @param digest   The digest to query for.
    * @tparam T       The type of data.
    * @return         An Option of O that is Some[O] if the digest ID is found in storage.
    */
  def loadValueOption[T : ClassTag : Encoder : Decoder](digest: Digest): Option[T]

  /**
    * A type-agnostic version of loadValueSerializedDataOption.  This works with a class string as text.
    *
    * @param className    The name of the class to load.
    * @param digest       The digest ID of the value (query param).
    * @return             An optional array of serialized bytes.
    */
  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]]

  // Support methods

  /**
    * This method does part of the loadValue logic, but stops after retrieving the serialized bytes.
    *
    * @param digest   The digest ID of the value (query param).
    * @tparam O       The type of the output data.
    * @return         An optional array of serialized bytes.
    */
  def loadValueSerializedDataOption[O : ClassTag : Encoder : Decoder](digest: Digest): Option[Array[Byte]] = {
    val ct = implicitly[ClassTag[O]]
    loadValueSerializedDataOption(ct.runtimeClass.getName, digest)
  }

  /**
    * Retrieve one value from storage if the specified digest key is found.
    * Throws an exception if not found.
    *
    * @param digest   The digest to query for.
    * @tparam T       The type of data.
    * @return         An Option of T that is Some[T] if the digest ID is found in storage.
    */
  def loadValue[T : ClassTag : Encoder : Decoder](digest: Digest): T =
    loadValueOption[T](digest) match {
      case Some(obj) =>
        obj
      case None =>
        val ct = implicitly[ClassTag[T]]
        throw new NoSuchElementException(f"Failed to find content for $ct with ID $digest!")
    }

  /**
    * An UnresolvedVersionException is thrown if the system attempts to deflate a call with an unresolved version.
    * The version is typically static, but if it is not, the call cannot be saved until it is resolved.
    * @param call:  The offending call
    * @tparam O     The output type
    */
  case class UnresolvedVersionException[O](call: FunctionCallWithProvenance[O])
    extends RuntimeException(f"Cannot deflate calls with an unresolved version: $call")

  /**
    * A known failure mode for the ResultTracker.
    */
  class FailedSaveException(msg: String) extends RuntimeException(msg)
}


