package com.cibo.provenance


import com.typesafe.scalalogging.Logger
import io.circe.{Decoder, Encoder}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

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
trait ResultTracker extends Serializable {

  @transient
  lazy val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(getClass.getName))

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
        val newResult: FunctionCallResultWithProvenance[O] = callWithInputDigests.run(this)
        FunctionCallResultWithKnownProvenanceSerializable.save(newResult)(this)
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
    * @param callInSavableForm:  The call to save.
    * @tparam O:    The output type of the call and returned deflated call.
    * @return       The deflated call created during saving.
    */
  def saveCall[O](callInSavableForm: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceSaved[O]
  
  /**
    * Write out one `FunctionCallResultWithProvenanceSaved`.  
    * 
    * This is directly called by the constructor of the *Saved object when called with an unsaved result.
    * 
    * This is automatically called during resolve(), but can be called independently when 
    * moving results made from one ResultTracker to another.
    *
    * @param resultInSavableForm:   The result to save after conversion to savable form.
    * @param inputResults:          The inputs, which must themselves be results at this point.
    * @return                       The deflated result.
    */
  def saveResult(
    resultInSavableForm: FunctionCallResultWithKnownProvenanceSerializable,
    inputResults: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceSaved[_]

  /**
    * Save a regular input/output value to the storage fabric.
    *
    * This is used by the 
    *
    * @param obj:   The object to save.  It must have a circe Encoder and Decoder implicitly available.
    * @tparam T     The type of data to save.
    * @return       The Digest of the serialized data.  A unique ID usable to re-load later.
    */
  def saveOutputValue[T : ClassTag : Encoder : Decoder](obj: T): Digest
  
  def saveBuildInfo: Digest

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
    * Load the saved representation of a Call if its digest is known.
    *
    * When this is initially saved, its digest is used in a wrapper call that does not have inputs,
    * deflated untyped results and in the input descriptions of calls that reference back to it.
    *
    * @param functionName       The class of FunctionWithProvenance for which the call is made.
    * @param functionVersion    The version of the function represented in the call.
    * @param digest             The digested value of the serialized call.
    * @return                   A FunctionCallWithProvenance[O] that is NOT deflated, but still has deflated inputs.
    */
  def loadCallMetaByDigest(
    functionName: String,
    functionVersion: Version,
    digest: Digest
  ): Option[FunctionCallWithKnownProvenanceSerializableWithInputs]

  /**
    * Retrieve one value from storage if the specified digest key is found.
    *
    * @param digest   The digest to query for.
    * @tparam T       The type of data.
    * @return         An Option of O that is Some[O] if the digest ID is found in storage.
    */
  def loadValueOption[T : ClassTag : Codec](digest: Digest): Option[T]

  def loadValueOption(className: String, digest: Digest): Option[_] = {
    val clazz = Class.forName(className)
    loadValueOption(clazz, digest)
  }

  protected def loadValueOption[T](clazz: Class[T], digest: Digest): Option[_] = {
    implicit val ct: ClassTag[T] = ClassTag(clazz)
    Try {
      val (value, codec) = loadValueWithCodec[T](digest)
      value
    } match {
      case Success(codec) =>
        Some(codec)
      case Failure(err) =>
        logger.error(f"Failed ot load codec: $err")
        None
    }
  }

  def loadValueWithCodec[T : ClassTag](valueDigest: Digest): (T, Codec[T]) = {
    val stream = loadCodecStreamByValueDigest[T](valueDigest: Digest).flatMap {
      codec =>
        implicit val c: Codec[T] = codec
        val foundAndLoadable: Option[T] =
          Try {
            loadValueOption[T](valueDigest) match {
              case Some(value) =>
                value
              case None =>
                throw new RuntimeException(s"Failed to find value of type ${implicitly[ClassTag[T]]} for digest $valueDigest")
            }
          } match {
            case Success(value) =>
              Some(value)
            case Failure(err) =>
              logger.error(err.toString)
              None
          }
        foundAndLoadable.map { value => (value, codec)}
    }
    stream.headOption match {
      case Some(pair) =>
        pair
      case None =>
        throw new RuntimeException(f"Failed to find codec for value digest $valueDigest for type ${implicitly[ClassTag[T]]}")
    }
  }

  def loadCodecByType[T : ClassTag]: Codec[T]

  def loadCodecByCodecDigest[T : ClassTag](codecDigest: Digest): Codec[T] = {
    val valueClassName = Util.classToName[T]
    loadCodecByClassNameAndCodecDigest[T](valueClassName, codecDigest)
  }

  def loadCodecByClassNameAndCodecDigest[T : ClassTag](valueClassName: String, codecDigest: Digest): Codec[T]

  def loadCodecStreamByValueDigest[T : ClassTag](valueDigest: Digest): Seq[Codec[T]]

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo]


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


