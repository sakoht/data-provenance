package com.cibo.provenance

import scala.concurrent.{ExecutionContext, Future}


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
  import org.slf4j.LoggerFactory

  import scala.reflect.ClassTag
  import scala.reflect.runtime.universe.TypeTag
  import scala.util.{Failure, Success, Try}

  @transient
  lazy val logger: org.slf4j.Logger = LoggerFactory.getLogger(getClass.getName)

  /**
    * The .resolve method is the core function of a ResultTracker.
    * It converts a call which may or may not have a result into a result.
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
    loadResultByCallOption[O](callWithInputDigests) match {
      case Some(existingResult) =>
        existingResult
      case None =>
        val newResult: FunctionCallResultWithProvenance[O] = callWithInputDigests.run(this)
        FunctionCallResultWithKnownProvenanceSerializable.save(newResult)(this)
        newResult
    }
  }

  def resolveAsync[O](call: FunctionCallWithProvenance[O])(implicit ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]] = {
    implicit val rt: ResultTracker = this
    for {
      callWithInputDigests <- call.resolveInputsAsync
      resultOption <- loadResultByCallOptionAsync[O](callWithInputDigests)
    }
      yield resultOption match {
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
    * A query interface to load any saved by its ID (digest)
    * .
    * param callId   The digest of the serialized call as referenced elsewhere in the system.
    * return         Some FunctionCallWithProvenance[_] if it exists.
    */
  def  loadCallById(callId: Digest): Option[FunctionCallWithProvenanceDeflated[_]]

  /**
    *
    * @param resultId The digest of the serialized call as referenced elsewhere in the system.
    * @return         Some FunctionCallResultWithProvenance[_] if it exists.
    */
  def loadResultById(resultId: Digest): Option[FunctionCallResultWithProvenanceDeflated[_]]


  /**
    * The BuildInfo for the current running application.
    * This is used to construct new results for anything the tracker resolves.
    * It is typically supplied at construction time.
    *
    * @return Returns the buildInfo
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
    * @param callInSerializableForm:  The call to save.
    * @tparam O:    The output type of the call and returned deflated call.
    * @return       The deflated call created during saving.
    */
  def saveCallSerializable[O](callInSerializableForm: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceDeflated[O]

  /**
    * Write out one `FunctionCallResultWithProvenanceSaved`.
    *
    * This is directly called by the constructor of the *Saved object when called with an unsaved result.
    *
    * This is automatically called during resolve(), but can be called independently when
    * moving results made from one ResultTracker to another.
    *
    * @param resultInSerializableForm:   The result to save after conversion to savable form.
    * @param inputResults:          The inputs, which must themselves be results at this point.
    * @return                       The deflated result.
    */
  def saveResultSerializable(
    resultInSerializableForm: FunctionCallResultWithKnownProvenanceSerializable,
    inputResults: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceDeflated[_]

  /**
    * Save a regular input/output value to the storage fabric.
    *
    * This is used by the
    *
    * @param obj:   The object to save.  It must have a circe Codec implicitly available.
    * @tparam T     The type of data to save.
    * @return       The Digest of the serialized data.  A unique ID usable to re-load later.
    */
  def saveOutputValue[T : Codec](obj: T)(implicit cdcd: Codec[Codec[T]]): Digest

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
  def hasValue[T : Codec](obj: T): Boolean

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
  def loadResultByCallOption[O](call: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]]

  def loadResultByCallOptionAsync[O](call: FunctionCallWithProvenance[O])(implicit ec: ExecutionContext): Future[Option[FunctionCallResultWithProvenance[O]]]

  /**
    * Retrieve one value from storage if the specified digest key is found.
    *
    * @param digest   The digest to query for.
    * @tparam T       The type of data.
    * @return         An Option of O that is Some[O] if the digest ID is found in storage.
    */
  def loadValueOption[T : Codec](digest: Digest): Option[T]

  def loadValueOption(className: String, digest: Digest): Option[_] = {
    val clazz = Class.forName(className)
    loadValueOption(clazz, digest)
  }

  protected def loadValueOption[T](clazz: Class[T], digest: Digest)(implicit cdcd: Codec[Codec[T]]): Option[_] = {
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

  //def loadValueWithCodec[T : ClassTag](valueDigest: Digest)(implicit cdcd: Codec[Codec[T]]): (T, Codec[T]) = {
  def loadValueWithCodec[T : ClassTag](valueDigest: Digest)(implicit cct: Codec[Codec[T]]): (T, Codec[T]) = {
    val allCodecs: List[Codec[T]] = loadCodecsByValueDigest[T](valueDigest: Digest).toList
    val workingCodecValuePairs = allCodecs.flatMap {
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
    workingCodecValuePairs.headOption match {
      case Some(pair) =>
        pair
      case None => throw new RuntimeException(f"Failed to find codec for value digest $valueDigest for type ${implicitly[ClassTag[T]]}")
    }
  }

  def loadCodecByType[T : ClassTag : TypeTag](implicit cdcd: Codec[Codec[T]]): Codec[T]

  def loadCodecByCodecDigest[T : ClassTag](codecDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Codec[T] = {
    val valueClassName = Codec.classTagToSerializableName[T]
    loadCodecByClassNameAndCodecDigest[T](valueClassName, codecDigest)
  }

  def loadCodecByClassNameAndCodecDigest[T : ClassTag](valueClassName: String, codecDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Codec[T]

  def loadCodecsByValueDigest[T : ClassTag](valueDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Seq[Codec[T]]

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo]


  /**
    * A type-agnostic version of loadValueSerializedDataOption.  This works with a class string as text.
    *
    * @param className    The name of the class to load.
    * @param digest       The digest ID of the value (query param).
    * @return             An optional array of serialized bytes.
    */
  def loadValueSerializedDataByClassNameAndDigestOption(className: String, digest: Digest): Option[Array[Byte]]

  // Support methods

  /**
    * This method does part of the loadValue logic, but stops after retrieving the serialized bytes.
    *
    * @param digest   The digest ID of the value (query param).
    * @tparam O       The type of the output data.
    * @return         An optional array of serialized bytes.
    */
  def loadValueSerializedDataOption[O : Codec](digest: Digest): Option[Array[Byte]] =
    loadValueSerializedDataByClassNameAndDigestOption(
      Codec.classTagToSerializableName[O](
        implicitly[Codec[O]].classTag
      ),
      digest
    )

  /**
    * Retrieve one value from storage if the specified digest key is found.
    * Throws an exception if not found.
    *
    * @param digest   The digest to query for.
    * @tparam T       The type of data.
    * @return         An Option of T that is Some[T] if the digest ID is found in storage.
    */
  def loadValue[T : Codec](digest: Digest): T = {
    //val ct = implicitly[ClassTag[T]]
    //val className = Util.classToName[T]
    //val clazz = Class.forName(className)
    loadValueOption[T](digest) match {
      case Some(obj) =>
        obj
      case None =>
        val ct = implicitly[Codec[T]].classTag
        throw new NoSuchElementException(f"Failed to find content for $ct with ID $digest!")
    }
  }

  /**
    * Query Interface
    *
    * The low-level interface on the ResultTracker returns data even if the types are not
    * fully instantiatable in the current application.
    *
    * To fully vivify an object, call .load to get a fully typified object.
    */

  // Functions

  /**
    * Return the names of all functions with results tracked in this tracker.
    *
    * @return  The fully-qualified names of the functions.
    */
  def findFunctionNames: Iterable[String]

  /**
    * Return all versions of a given function with calls or results in storage.
    *
    * @param functionName
    * @return
    */
  def findFunctionVersions(functionName: String): Iterable[Version]

  // Calls

  /**
    * Return all calls for which there is data in storage.
    *
    * @return An iterable of calls.
    */
  def findCallData: Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs]

  /**
    * Return all calls for which there is data in storage for a given function name.
    *
    * @return An iterable of calls.
    */
  def findCallData(functionName: String): Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs]

  /**
    * Return all calls for which there is data in storage for a given function name and verison number.
    *
    * @return An iterable of calls.
    */
  def findCallData(functionName: String, version: Version): Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs]


  // Results

  /**
    * Return all results for which there is data in storage.
    *
    * @return An iterable of results.
    */
  def findResultData: Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  /**
    * Return all calls for which there is data in storage for a given function name.
    *
    * @return An iterable of results.
    */
  def findResultData(functionName: String): Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  /**
    * Return all calls for which there is data in storage for a given function name and version.
    *
    * @return An iterable of results.
    */
  def findResultData(functionName: String, version: Version): Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  // Results by output

  def findResultDataByOutput(outputDigest: Digest): Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  def findResultDataByOutput[O : Codec](output: O): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = {
    val codec = implicitly[Codec[O]]
    val bytes = codec.serialize(output)
    val digest = Codec.digestBytes(bytes)
    findResultDataByOutput(digest)
  }

  def findResultsByOutput(outputDigest: Digest): Iterable[FunctionCallResultWithProvenance[_]] =
    findResultDataByOutput(outputDigest).map(_.load(this))

  def findResultsByOutput[O : Codec](output: O): Iterable[FunctionCallResultWithProvenance[O]] = {
    val codec = implicitly[Codec[O]]
    val bytes = codec.serialize(output)
    val digest = Codec.digestBytes(bytes)
    findResultsByOutput(digest).map(_.asInstanceOf[FunctionCallResultWithProvenance[O]])
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
    * Known failure modes for the ResultTracker.
    */

  class FailedSaveException(msg: String) extends RuntimeException(msg)

  class DataNotFoundException(msg: String) extends RuntimeException(msg)

  class SerializationInconsistencyException(msg: String) extends RuntimeException(msg)

  class DataInconsistencyException(msg: String) extends RuntimeException(msg)
}


