package com.cibo.provenance

import java.time.Instant

import scala.collection.immutable
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
    * @param callSerializable   The call to save.
    * @tparam O:                The output type of the call and returned deflated call.
    * @return                   The deflated call created during saving.
    */
  def saveCallSerializable[O](callSerializable: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceDeflated[O]

  /**
    * Save an UnknownProvenance call.  This is mostly a no-op, since there was no trackable call that generated the data.
    * The ResultTracker indexes this call so that it can track forward to its usage.
    *
    * @param callSerializable   The call to save.
    * @tparam O                 The output type of the call and returned deflated call.
    * @return                   The deflated call created during the save process.
    */
  def saveCallSerializable[O](callSerializable: FunctionCallWithUnknownProvenanceSerializable): FunctionCallWithProvenanceDeflated[O]

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

  def loadValueWithCodec[T : ClassTag](valueDigest: Digest)(implicit cct: Codec[Codec[T]]): (T, Codec[T]) = {
    val allCodecs: List[Codec[T]] = loadCodecsByValueDigestTyped[T](valueDigest: Digest).toList
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

  def loadCodecByType[T : ClassTag](implicit cdcd: Codec[Codec[T]]): Codec[T]

  def loadCodecByCodecDigest[T : ClassTag](codecDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Codec[T] = {
    val valueClassName = Codec.classTagToSerializableName[T]
    loadCodecByClassNameCodecDigestClassTagAndSelfCodec[T](valueClassName, codecDigest)
    //loadCodecByClassNameAndCodecDigest(valueClassName, codecDigest)
  }

  def loadCodecByClassNameCodecDigestClassTagAndSelfCodec[T : ClassTag](valueClassName: String, codecDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Codec[T]

  def loadCodecsByValueDigestTyped[T : ClassTag](valueDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Seq[Codec[T]]

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
    * Return all call data for which there is data in storage.
    *
    * @return An iterable of calls.
    */
  def findCallData: Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs]

  /**
    * Return all call data for which there is data in storage for a given function name.
    *
    * @return An iterable of calls.
    */
  def findCallData(functionName: String): Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs]

  /**
    * Return all call data for which there is data in storage for a given function name and verison number.
    *
    * @return An iterable of calls.
    */
  def findCallData(functionName: String, version: Version): Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs]


  // Results

  /**
    * Return all result data for which there is data in storage.
    *
    * @return An iterable of results.
    */
  def findResultData: Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  /**
    * Return all result data for which there is data in storage for a given function name.
    *
    * @return An iterable of results.
    */
  def findResultData(functionName: String): Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  /**
    * Return all result data for which there is data in storage for a given function name and version.
    *
    * @return An iterable of results.
    */
  def findResultData(functionName: String, version: Version): Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  // Results by output

  /**
    * Return all result data (unvivified) that returns an output with the specified digest.
    *
    * @param outputDigest   The digest of to search for.
    * @return               An iterable of results, un-vivified.
    */
  def findResultDataByOutput(outputDigest: Digest): Iterable[FunctionCallResultWithKnownProvenanceSerializable]

  /**
    * Return all result data (unvivified) that returns a given output value.
    *
    * @param output         The output value to search for.
    * @tparam O             The type of the output value (implicit Codec required),
    * @return               An iterable of results, un-vivified.
    */
  def findResultDataByOutput[O : Codec](output: O): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = {
    val codec = implicitly[Codec[O]]
    val bytes = codec.serialize(output)
    val digest = Codec.digestBytes(bytes)
    findResultDataByOutput(digest)
  }

  /**
    * Return results for a given output digest.
    *
    * @param outputDigest   The digest of to search for.
    * @return               An iterable of fully vivified results.
    */
  def findResultsByOutput(outputDigest: Digest): Iterable[FunctionCallResultWithProvenance[_]] =
    findResultDataByOutput(outputDigest).map(_.load(this))

  /**
    * Return results for a given output value.
    *
    * @param output         The output value to search for.
    * @tparam O             The type of the output value (implicit Codec required),
    * @return               An iterable of fully vivified results.
    */
  def findResultsByOutput[O : Codec](output: O): Iterable[FunctionCallResultWithProvenance[O]] = {
    val codec = implicitly[Codec[O]]
    val bytes = codec.serialize(output)
    val digest = Codec.digestBytes(bytes)
    findResultsByOutput(digest).map(_.asInstanceOf[FunctionCallResultWithProvenance[O]])
  }

  // Use

  /**
    * Find all places a given value is used as an input, including where the value had no
    * prior provenance tracking, and where it was an output of something with tracking.
    *
    * @param value      The value to search for.
    * @tparam O         The value type, which must have an implicit Codec.
    * @return           An iterable of un-vivified results that use the value as an input.
    */
  def findUsesOfValue[O : Codec](value: O): Iterable[FunctionCallResultWithProvenanceSerializable] = {
    val knownProvenance = findResultDataByOutput(value).flatMap(findUsesOfResult)
    val unknownProvenance = findUsesOfResult(UnknownProvenance(value).resolve(this))
    unknownProvenance ++ knownProvenance
  }

  /**
    * Find all places a given result is used as an input.
    *
    * @param result     The value to search for.
    * @return           An iterable of un-vivified results that use the value as an input.
    */
  def findUsesOfResult(result: FunctionCallResultWithProvenance[_]): Iterable[FunctionCallResultWithProvenanceSerializable] =
    findUsesOfResultWithIndex(result).map(_._1)

  /**
    * Find all places a given result is used as an input, specified by serialized result data.
    *
    * @param result     The value to search for.
    * @return           An iterable of un-vivified results that use the value as an input.
    */
  def findUsesOfResult(result: FunctionCallResultWithProvenanceSerializable): Iterable[FunctionCallResultWithProvenanceSerializable] =
    findUsesOfResultWithIndex(result).map(_._1)

  /**
    * Find all places a given result is used as an input.
    *
    * @param result     The value to search for.
    * @return           An iterable of un-vivified results that use the value as an input,
    *                   with the index position of the value in the inputs of each output result.
    */
  def findUsesOfResultWithIndex(result: FunctionCallResultWithProvenance[_]): Iterable[(FunctionCallResultWithProvenanceSerializable, Int)] = {
    val resultAsData = FunctionCallResultWithProvenanceSerializable.save(result)(this)
    findUsesOfResultWithIndex(resultAsData)
  }

  /**
    * Find all places a given result is used as an input, specifying the input result as unvivified data.
    *
    * @param result     The value to search for.
    * @return           An iterable of un-vivified results that use the value as an input,
    *                   with the index position of the value in the inputs of each output result.
    */
  def findUsesOfResultWithIndex(result: FunctionCallResultWithProvenanceSerializable): Iterable[(FunctionCallResultWithProvenanceSerializable, Int)] =
    result.call match {
      case k: FunctionCallWithKnownProvenanceSerializableWithoutInputs =>
        findUsesOfResultWithIndex(k.functionName, k.functionVersion, result.toDigest)
      case u: FunctionCallWithUnknownProvenanceSerializable =>
        findUsesOfResultWithIndex(f"UnknownProvenance[${u.outputClassName}]", NoVersion, u.valueDigest)
      case other =>
        throw new RuntimeException(f"Unexpected call type: $other")
    }

  /**
    * Find all places a given result is used as an input.
    *
    * @param functionName     The name of the function that produced the value to search for.
    * @param version          The version of the function that produced the value to search for.
    * @param resultDigest     The value to search for, expressed as a digest.
    * @return                 An iterable of un-vivified results that use the value as an input,
    *                         with the index position of the value in the inputs of each output result.
    */
  def findUsesOfResultWithIndex(functionName: String, version: Version, resultDigest: Digest): Iterable[(FunctionCallResultWithProvenanceSerializable, Int)]

  /**
    * Copy results from one storage to another.
    *
    * This can also be used to upgrade the storage tree if the structure of storage has changed,
    * presuming the old FunctionCallResultWithKnownProvenanceSerializable still deserializable by its codec.
    *
    */
  def copyResultsFrom(otherResultTracker: ResultTracker): Iterable[Try[FunctionCallResultWithKnownProvenanceSerializable]] =
    otherResultTracker.findResultData.map {
      resultAsData =>
        Try {
          val result = resultAsData.load(otherResultTracker)
          FunctionCallResultWithKnownProvenanceSerializable.save(result)(this)
        }
    }

  // Find tags

  def findTagHistory: Iterable[TagHistoryEntry] = {
    findFunctionNames.filter(
      n => n.startsWith("com.cibo.provenance.AddTag[") || n.startsWith("com.cibo.provenance.RemoveTag[")
    ).flatMap {
      functionName =>
        findResultData(functionName).flatMap(convertAddTagOrRemoveTagToTagHistoryEntry)
    }
  }

  def convertAddTagOrRemoveTagToTagHistoryEntry(result: FunctionCallResultWithProvenanceSerializable): Option[TagHistoryEntry] =
    result.call match {
      case call: FunctionCallWithKnownProvenanceSerializableWithoutInputs =>
        implicit val rt: ResultTracker = this
        call.expandInputs(this).inputList match {
          case subject :: tag :: ts :: Nil =>
            val addOrRemove =
              if (call.functionName.startsWith("com.cibo.provenance.AddTag["))
                AddOrRemoveTag.AddTag
              else if (call.functionName.startsWith("com.cibo.provenance.RemoveTag["))
                AddOrRemoveTag.RemoveTag
              else
                throw new RuntimeException(f"Unexpected AddOrRemoveTag flag: $call")
            Some(
              TagHistoryEntry(
                addOrRemove,
                subject.asInstanceOf[FunctionCallResultWithProvenanceSerializable],
                tag.loadAs[FunctionCallResultWithProvenance[Tag]].output,
                ts.loadAs[FunctionCallResultWithProvenance[Instant]].output
              )
            )
          case other => throw new RuntimeException(f"Unexpected: input to a result is not resolved? $other")
        }
      case other =>
        None
    }


  def findTagHistoryByOutputType(outputClassName: String): Iterable[TagHistoryEntry] =
    findTagHistory.filter { _.subjectData.call.outputClassName == outputClassName }

  def findTagHistorysByResultFunctionName(functionName: String): Iterable[TagHistoryEntry] =
    findTagHistory.filter {
      _.subjectData match {
        case k: FunctionCallResultWithKnownProvenanceSerializable =>
          k.call.functionName == functionName
        case u: FunctionCallResultWithUnknownProvenanceSerializable =>
          f"UnknownProvenance[${u.call.outputClassName}]" == functionName
      }
    }

  //

  /**
    * Collapse a history of add/remove tag events into just one item for tags that still exist.
    *
    * If the most recent event for a tag/subject pair is AddTag, it will be included.  Everything else is omitted.
    *
    * @param history  An iterable of TagHistoryEntry objects, potentially with multiple events per tag/subject.
    * @return         The most recent tag for each subject that has not been deleted.
    */
  def distillTagHistory(history: Iterable[TagHistoryEntry]): Iterable[TagHistoryEntry] = {
    history.groupBy(h => (h.subjectData, h.tag)).values.flatMap {
      addAndRemoveEventsForOneTagAndSubject =>
        val latest = addAndRemoveEventsForOneTagAndSubject.toSeq.maxBy(_.ts)
        if (latest.addOrRemove == AddOrRemoveTag.AddTag)
          Some(latest)
        else if (latest.addOrRemove == AddOrRemoveTag.RemoveTag)
          None
        else
          throw new RuntimeException(s"Unexpected add/remove flag: ${latest.addOrRemove}")
    }
  }

  def findTagApplications: Iterable[TagHistoryEntry] =
    distillTagHistory(findTagHistory)

  def findTagApplicationsByOutputType(outputClassName: String): Iterable[TagHistoryEntry] =
    distillTagHistory(findTagHistory.filter(_.subjectData.call.outputClassName == outputClassName))

  def findTagApplicationsByResultFunctionName(functionName: String): Iterable[TagHistoryEntry] =
    distillTagHistory(
      findTagHistory.filter {
        _.subjectData match {
          case k: FunctionCallResultWithKnownProvenanceSerializable =>
            k.call.functionName == functionName
          case u: FunctionCallResultWithUnknownProvenanceSerializable =>
            f"UnknownProvenance[${u.call.outputClassName}]" == functionName
        }
      }
    )

  def findTags: Iterable[Tag] =
    findTagApplications.toVector.sortBy(_.ts).reverseIterator.toIterable.map(_.tag).toVector.distinct

  def findTagsByOutputClassName(outputClassName: String): Iterable[Tag] =
    findTagApplicationsByOutputType(outputClassName).map(_.tag).toVector.distinct

  def findTagsByResultFunctionName(functionName: String): Iterable[Tag] =
    findTagApplicationsByResultFunctionName(functionName).map(_.tag).toVector.distinct


  // Find results by tag

  def findResultDataByTag(text: String): Iterable[FunctionCallResultWithProvenanceSerializable] =
    findResultDataByTag(Tag(text))

  def findResultDataByTag(tag: Tag): Iterable[FunctionCallResultWithProvenanceSerializable] = {
    // Find all places the tag is used as an input.
    val uses = findUsesOfValue(tag).flatMap {
      result =>
        // Limit to where it is used as an input to AddTag[...]
        result.call match {
          case call: FunctionCallWithKnownProvenanceSerializableWithoutInputs
              if call.functionName.startsWith("com.cibo.provenance.AddTag[") ||
                call.functionName.startsWith("com.cibo.provenance.RemoveTag[")
            =>
            Some(result)
          case other =>
            None
        }
    }
    distillTagHistory(uses.flatMap(convertAddTagOrRemoveTagToTagHistoryEntry)).map(_.subjectData)
  }

  def findTagHistory(tag: Tag): Iterable[TagHistoryEntry] =
    findUsesOfValue(tag).flatMap { result => convertAddTagOrRemoveTagToTagHistoryEntry(result) }

  def findTagHistoryBySubject(tag: Tag, subject: FunctionCallResultWithProvenanceSerializable): Iterable[FunctionCallResultWithProvenanceSerializable] = {
    // Find all places the tag is used as an input.
    findUsesOfValue(tag).flatMap {
      result =>
        // Limit to where it is used as an input to AddTag[...] or RemoveTag[...]
        result.call match {
          case call: FunctionCallWithKnownProvenanceSerializableWithoutInputs
              if call.functionName.startsWith("com.cibo.provenance.AddTag[") ||
                call.functionName.startsWith("com.cibo.provenance.RemoveTag[") =>
            // The first input is the subject.
            val firstInput = call.expandInputs(this).inputList.head.asInstanceOf[FunctionCallResultWithProvenanceSerializable]
            if (firstInput == subject)
              Some(result)
            else
              None
          case other =>
            None
        }
    }
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


