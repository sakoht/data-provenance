package com.cibo.provenance

import com.amazonaws.services.s3.model.PutObjectResult
import com.cibo.io.s3.{S3DB, SyncablePath}
import com.cibo.provenance.exceptions.InconsistentVersionException
import com.cibo.cache.GCache

import scala.util.{Failure, Success, Try}

/**
  * Created by ssmith on 5/16/17.
  *
  * This ResultTracker can use Amazon S3 or the local filesystem for storage.
  *
  * It is shaped to be idempotent, contention-free, and to retroactively correct versioning errors.
  *
  * All data is at paths like this, stored as serialized objects, and keyed by the SHA1 of the serialized bytes:
  *   data/VALUE_DIGEST
  *
  * A master index of provenance data is stored in this form:
  *   data-provenance/VALUE_DIGEST/as/OUTPUT_CLASS_NAME/from/FUNCTION_NAME/VERSION/with-inputs/INPUT_GROUP_DIGEST/with-provenance/PROVENANCE_DIGEST/at/COMMIT_ID/BUILD_ID
  *
  * Per-function provenance metadata lives under:
  *   functions/FUNCTION_NAME/VERSION/
  *
  * These paths under a function/version hold serialized data:
  *   - input-groups/INPUT_GROUP_DIGEST     A simple List[String] of the Digest IDs for each input value.
  *   - calls/CALL_DIGEST                   A FunctionCallWithProvenanceSaved with full inputs,
  *                                         but those inputs point have abbreviated calls.
  *
  * These these paths under a function/version record associations as they are made (zero-size: info is in the path):
  *   - call-resolved-inputs/PROVENANCE_DIGEST_TYPED/INPUT_GROUP_DIGEST
  *   - inputs-to-output/INPUT_GROUP_DIGEST/OUTPUT_DIGEST/COMMIT_ID/BUILD_ID
  *   - output-to-provenance/OUTPUT_DIGEST/INPUT_GROUP_DIGEST/DEFLATED_CALL_DIGEST
  *
  * Note that, for the three paths above that link output->input->provenance, an output can come from one or more
  * different inputs, and the same input can come from one-or more distinct provenance paths.  So the outputs-to-provenance
  * path could have a full tree of data, wherein each "subdir" has multiple values, and the one under it does too.
  *
  * The converse is _not_ true.  An input should have only one output (at a given version), and provenance value
  * should have only one input digest. When an input gets multiple outputs, we flag the version as bad at the current
  * commit/build.  When a provenance gets multiple inputs, the same is true, but the fault is in the inconsistent
  * serialization of the inputs, typically.
  *
  * @param basePath           The destination for new results, and default storage space for queries.
  * @param underlyingTracker  An optional list of other trackers which "underlay" this one, read-only.
  */
class ResultTrackerSimple(
  val basePath: SyncablePath,
  val writable: Boolean = true,
  val underlyingTracker: Option[ResultTrackerSimple] = None
)(implicit val currentAppBuildInfo: BuildInfo) extends ResultTracker {

  def over(underlying: ResultTrackerSimple): ResultTrackerSimple = {
    new ResultTrackerSimple(basePath, writable, Some(underlying))
  }

  def over(underlyingPath: SyncablePath): ResultTrackerSimple = {
    new ResultTrackerSimple(basePath, writable, Some(ResultTrackerSimple(underlyingPath, writable=false)))
  }

  import scala.reflect.ClassTag

  // These flags allow the tracker to operate in a more conservative mode.
  // They are useful in development when things like serialization consistency are still uncertain.
  protected def checkForInconsistentSerialization[O](obj: O): Boolean = false
  protected def blockSavingConflicts(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = false
  protected def checkForConflictedOutputBeforeSave(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = false
  protected def checkForResultAfterSave(newResult: FunctionCallResultWithKnownProvenanceSerializable): Boolean = false

  // The cache for saved results.
  val resultCacheSize = 1000L

  @transient
  private lazy val resultCache =
    GCache[FunctionCallResultWithKnownProvenanceSerializable, Boolean]()
      .maximumSize(resultCacheSize)
      .logRemoval(logger)
      .buildWith[FunctionCallResultWithKnownProvenanceSerializable, Boolean]

  def saveResult(
    resultSerializable: FunctionCallResultWithKnownProvenanceSerializable,
    inputResultsAlreadySaved: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceSaved[_] = {
    Option(resultCache.getIfPresent(resultSerializable)) match {
      case None =>
        saveResultImpl(resultSerializable, inputResultsAlreadySaved)
        resultCache.put(resultSerializable, true)
      case Some(_) =>
    }
    FunctionCallResultWithProvenanceSaved(resultSerializable)
  }

  private def saveResultImpl[O](
    resultSerializable: FunctionCallResultWithKnownProvenanceSerializable,
    inputResultsAlreadySaved: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceSaved[O] = {

    val inputGroupDigest = resultSerializable.inputGroupDigest
    val inputGroupId = inputGroupDigest.id

    val outputId = resultSerializable.outputDigest.id

    val commitId = resultSerializable.commitId
    val buildId = resultSerializable.buildId

    val callWithoutInputs = resultSerializable.call
    val callWithInputsId = callWithoutInputs.digestOfEquivalentWithInputs.id
    val outputClassName = callWithoutInputs.outputClassName
    val functionName = callWithoutInputs.functionName

    val functionVersion = callWithoutInputs.functionVersion
    val functionVersionId = functionVersion.id

    // While the raw data goes under data/, and a master index goes into data-provenance,
    // the rest of the data saved for the result is function-specific.
    val prefix = f"functions/$functionName/$functionVersionId"

    // When we save a result we save a single ID for the whole group of inputs.
    saveEmptyObjectToPath(f"$prefix/call-resolved-inputs/$callWithInputsId/$inputGroupId")

    // Link the output to the input group, and to the exact deflated provenance behind the inputs.
    saveEmptyObjectToPath(f"$prefix/output-to-provenance/$outputId/$inputGroupId/$callWithInputsId")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    saveEmptyObjectToPath(
      f"data-provenance/$outputId/as/$outputClassName/from/$functionName/$functionVersionId/" +
        f"with-inputs/$inputGroupId/with-provenance/$callWithInputsId/at/$commitId/$buildId"
    )

    // Make each of the up-stream functions behind the  inputs link to this one as known progeny.
    // If the up-stream functions later are determined to be defective/inconsistent at some particular builds,
    // these links will be traversed to potentially invalidate derived data.
    inputResultsAlreadySaved.indices.map {
      n =>
        val inputResultSaved: FunctionCallResultWithProvenanceSerializable = inputResultsAlreadySaved(n)
        val inputCallSaved: FunctionCallWithProvenanceSerializable = inputResultSaved.call
        inputCallSaved match {
          case _: FunctionCallWithUnknownProvenanceSerializable =>
          // We don't do this linkage for inputs with unknown provenance.
          // If we did this, values like "true" and "1" would point to every function that took them as inputs,
          // leading to a ton of noise in the data fabric.
          case i: FunctionCallWithKnownProvenanceSerializableWithoutInputs =>
            //val (inputCallBytes, inputCallDigest) = Util.getBytesAndDigest(i)
            val inputCallDigest = i.digestOfEquivalentWithInputs
            val path =
              f"functions/${i.functionName}/${i.functionVersion.id}/output-uses/" +
                f"${inputResultSaved.outputDigest.id}/from-input-group/$inputGroupId/with-prov/${inputCallDigest.id}/" +
                f"went-to/$functionName/$functionVersionId/input-group/$inputGroupId/arg/$n"
            saveEmptyObjectToPath(path)
          case other =>
            throw new FailedSaveException(f"Unexpected input type $other.  Cannot save result $resultSerializable!")
        }
    }

    // Save the group of input digests as a single entity.  The hash returned represents the complete set of inputs.
    val inputIds: List[String] = inputResultsAlreadySaved.toList.map(_.outputDigest.id)
    saveObject(f"$prefix/input-groups/$inputGroupId", inputIds)

    // Link the input group to the call.
    val callId: String = resultSerializable.call.digestOfEquivalentWithInputs.id
    saveEmptyObjectToPath(f"$prefix/call-resolved-inputs/$callId/$inputGroupId")

    // We save the link from inputs to output as the last step.  This will prevent subsequent calls to similar logic
    // from calling the internal implementation of the function.
    // This is done at the end to make up for the fact that we are using a lock-free architecture.
    // If the save logic above partially completes, this will not be present, and the next attempt will run again.
    // Some of the data it saves above will already be there, but should be identical.
    val inputOutputLinkPath = f"$prefix/inputs-to-output/$inputGroupId/$outputId/$commitId/$buildId"

    // Perform the save, possibly with additional sanity checks.
    if (checkForConflictedOutputBeforeSave(resultSerializable)) {
      // These additional sanity checks are not run by default.  They can be activated for debugging.
      val previousSavePaths = getListingRecursive(f"$prefix/inputs-to-output/$inputGroupId")
      if (previousSavePaths.isEmpty) {
        logger.debug("New data.")
        saveEmptyObjectToPath(inputOutputLinkPath)
      } else {
        previousSavePaths.find(previousSavePath => inputOutputLinkPath.endsWith(previousSavePath)) match {
          case Some(_) =>
            logger.debug("Skip re-saving.")
          case None =>
            if (blockSavingConflicts(resultSerializable)) {
              throw new FailedSaveException(
                f"Blocked attempt to save a second output ($outputId) for the same input!  " +
                  f"New output is $inputOutputLinkPath.  Previous paths: $previousSavePaths"
              )
            } else {
              logger.error(f"SAVING CONFLICTING OUTPUT.  Previous paths: $previousSavePaths.  " +
                "New path $inputOutputLinkPath."
              )
              saveEmptyObjectToPath(inputOutputLinkPath)
            }
        }
      }
    } else {
      // Default: Save without additional checking.
      // In production, if there is a conflict represented by the save, it will be detected and flagged by
      // the first code that uses the data.  It is actually good that the save completes, because it creates
      // evidence that the current function is behaving consistently at the current version.
      saveEmptyObjectToPath(inputOutputLinkPath)
    }

    // Optionally do additional post-save validation.
    if (checkForResultAfterSave(resultSerializable)) {
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, functionVersion, inputGroupDigest) match {
          case Some(ids) =>
            logger.debug(f"Found $ids")
          case None =>
            throw new FailedSaveException(f"No data saved for the current inputs?")
        }
      } catch {
        case e: FailedSaveException =>
          throw e
        case e: Exception =>
          // To debug, un-comment:
          // loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest)
          throw new FailedSaveException(f"Error finding a single output for result after save!: $e")
      }
    }

    // Return the result with the deflated wrapper.
    FunctionCallResultWithProvenanceSaved(resultSerializable)
  }

  def saveCall[O](callInSavableForm: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceSaved[O] = {

    val functionName = callInSavableForm.functionName
    val version = callInSavableForm.functionVersion
    val versionId = version.id

    val prefix = f"functions/$functionName/$versionId"

    val callBytes = callInSavableForm.toBytes
    val callId = callInSavableForm.toDigest.id

    saveBytes(f"functions/$functionName/${version.id}/calls/$callId", callBytes)

    FunctionCallWithProvenanceSaved(callInSavableForm)
  }

  def saveInputGroup(basePath: String, inputIds: List[Digest]): Digest = {
    val (bytes, digest) = Util.getBytesAndDigest(inputIds.map(_.id))
    saveBytes(f"$basePath/${digest.id}", bytes)
    digest
  }

  def saveOutputValue[T : ClassTag : Codec](obj: T): Digest = {
    val ct = implicitly[ClassTag[T]]
    val clazz: Class[_ <: T] = obj.getClass
    val ct3 = ClassTag(clazz)
    val (bytes, digest) = Util.getBytesAndDigest(obj, checkForInconsistentSerialization(obj))
    val outputClassName = Util.classToName[T]
    saveCodec[T](digest)
    if (!hasValue(digest)) {
      val path = f"data/${digest.id}"
      logger.info(f"Saving raw $obj to $path")
      saveBytes(path, bytes)
    }
    digest
  }

  // The cache for saved codecs.
  val codecCacheSize = 1000L

  @transient
  private lazy val codecCache =
    GCache[Codec[_], Digest]()
      .maximumSize(codecCacheSize)
      .logRemoval(logger)
      .buildWith[Codec[_], Digest]

  def saveCodec[T : ClassTag : Codec](outputDigest: Digest): Digest = {
    val codec = implicitly[Codec[T]]
    val outputClassName = Util.classToName[T]
    val codecDigest = Option(codecCache.getIfPresent(codec)) match {
      case None =>
        val codecDigest = saveCodecImpl[T](outputClassName, outputDigest)
        codecCache.put(codec, codecDigest)
        codecDigest
      case Some(codecDigest) =>
        codecDigest
    }
    saveEmptyObjectToPath(f"data-codecs/${outputDigest.id}/$outputClassName/${codecDigest.id}")
  }
  
  def saveCodecImpl[T : ClassTag : Codec](outputClassName: String, outputDigest: Digest): Digest = {
    val codec = implicitly[Codec[T]]
    implicit val codecCodec = Codec.selfCodec[T]
    val (codecBytes, codecDigest) =
      Util.getBytesAndDigest(codec, checkForInconsistentSerialization(codec))
    saveBytes(f"codecs/$outputClassName/${codecDigest.id}", codecBytes)
    codecDigest
  }


  def loadCodecByClassNameAndCodecDigest[T: ClassTag](valueClassName: String, codecDigest: Digest): Codec[T] = {
    val bytes = loadBytes(f"codecs/$valueClassName/${codecDigest.id}")
    Util.deserialize[Codec[T]](bytes)
  }

  def loadCodecStreamByValueDigest[T: ClassTag](valueDigest: Digest): Seq[Codec[T]] = {
    val valueClassName = Util.classToName[T]
    getListingRecursive(f"data-codecs/${valueDigest.id}/$valueClassName").flatMap {
       codecId =>
        Try(
          loadCodecByClassNameAndCodecDigest[T](valueClassName, Digest(codecId))
        ) match {
          case Success(codec) =>
            Some(codec)
          case Failure(err) =>
            val msg = f"Failed to deserialize codec $codecId for $valueClassName: $err"
            logger.warn(msg)
            None
        }
    }
  }

  def loadCodecByType[T : ClassTag]: Codec[T] = {
    val valueClassName = Util.classToName[T]
    implicit val codecCodec: Codec[Codec[T]] = Codec.selfCodec[T]
    getListingRecursive(f"codecs/$valueClassName").flatMap {
      path =>
        val bytes = loadBytes(path)
        Try(Util.deserialize[Codec[T]](bytes)).toOption
    }.head
  }

  def hasValue[T : ClassTag : Codec](obj: T): Boolean = {
    val digest = Util.digestObject(obj)
    hasValue(digest)
  }

  def hasValue(digest: Digest): Boolean =
    pathExists(f"data/${digest.id}")

  def hasOutputForCall[O](call: FunctionCallWithProvenance[O]): Boolean =
    loadOutputIdsForCallOption(call).nonEmpty


  def loadCallMetaByDigest(
    functionName: String,
    functionVersion: Version,
    digest: Digest
  ): Option[FunctionCallWithKnownProvenanceSerializableWithInputs] =
    loadCallSerializedDataOption(functionName, functionVersion, digest) map {
      bytes =>
        Util.deserialize[FunctionCallWithKnownProvenanceSerializableWithInputs](bytes)
    }

  protected def loadCallSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadBytesOption(f"functions/$functionName/${version.id}/calls/${digest.id}")

  private def extractDigest[Z](i: ValueWithProvenance[Z]) = {
    val iCall = i.unresolve(this)
    val iResult = i.resolve(this)
    val iValueDigested = iResult.outputAsVirtualValue.resolveDigest(iCall.outputCodec)
    iValueDigested.digestOption.get
  }

  def loadResultForCallOption[O](call: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    loadOutputIdsForCallOption(call).map {
      case (outputId, commitId, buildId) =>
        implicit val c: ClassTag[O] = call.outputClassTag
        implicit val e: Codec[O] = call.outputCodec

        val output: O = try {
          loadValue[O](outputId)
        } catch {
          case e: Exception =>
            // TODO: Remove debug code
            println(e.toString)
            loadValue[O](outputId)(call.outputClassTag, call.outputCodec)
        }

        val outputId2 = Util.digestObject(output)
        if (outputId2 != outputId) {
          throw new SerializationInconsistencyException(f"Output value saved as $outputId reloads with a digest of $outputId2: $output")
        }

        val outputWrapped = VirtualValue(valueOption = Some(output), digestOption = Some(outputId), serializedDataOption = None)
        val bi = BuildInfoBrief(commitId, buildId)
        call.newResult(outputWrapped)(bi)
    }
  }

  def loadValueOption[T : ClassTag : Codec](digest: Digest): Option[T] = {
    loadValueSerializedDataOption[T](digest) map {
      bytes =>
        Util.deserialize[T](bytes)
    }
  }

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]] =
    loadBytesOption(f"data/${digest.id}")

  def loadOutputIdsForCallOption[O](call: FunctionCallWithProvenance[O]): Option[(Digest, String, String)] = {
    val inputGroupValuesDigest = call.getInputGroupValuesDigest(this)
    loadOutputCommitAndBuildIdForInputGroupIdOption(
      call.functionName,
      call.versionValue(this),
      inputGroupValuesDigest
    ) match {
      case Some(ids) =>
        Some(ids)
      case None =>
        logger.debug(f"Failed to find value for $call")
        None
    }
  }

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo] = {
    val basePrefix = f"commits/$commitId/builds/$buildId"
    val suffixes = getListingRecursive(basePrefix)
    suffixes match {
      case suffix :: Nil =>
        val bytes = loadBytes(f"$basePrefix/$suffix")
        val build = Util.deserialize[BuildInfo](bytes)
        Some(build)
      case Nil =>
        None
      case many =>
        throw new DataInconsistencyException(f"Multiple objects saved for build $commitId/$buildId?: $many")
    }
  }

  // private methods

  // This is called only once, and only rigth before a given build tries to actually save anything.
  @transient
  lazy val saveBuildInfo: Digest = {
    val bi = currentAppBuildInfo
    val (bytes, digest) = Util.getBytesAndDigest(bi)
    saveBytes(s"commits/${bi.commitId}/builds/${bi.buildId}/${digest.id}", bytes)
    digest
  }

  private def loadOutputCommitAndBuildIdForInputGroupIdOption(fname: String, fversion: Version, inputGroupId: Digest): Option[(Digest,String, String)] = {
    getListingRecursive(f"functions/$fname/${fversion.id}/inputs-to-output/${inputGroupId.id}") match {
      case Nil =>
        None
      case head :: Nil =>
        val words = head.split('/')
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        logger.debug(f"Got $outputId at commit $commitId from $buildId")
        Some((Digest(outputId), commitId, buildId))
      case listOfIds =>
        flagConflict(
          fname,
          fversion,
          inputGroupId,
          listOfIds
        )

        // Do some verbose logging.  If there are problems logging don't raise those, just the real error.
        Try {
          val ids = listOfIds.map {
            key =>
              val words = key.split("/")
              val outputId = words.head
          }
          logger.error(f"Multiple outputs for $fname $fversion $inputGroupId")
          ids.foreach {
            id =>
              logger.error(f"output ID: $id")
          }
        }

        throw new InconsistentVersionException(fname, fversion, listOfIds, Some(inputGroupId))
    }
  }

  def loadInputIds(fname: String, fversion: Version, inputGroupId: Digest): Seq[Digest] = {
    loadObject[List[String]](f"functions/$fname/${fversion.id}/input-groups/${inputGroupId.id}").map(Digest.apply)
  }

  def loadInputs[O : ClassTag : Codec](fname: String, fversion: Version, inputGroupId: Digest): Seq[Any] = {
    val digests = loadObject[List[Digest]](f"functions/$fname/${fversion.id}/input-groups/${inputGroupId.id}")
    digests.map {
      digest =>
        loadValueSerializedDataOption(digest) match {
          case Some(bytes) =>
            Util.deserialize(bytes)
          case None =>
            throw new DataNotFoundException(f"Failed to find data for input digest $digest for $fname $fversion!")
        }
    }
  }

  def flagConflict(
    functionName: String,
    functionVersion: Version,
    inputGroupId: Digest,
    conflictingOutputKeys: Seq[String]
  ): Unit = {
    // When this happens we recognize that there was, previously, a failure to set the version correctly.
    // This hopefully happens during testing, and the error never gets committed.
    // If it ends up in production data, we can compensate after the fact.
    saveEmptyObjectToPath(f"functions/$functionName/${functionVersion.id}/conflicted")
    saveEmptyObjectToPath(f"functions/$functionName/${functionVersion.id}/conflict/$inputGroupId")
    val inputIdSeq: Seq[Digest] = loadInputIds(functionName, functionVersion, inputGroupId)
    conflictingOutputKeys.foreach {
      s =>
        val words = s.split("/")
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        logger.error(f"Inconsistent output for $functionName at $functionVersion: at $commitId/$buildId input IDs ($inputIdSeq) return $outputId.")
    }

    /*
     * TODO: Auto-flag downstream results that used the bad output.
     *
     * The conflicted function+version has invalid commits, found by noting inputs with multiple outputs.
     * We next flag all commits except the earliest one for that output group as conflicted.
     * All outputs made from that commit are also conflicted.
     * We then find downstream calls used that those output values as an input, when coming from this function/version.
     * Those functions outputs are also flagged as conflicted.
     * The process repeats recursively.
     */
  }

  /*
   * Synchronous I/O Interface.
   *
   * An asynchronous interface is in development, but ideally the "chunk size" for units of work like this
   * are large enough that the asynchronous logic is happening _within_ the functions() instead of _across_ the them.
   *
   * Each unit of work should be something of a size that is reasonable to queue, check status, store,
   * check-for before producing, etc.  Granular chunks will have too much overhead to be used at this layer.
   *
   */

  import scala.concurrent.duration._
  import scala.concurrent.Await

  @transient
  protected lazy val s3db: S3DB = S3DB.fromSyncablePath(basePath)

  protected val saveTimeout: FiniteDuration = 5.minutes

  /**
    * There are two caches:
    * - lightCache (path -> Unit)
    *     - larger max size
    *     - holds just paths, no content
    *     - prevents duplicate saves
    *
    * - heavyCache: (path -> bytes)
    *     - consumes more space per path
    *     - smaller max size
    *     - prevents duplicate loads
    *
    */

  val lightCacheSize = 50000L

  @transient
  private lazy val lightCache =
    GCache[String, Unit]().maximumSize(lightCacheSize).buildWith[String, Unit]

  val heavyCacheSize = 500L

  @transient
  private lazy val heavyCache =
    GCache[String, Array[Byte]]().maximumSize(heavyCacheSize).buildWith[String, Array[Byte]]

  //protected
  def saveBytes(path: String, bytes: Array[Byte]): Unit = {
    if (!writable) {
      throw new ReadOnlyTrackerException(f"Attempt to save to a read-only ResultTracker $this.")
    } else {
      Option(lightCache.getIfPresent(path)) match {
        case Some(_) =>
        // Already saved.  Do nothing.
        case None =>
          Option(heavyCache.getIfPresent(path)) match {
            case Some(_) =>
              // Recently loaded.  Flag in the larger save cache, but otherwise do nothing.
              lightCache.put(path, Unit)
            case None =>
              // Actually save.
              s3db.putObject(path, bytes) match {
                case Some(f) =>
                  val result: PutObjectResult = Await.result(f, saveTimeout)
                  logger.debug(result.toString)
                case None =>
                // No future...
              }
              lightCache.put(path, Unit)
              heavyCache.put(path, bytes)
          }
      }
    }
  }


  def loadBytes(path: String): Array[Byte] = {
    try {
      val bytes = Option(heavyCache.getIfPresent(path)) match {
        case Some(found) =>
          found
        case None =>
          s3db.getBytesForPrefix(path)
      }
      lightCache.put(path, Unit)   // larger, lighter, prevents re-saves
      heavyCache.put(path, bytes)  // smaller, heavier, actually provides data
      bytes
    } catch {
      case e: Exception =>
        underlyingTracker match {
          case Some(underlying) =>
            underlying.loadBytes(path)
          case None =>
            throw e
        }
    }
  }

  protected def getListingRecursive(path: String): List[String] = {
    val listing1 = s3db.getSuffixesForPrefix(path).toList
    underlyingTracker match {
      case Some(underlying) =>
        val listing2 = underlying.getListingRecursive(path)
        (listing1.toVector ++ listing2.toVector).sorted.toList
      case None =>
        listing1
    }
  }

  protected def pathExists(path: String): Boolean = {
    Option(lightCache.getIfPresent(path)) match {
      case Some(_) =>
        true
      case None =>
        Option(heavyCache.getIfPresent(path)) match {
          case Some(_) =>
            true
          case None =>
            if (basePath.extendPath(path).exists) {
              lightCache.put(path, Unit)
              true
            } else {
              underlyingTracker match {
                case Some(under) =>
                  // We don't cache this because the underlying tracker will decide on that.
                  under.pathExists(path)
                case None =>
                  // We don't cache anything for false b/c the path may be added later
                  false
              }
            }
        }
    }
  }

  // Wrappers for loadBytes() and saveBytes():

  protected def loadBytesOption(path: String): Option[Array[Byte]] = {
    try {
      val bytes = loadBytes(path)
      Some(bytes)
    } catch {
      case e: Exception =>
        logger.debug(f"Failed to find at $path: $e")
        None
    }
  }

  protected def loadObject[T : ClassTag : Codec](path: String): T = {
    val bytes = loadBytes(path)
    Util.deserialize[T](bytes)
  }

  protected def saveObject[T : ClassTag: Codec](path: String, obj: T): String = {
    obj match {
      case _ : Array[Byte] =>
        throw new FailedSaveException("Attempt to save pre-serialized data?")
      case _ =>
        val (bytes, digest) = Util.getBytesAndDigest(obj, checkForInconsistentSerialization(obj))
        saveBytes(path, bytes)
        digest.id
    }
  }
  
  @transient
  private lazy val emptyBytesAndDigest = Util.getBytesAndDigest("")
  protected def emptyBytes: Array[Byte] = emptyBytesAndDigest._1
  protected def emptyDigest: Digest = emptyBytesAndDigest._2
  
  protected def saveEmptyObjectToPath(path: String): Digest = {
    saveBytes(path, emptyBytes)
    emptyDigest
  }
}

object ResultTrackerSimple {
  def apply(basePath: SyncablePath, writable: Boolean)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    new ResultTrackerSimple(basePath, writable)(currentAppBuildInfo)

  def apply(basePath: String, writable: Boolean)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    new ResultTrackerSimple(SyncablePath(basePath), writable=writable)(currentAppBuildInfo)

  def apply(basePath: String)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    new ResultTrackerSimple(SyncablePath(basePath))(currentAppBuildInfo)

  def apply(basePath: SyncablePath)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    new ResultTrackerSimple(basePath)(currentAppBuildInfo)

  // It is not normal to encode trackers, but it is possible that someone writes tools that record the trackers
  // themselves.  Also, if an application that takes calls as data attempt to save the Call, the result tracker
  // will be referenced in the codec.
}

class ReadOnlyTrackerException(msg: String)
  extends RuntimeException(f"Attempt to use a read-only tracker to write data: $msg")

