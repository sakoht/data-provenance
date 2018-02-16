package com.cibo.provenance

import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.generic.auto._
import com.cibo.io.s3.{S3DB, SyncablePath}
import com.cibo.provenance.exceptions.InconsistentVersionException

import scala.util.Try

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
  *   data-provenance/VALUE_DIGEST/as/OUTUT_CLASS_NAME/from/FUNCTION_NAME/VERSION/with-inputs/INPUT_GROUP_DIGEST/with-provenance/PROVENANCE_DIGEST/at/COMMIT_ID/BUILD_ID
  *
  * Per-function provenance metadata lives under:
  *   functions/FUNCTION_NAME/VERSION/
  *
  * These paths under a function/version hold serialized data:
  *   - input-group-values/INPUT_GROUP_DIGEST
  *   - provenance-values-typed/REGULAR_PROVENANCE_DIGEST
  *   - provenance-values-deflated/DEFLATED_PROVENANCE_DIGEST (contains the ID for the fully-typed object)
  *
  * These these paths under a function/version record associations as they are made (zero-size: info is in the placement):
  *   - provenance-to-inputs/PROVENANCE_DIGEST_TYPED/INPUT_GROUP_DIGEST
  *   - inputs-to-output/INPUT_GROUP_DIGEST/OUTPUT_DIGEST/COMMIT_ID/BUILD_ID
  *   - output-to-provenance/OUTPUT_DIGEST/INPUT_GROUP_DIGEST/DEFLATED_PROVENANCE_DIGEST
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
  */
class ResultTrackerSimple(baseSyncablePath: SyncablePath)(implicit val currentAppBuildInfo: BuildInfo) extends ResultTracker with LazyLogging {

  import scala.reflect.ClassTag

  private val s3db = S3DB.fromSyncablePath(baseSyncablePath)

  // These flags allow the tracker to operate in a more conservative mode.
  // They are useful in development when things like serialization consistency are still uncertain.
  protected def checkForInconsistentSerialization[O](obj: O): Boolean = false
  protected def blockSavingConflicts(newResult: FunctionCallResultWithProvenance[_]): Boolean = false
  protected def checkForConflictedOutputBeforeSave(newResult: FunctionCallResultWithProvenance[_]): Boolean = false
  protected def checkForResultAfterSave(newResult: FunctionCallResultWithProvenance[_]): Boolean = false

  // public interface

  val basePath = baseSyncablePath

  def saveResult[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    saveResultImpl(result)
  }

  //scalastyle:off
  def saveResultImpl[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this

    // Note on performance:
    //
    // This function should be idempotent for a given result and tracker pair, so the cost of re-saving
    // is just time.   We could prevent this by remembering that a result has been saved.
    // A result could, though, be in different states in different tracking systems, so there are actually N answers
    // to this question.
    //
    // In practice, we could prevent the majority of attempts to re-save if a result remembers
    // the last place it was saved and simply doesn't repeat _that_.

    // The result is broken down into its constituent parts: the call, the output, and the build info (with commit ID).
    // These three entities are all saved and linked below.
    val call: FunctionCallWithProvenance[O] = result.call
    val output: O = result.output
    val buildInfoAbbreviated: BuildInfo = result.getOutputBuildInfoBrief

    call match {
      case _: UnknownProvenance[O] =>
        // We should never attempt to save these wrappers for untracked data.
        // The underlying values themselves are saved when used as inputs to other calls, but not before.
        throw new RuntimeException("Attempting to save the result of a dummy stub for unknown provenance!")
      case _ =>
    }

    // This is a lazy value that ensures we only save the BuildInfo once for this ResultTracker.
    ensureBuildInfoIsSaved

    // Unpack the call and build information for clarity below.
    val functionName = call.functionName
    val version = call.getVersionValue
    val versionId = version.id
    val commitId = buildInfoAbbreviated.commitId
    val buildId = buildInfoAbbreviated.buildId

    // While the raw data goes under data/, and a master index goes into data-provenance,
    // the rest of the data saved for the result is function-specific.
    val prefix = f"functions/$functionName/$versionId"

    // Get implicits related to the output type O.
    implicit val outputClassTag = result.getOutputClassTag
    implicit val outputEncoder = result.call.getEncoder
    implicit val outputDecoder = result.call.getDecoder

    // Save the actual output value.
    // This is saved at data/$digestKey, so there will only be one copy for re-used values.
    val outputDigest = saveValue(output)
    val outputKey = outputDigest.id

    // This deflates the inputs, but not the call itself.  It is the first step to making a fully deflated call.
    // When we reverse the process to re-inflate, it happens in pieces so it can go just as far as the types
    // in the current app allow.
    // 1. Start with a deflated call w/ deflated inputs.
    // 2. Inflated the call w/ but leave the inputs deflated.
    // 3. Inflate each input where possible (applying this procedure recursively).
    val callWithDeflatedInputsExcludingVersion: FunctionCallWithProvenance[O] =
      call.deflateInputs(this)

    // This is a sanity check/assertion.
    val expectedInputsDeflated: Vector[FunctionCallResultWithProvenanceDeflated[_]] =
      callWithDeflatedInputsExcludingVersion
        .getInputs
        .toVector
        .asInstanceOf[Vector[FunctionCallResultWithProvenanceDeflated[_]]]

    // Make a deflated copy of the inputs.  These are used further below, and also have a digest value used now.
    val inputsDeflated: Vector[FunctionCallResultWithProvenanceDeflated[_]] =
      call.getInputsDeflated(this)

    if (inputsDeflated != expectedInputsDeflated) {
      throw new RuntimeException("The call with deflated inputs does not match the original call's getInputsDeflated!")
    }

    // The deflation process will save any inputs that are not saved yet _except_ inputs with no provenance.
    // These only save when used as an input to another tracked entity (i.e. now).
    (call.getVersion +: call.getInputs).foreach {
      case u: UnknownProvenance[_] =>
        saveCall(u)
      case u: UnknownProvenanceValue[_] =>
        saveCall(u.call)
      case _ =>
        // Already saved.
    }

    // Save the group of input digests as a single entity.  The hash returned represents the complete set of inputs.
    val inputDigests = inputsDeflated.map(_.outputDigest).toList
    val inputGroupDigest = saveObjectToSubPathByDigest(f"$prefix/input-group-values", inputDigests)
    val inputGroupKey = inputGroupDigest.id

    // This is imported locally to ensure we can serialize the deflated call.
    import io.circe.generic.auto._

    // Save the deflated version of the call.
    // This can be referenced from any scala code, including code that cannot instantiate the types.
    // It is the bridge to the inflated version saved above which only needs to recognize the output type.
    // Its digest is the universal identifier for the complete history of this result.
    val provenanceDeflated: FunctionCallWithProvenanceDeflated[O] = saveCall(callWithDeflatedInputsExcludingVersion)
    val provenanceDeflatedSerialized = Util.serialize(provenanceDeflated)
    val provenanceDeflatedKey = Util.digestBytes(provenanceDeflatedSerialized).id

    /**
      * In production, this flag is NOT set.  We skip the slow check for a possible conflict, and
      * if one is found later, we it records a taint in the data fabric.
      *
      * During development, it is more helpful to throw an exception before recording the conflict.
      * When the inconsistency is present in unpublished code, it is likely the developer will fix the problem,
      * and the conflict record will be meaningless.
      *
      * Note, this flag is checked a second time if checkForConflictedOutputBeforeSave, since it creates a second
      * chance to notice a save collision.
      */
    if (blockSavingConflicts(result))
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest) match {
          case Some((prevOutputId, prevCommitId, prevBuildId)) =>
            if (prevOutputId != outputDigest) {
              val prevOutput = loadValueOption(prevOutputId)
              throw new FailedSaveException(
                f"Blocked attempt to save a second output ($outputDigest) for the same input!  " +
                f"Original output was $prevOutputId." +
                f"\nOld output: $prevOutput" +
                f"\nNew output: $output"
              )
            } else {
              logger.debug(f"Previous output matches new output.")
            }
          case None =>
            logger.debug(f"No previous output found.")
        }
      } catch {
          case e: Exception =>
            throw new FailedSaveException(f"Error checking for conflicting output before saving! $e")
      }

    // Link the fully deflated provenance to the inputs.
    saveObjectToPath(f"$prefix/provenance-to-inputs/$provenanceDeflatedKey/$inputGroupKey", "")

    // Link the output to the inputs, and to the exact deflated provenance behind the inputs.
    saveObjectToPath(f"$prefix/output-to-provenance/$outputKey/$inputGroupKey/$provenanceDeflatedKey", "")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    val outputClassName = outputClassTag.runtimeClass.getName
    saveObjectToPath(f"data-provenance/$outputKey/as/$outputClassName/from/$functionName/$versionId/with-inputs/$inputGroupKey/with-provenance/$provenanceDeflatedKey/at/$commitId/$buildId", "")

    // Make each of the up-stream functions behind the inputs link to this one as known progeny.
    // If the up-stream functions later are determined to be defective/inconsistent at some particular builds,
    // these links will be traversed to potentially invalidate derived data.
    inputsDeflated.indices.map {
      n =>
        val inputResult: FunctionCallResultWithProvenanceDeflated[_] = inputsDeflated(n)
        val inputCall = inputResult.deflatedCall
        inputCall match {
          case _: FunctionCallWithUnknownProvenanceDeflated[_] =>
            // We don't do this linkage for inputs with known provenance.
            // If we did this, values like "true" and "1" would point to every function that took them as inputs,
            // leading to a ton of noise in the data fabric.
          case i: FunctionCallWithKnownProvenanceDeflated[_] =>
            val path =
              f"functions/${i.functionName}/${i.functionVersion.id}/output-uses/" +
              f"${inputResult.outputDigest.id}/from-input-group/${inputGroupDigest.id}/with-prov/${i.inflatedCallDigest.id}/" +
              f"went-to/$functionName/$versionId/input-group/$inputGroupKey/arg/$n"
            saveObjectToPath(path, "")
        }
    }

    // Generate the final return value: a deflated result to match the deflated call.
    // This is a simple wrapper around the deflated call.
    // It should re-constitute to match the original.
    val deflated = FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = provenanceDeflated,
      inputGroupDigest = inputGroupDigest,
      outputDigest = outputDigest,
      buildInfo = buildInfoAbbreviated
    )(outputClassTag, outputEncoder, outputDecoder)

    // We save the link from inputs to output as the last step.  It will prevent subsequent calls to similar logic
    // from calling the internal implementation of the function.
    // This is done at the end to make up for the fact that we are using a lock-free architecture.
    // If the save logic above partially completes, this will not be present, and the next attempt will run again.
    // Some of the data it saves above will already be there, but should be identical.
    val savePath = f"$prefix/inputs-to-output/$inputGroupKey/$outputKey/$commitId/$buildId"

    // Perform the save, possibly with additional sanity checks.
    if (checkForConflictedOutputBeforeSave(result)) {
      // These additional sanity checks are not run by default.  They can be activated for debugging.
      val previousSavePaths = s3db.getSuffixesForPrefix(f"$prefix/inputs-to-output/$inputGroupKey").toList
      if (previousSavePaths.isEmpty) {
        logger.debug("New data.")
        saveObjectToPath(savePath, "")
      } else {
        previousSavePaths.find(previousSavePath => savePath.endsWith(previousSavePath)) match {
          case Some(_) =>
            logger.debug("Skip re-saving.")
          case None =>
            if (blockSavingConflicts(result)) {
              throw new FailedSaveException(
                f"Blocked attempt to save a second output ($outputDigest) for the same input!  " +
                  f"New output is $savePath.  Previous paths: $previousSavePaths"
              )
            } else {
              logger.error(f"SAVING CONFLICTING OUTPUT.  Previous paths: $previousSavePaths.  " +
                "New path $savePath."
              )
              saveObjectToPath(savePath, "")
            }
        }
      }
    } else {
      // Default: Save without additional checking.
      // In production, if there is a conflict represented by the save, it will be detected and flagged by
      // the first code that uses the data.  It is actually good that the save completes, because it creates
      // evidence that the current function is behaving consistently at the current version.
      saveObjectToPath(savePath, "")
    }

    // Optinally do additional post-save validation.
    if (checkForResultAfterSave(result)) {
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest) match {
          case Some(ids) =>
            logger.debug(f"Found ${ids}")
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

    // Return the fully deflated result that compliments the incoming result parameter.
    deflated
  }

  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this
    implicit val outputClassTag: ClassTag[O] = call.getOutputClassTag
    implicit val outputEncoder: io.circe.Encoder[O] = call.getEncoder
    implicit val outputDecoder: io.circe.Decoder[O] = call.getDecoder

    call match {

      case unknown: UnknownProvenance[O] =>
        // Skip saving calls with unknown provenance.
        // Whatever uses them can re-constitute the value from the input values.
        // But ensure the raw input value is saved as data.
        val digest = Util.digestObject(unknown.value)
        if (!hasValue(digest)) {
          val digest = saveValue(unknown.value)
          val outputClassName = outputClassTag.runtimeClass.getName
          saveObjectToPath(f"data-provenance/${digest.id}/as/$outputClassName/from/-", "")
        }
        // Return a deflated object.
        FunctionCallWithProvenanceDeflated(call)(rt = this)

      case known =>
        // Save and let the saver produce the deflated version it saves.

        // Deflate the current call, saving upstream calls as needed.
        // This will stop deflating when it encounters an already deflated call.
        val inflatedCallWithDeflatedInputs: FunctionCallWithProvenance[O] =
        try {
          known.deflateInputs
        } catch {
          case e: UnresolvedVersionException[_] =>
            // An upstream call used a version that was not a raw value.
            // Don't deflate further in this corner case.
            // Just let the call contain the indirect form of the version.
            logger.warn(f"Unresolved version when saving call to $call: $e.")
            known
        }

        // Extract the version Value.
        val versionValue: Version = known.getVersionValueAlreadyResolved match {
          case Some(value) =>
            value
          case None =>
            // While this call cannot be saved directly, any downstream call will be allowed to wrap it.
            // (This exception is intercepted in the block above of the downstream call.)
            throw new UnresolvedVersionException(known)
        }

        val functionName = known.functionName
        val versionId = versionValue.id

        // Save the provenance object w/ the inputs deflated, but the type known.
        // Re-constituting the tree can happen one layer at a time.
        val inflatedProvenanceWithDeflatedInputsBytes: Array[Byte] = Util.serializeRaw(inflatedCallWithDeflatedInputs)
        val inflatedProvenanceWithDeflatedInputsDigest = Util.digestBytes(inflatedProvenanceWithDeflatedInputsBytes)
        saveSerializedDataToPath(
          f"functions/$functionName/$versionId/provenance-values-typed/${inflatedProvenanceWithDeflatedInputsDigest.id}",
          inflatedProvenanceWithDeflatedInputsBytes
        )

        // Generate and save a fully "deflated" call, referencing the non-deflated one we just saved.
        // In this, even the return type is just a string, so it will work in foreign apps.
        val deflatedCall = FunctionCallWithKnownProvenanceDeflated[O](
          functionName = known.functionName,
          functionVersion = versionValue,
          inflatedCallDigest = inflatedProvenanceWithDeflatedInputsDigest,
          outputClassName = known.getOutputClassTag.runtimeClass.getName
        )
        val deflatedCallBytes = Util.serialize(deflatedCall)
        val deflatedCallDigest = Util.digestBytes(deflatedCallBytes)
        saveSerializedDataToPath(
          f"functions/$functionName/$versionId/provenance-values-deflated/${deflatedCallDigest.id}",
          deflatedCallBytes
        )

        deflatedCall
    }
  }

  def getBytesAndDigest[T : io.circe.Encoder : io.circe.Decoder](obj: T): (Array[Byte], Digest) = {
    val bytes1 = Util.serialize(obj)
    val digest1 = Util.digestBytes(bytes1)
    if (checkForInconsistentSerialization(obj)) {
      val obj2 = Util.deserialize[T](bytes1)
      val bytes2 = Util.serialize(obj2)
      val digest2 = Util.digestBytes(bytes2)
      if (digest2 != digest1) {
        val obj3 = Util.deserialize[T](bytes2)
        val bytes3 = Util.serialize(obj3)
        val digest3 = Util.digestBytes(bytes3)
        if (digest3 == digest2)
          logger.warn(f"The re-constituted version of $obj digests differently $digest1 -> $digest2!  But the reconstituted object saves consistently.")
        else
          throw new RuntimeException(f"Object $obj digests as $digest1, re-digests as $digest2 and $digest3!")
      }
      (bytes2, digest2)
    } else {
      (bytes1, digest1)
    }
  }

  def isTainted[T : io.circe.Encoder : io.circe.Decoder](obj: T): Boolean = {
    val bytes1 = Util.serialize(obj)
    val digest1 = Util.digestBytes(bytes1)
    val obj2 = Util.deserialize[T](bytes1)
    val bytes2 = Util.serialize(obj2)
    val digest2 = Util.digestBytes(bytes2)
    if (digest2 != digest1) {
      logger.warn(f"The re-constituted version of $obj digests differently $digest1 -> $digest2!  But the reconstituted object saves consistently.")
      true
    } else {
      false
    }
  }

  def saveValue[T : ClassTag : Encoder : Decoder](obj: T): Digest = {
    val (bytes, digest) = getBytesAndDigest(obj)
    if (!hasValue(digest)) {
      val path = f"data/${digest.id}"
        logger.info(f"Saving raw $obj to $path")
      s3db.putObject(path, bytes)
    }
    digest
  }

  def hasValue[T : ClassTag : Encoder : Decoder](obj: T): Boolean = {
    val digest = Util.digestObject(obj)
    hasValue(digest)
  }

  def hasValue(digest: Digest): Boolean = {
    val path: SyncablePath = baseSyncablePath.extendPath(f"data/${digest.id}")
    path.exists
  }

  def hasResultForCall[O](call: FunctionCallWithProvenance[O]): Boolean =
    loadOutputIdsForCallOption(call).nonEmpty

  def loadDeflatedCallOption[O : ClassTag : Encoder : Decoder](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenanceDeflated[O]] =
    loadCallDeflatedSerializedDataOption(functionName, version, digest) map {
      bytes => Util.deserialize[FunctionCallWithProvenanceDeflated[O]](bytes)
    }

  def loadInflatedCallWithDeflatedInputsOption[O : ClassTag : Encoder : Decoder](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]] =
    loadCallSerializedDataOption(functionName, version, digest) map {
      bytes =>
        Util.deserializeRaw[FunctionCallWithProvenance[O]](bytes)
    }

  def loadCallSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPathOption(f"functions/$functionName/${version.id}/provenance-values-typed/${digest.id}")

  def loadCallDeflatedSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPathOption(f"functions/$functionName/${version.id}/provenance-values-deflated/${digest.id}")


  private def extractDigest[Z](i: ValueWithProvenance[Z]) = {
    val iCall = i.unresolve(this)
    val iResult = i.resolve(this)
    val iValueDigested = iResult.outputAsVirtualValue.resolveDigest(iCall.getEncoder, iCall.getDecoder)
    iValueDigested.digestOption.get
  }

  def loadResultForCallOption[O](call: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    // TODO: Remove debug code

    val inputDigests: Seq[Digest] = call.getInputs.map {
      i => extractDigest(i)
    }

    // Real code
    loadOutputIdsForCallOption(call).map {
      case (outputId, commitId, buildId) =>
        implicit val c: ClassTag[O] = call.getOutputClassTag
        implicit val e: Encoder[O] = call.getEncoder
        implicit val d: Decoder[O] = call.getDecoder

        val output: O = try {
          loadValue[O](outputId)
        } catch {
          case e: Exception =>
            // TODO: Remove debug code
            println(e.toString)
            loadValue[O](outputId)(call.getOutputClassTag, call.getEncoder, call.getDecoder)
        }
        val outputId2 = Util.digestObject(output)
        if (outputId2 != outputId) {
          throw new RuntimeException(f"Output value saved as $outputId reloads with a digest of $outputId2: $output")
        }
        val outputWrapped = VirtualValue(valueOption = Some(output), digestOption = Some(outputId), serializedDataOption = None)
        val bi = BuildInfoBrief(commitId, buildId)
        call.newResult(outputWrapped)(bi)
    }
  }

  def loadValueOption[T : ClassTag : Encoder : Decoder](digest: Digest): Option[T] = {
    loadValueSerializedDataOption(digest) map {
      bytes =>
        Util.deserialize[T](bytes)
    }
  }

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPathOption(f"data/${digest.id}")

  private def loadSerializedDataForPathOption(path: String) = {
    try {
      val bytes = s3db.getBytesForPrefix(path)
      Some(bytes)
    } catch {
      case e: Exception =>
        logger.debug(f"Failed to find at $path: $e")
        None
    }
  }

  def loadOutputIdsForCallOption[O](call: FunctionCallWithProvenance[O]): Option[(Digest, String, String)] = {
    call.getInputGroupValuesDigest(this)
    val inputGroupValuesDigest = {
      val digests = call.getInputs.map(i => extractDigest(i)).toList
      Digest(Util.digestObject(digests).id)
    }

    implicit val outputClassTag: ClassTag[O] = call.getOutputClassTag
    implicit val e: Encoder[O] = call.getEncoder
    implicit val d: Decoder[O] = call.getDecoder
    loadOutputCommitAndBuildIdForInputGroupIdOption(call.functionName, call.getVersionValue(this), inputGroupValuesDigest) match {
      case Some(ids) =>
        Some(ids)
      case None =>
        logger.debug(f"Failed to find value for $call")
        None
    }
  }

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo] = {
    val basePrefix = f"commits/$commitId/builds/$buildId"
    val suffixes = s3db.getSuffixesForPrefix(basePrefix).toList
    suffixes match {
      case suffix :: Nil =>
        val bytes = s3db.getBytesForPrefix(f"$basePrefix/$suffix")
        val build = Util.deserialize[BuildInfo](bytes)
        Some(build)
      case Nil =>
        None
      case many =>
        throw new RuntimeException(f"Multiple objects saved for build $commitId/$buildId?: $suffixes")
    }
  }

  // private methods

  // This is called only once, and only rigth before a given build tries to actually save anything.
  private lazy val ensureBuildInfoIsSaved: Digest = {
    val bi = currentAppBuildInfo
    val bytes = Util.serialize(bi)
    val digest = Util.digestBytes(bytes)
    saveSerializedDataToPath(s"commits/${bi.commitId}/builds/${bi.buildId}/${digest.id}", bytes)
    digest
  }

  private def saveObjectToPath[T : ClassTag: Encoder: Decoder](path: String, obj: T): String = {
    obj match {
      case _ : Array[Byte] =>
        throw new RuntimeException("Attempt to save pre-serialized data?")
      case _ =>
        val (bytes, digest) = getBytesAndDigest(obj)
        saveSerializedDataToPath(path, bytes)
        digest.id
    }
  }

  private def saveSerializedDataToPath(path: String, serializedData: Array[Byte]): Unit =
    s3db.putObject(f"$path", serializedData)

  private def saveObjectToSubPathByDigest[T : ClassTag : Encoder : Decoder](path: String, obj: T): Digest = {
    val bytes = Util.serialize(obj)
    val digest = Util.digestBytes(bytes)
    s3db.putObject(f"$path/${digest.id}", bytes)
    digest
  }

  private def loadObjectFromPath[T : ClassTag : Encoder : Decoder](path: String): T = {
    val bytes = s3db.getBytesForPrefix(path)
    Util.deserialize[T](bytes)
  }

  private def loadOutputCommitAndBuildIdForInputGroupIdOption[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest): Option[(Digest,String, String)] = {
    s3db.getSuffixesForPrefix(f"functions/$fname/${fversion.id}/inputs-to-output/${inputGroupId.id}").toList match {
      case Nil =>
        None
      case head :: Nil =>
        val words = head.split('/')
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        logger.debug(f"Got $outputId at commit $commitId from $buildId")
        Some((Digest(outputId), commitId, buildId))
      case tooMany =>
        val ids: List[String] = tooMany
        flagConflict[O](
          fname,
          fversion,
          inputGroupId,
          tooMany
        )
        // Do some verbose logging
        try {
          val objects = tooMany.map {
            key =>
              val words = key.split("/")
              val outputId = words.head
              loadValueOption(Digest(outputId))
          }
          logger.error(f"Multiple outputs for $fname $fversion $inputGroupId")
          objects.foreach {
            obj =>
              logger.error(f"output: $obj")
          }
        } catch {
          case e: Exception =>
        }
        throw new InconsistentVersionException(fname, fversion, tooMany, Some(inputGroupId))
    }
  }

  def loadInputIds[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest): Seq[Digest] = {
    loadObjectFromPath[List[Digest]](f"functions/$fname/${fversion.id}/input-group-values/${inputGroupId.id}")
  }

  def loadInputs[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest): Seq[Any] = {
    val digests = loadObjectFromPath[List[Digest]](f"functions/$fname/${fversion.id}/input-group-values/${inputGroupId.id}")
    digests.map {
      digest =>
        loadValueSerializedDataOption(digest) match {
          case Some(bytes) =>
            Util.deserialize(bytes)
          case None =>
            throw new RuntimeException(f"Failed to find data for input digest $digest for $fname $fversion!")
        }
    }
  }

  def flagConflict[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest, conflictingOutputKeys: Seq[String]): Unit = {
    // When this happens we recognize that there was, previously, a failure to set the version correctly.
    // This hopefully happens during testing, and the error never gets committed.
    // If it ends up in production data, we can compensate after the fact.
    saveObjectToPath(f"functions/$fname/${fversion.id}/conflicted", "")
    saveObjectToPath(f"functions/$fname/${fversion.id}/conflict/$inputGroupId", "")
    val inputSeq = loadInputs(fname, fversion, inputGroupId)
    conflictingOutputKeys.foreach {
      s =>
        val words = s.split("/")
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        val output: O = loadValue(Digest(outputId))
        val inputSeq = loadInputs(fname, fversion, inputGroupId)
        logger.error(f"Inconsistent output for $fname at $fversion: at $commitId/$buildId inputs ($inputSeq) return $output.")
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
}


object ResultTrackerSimple {
  def apply(basePath: SyncablePath)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    new ResultTrackerSimple(basePath)(currentAppBuildInfo)

  def apply(basePath: String)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    apply(SyncablePath(basePath))(currentAppBuildInfo)
}
