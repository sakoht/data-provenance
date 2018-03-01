package com.cibo.provenance

import com.amazonaws.services.s3.model.PutObjectResult
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.generic.auto._
import com.cibo.io.s3.{S3DB, SyncablePath}
import com.cibo.provenance.exceptions.InconsistentVersionException
import com.cibo.cache.GCache

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
  *   data-provenance/VALUE_DIGEST/as/OUTPUT_CLASS_NAME/from/FUNCTION_NAME/VERSION/with-inputs/INPUT_GROUP_DIGEST/with-provenance/PROVENANCE_DIGEST/at/COMMIT_ID/BUILD_ID
  *
  * Per-function provenance metadata lives under:
  *   functions/FUNCTION_NAME/VERSION/
  *
  * These paths under a function/version hold serialized data:
  *   - input-group-values/INPUT_GROUP_DIGEST       A simple List[String] of the Digest IDs for each input value.  
  *   - calls-inflated/REGULAR_PROVENANCE_DIGEST    A fully-typified Call object w/ deflated inputs.
  *   - calls-deflated/DEFLATED_PROVENANCE_DIGEST   A deflated Call object pointing to the inflated one.
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
  */
class ResultTrackerSimple(val basePath: SyncablePath)(implicit val currentAppBuildInfo: BuildInfo) extends ResultTracker with LazyLogging {

  import scala.reflect.ClassTag

  // These flags allow the tracker to operate in a more conservative mode.
  // They are useful in development when things like serialization consistency are still uncertain.
  protected def checkForInconsistentSerialization[O](obj: O): Boolean = false
  protected def blockSavingConflicts(newResult: FunctionCallResultWithProvenance[_]): Boolean = false
  protected def checkForConflictedOutputBeforeSave(newResult: FunctionCallResultWithProvenance[_]): Boolean = false
  protected def checkForResultAfterSave(newResult: FunctionCallResultWithProvenance[_]): Boolean = false

  // public interface

  val resultCacheSize = 1000L
  private val resultCache =
    GCache[FunctionCallResultWithProvenance[_], FunctionCallResultWithProvenanceDeflated[_]]()
      .maximumSize(resultCacheSize)
      .logRemoval(logger)
      .buildWith[FunctionCallResultWithProvenance[_], FunctionCallResultWithProvenanceDeflated[_]]


  def saveResult[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    val deflated: FunctionCallResultWithProvenanceDeflated[O] =
      Option(resultCache.getIfPresent(result)) match {
        case Some(found) =>
          found.asInstanceOf[FunctionCallResultWithProvenanceDeflated[O]]
        case None =>
          saveResultImpl(result)
      }
    resultCache.put(result, deflated)
    deflated
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
    val buildInfoAbbreviated: BuildInfo = result.outputBuildInfoBrief

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
    val version = call.versionValue
    val versionId = version.id
    val commitId = buildInfoAbbreviated.commitId
    val buildId = buildInfoAbbreviated.buildId

    // While the raw data goes under data/, and a master index goes into data-provenance,
    // the rest of the data saved for the result is function-specific.
    val prefix = f"functions/$functionName/$versionId"

    // Get implicits related to the output type O.
    implicit val outputClassTag: ClassTag[O] = result.outputClassTag
    implicit val outputEncoder: Encoder[O] = result.call.outputEncoder
    implicit val outputDecoder: Decoder[O] = result.call.outputDecoder

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
    // Sometimes has a Call.
    val expectedInputsDeflated: Vector[FunctionCallResultWithProvenanceDeflated[_]] =
      callWithDeflatedInputsExcludingVersion
        .inputs
        .toVector
        .asInstanceOf[Vector[FunctionCallResultWithProvenanceDeflated[_]]]

    // Make a deflated copy of the inputs.  These are used further below, and also have a digest value used now.
    // Sometimes has a Result.
    val inputsDeflated: Vector[FunctionCallResultWithProvenanceDeflated[_]] =
      call.getInputsDeflated(this)

    // Sanity check.
    if (inputsDeflated.toString != expectedInputsDeflated.toString) {
        //throw new RuntimeException("The call with deflated inputs does not match the original call's getInputsDeflated!")
        println("The call with deflated inputs does not match the original call's getInputsDeflated!")
    }

    // The deflation process will save any inputs that are not saved yet _except_ inputs with no provenance.
    // These only save when used as an input to another tracked entity (i.e. now).
    (call.version +: call.inputs).foreach {
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
    val callDeflated: FunctionCallWithProvenanceDeflated[O] = saveCall(callWithDeflatedInputsExcludingVersion)
    val (_, callDeflatedDigest) = Util.getBytesAndDigest(callDeflated)
    val callDeflatedKey = callDeflatedDigest.id

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
                f"Original output was $prevOutputId, made at commit $prevCommitId build $prevBuildId." +
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
    saveObject(f"$prefix/call-resolved-inputs/$callDeflatedKey/$inputGroupKey", "")

    // Link the output to the inputs, and to the exact deflated provenance behind the inputs.
    saveObject(f"$prefix/output-to-provenance/$outputKey/$inputGroupKey/$callDeflatedKey", "")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    val outputClassName = outputClassTag.runtimeClass.getName
    saveObject(f"data-provenance/$outputKey/as/$outputClassName/from/$functionName/$versionId/with-inputs/$inputGroupKey/with-provenance/$callDeflatedKey/at/$commitId/$buildId", "")

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
            saveObject(path, "")
        }
    }

    // Generate the final return value: a deflated result to match the deflated call.
    // This is a simple wrapper around the deflated call.
    // It should re-constitute to match the original.
    val deflated = FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = callDeflated,
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
      val previousSavePaths = getListingRecursive(f"$prefix/inputs-to-output/$inputGroupKey")
      if (previousSavePaths.isEmpty) {
        logger.debug("New data.")
        saveObject(savePath, "")
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
              saveObject(savePath, "")
            }
        }
      }
    } else {
      // Default: Save without additional checking.
      // In production, if there is a conflict represented by the save, it will be detected and flagged by
      // the first code that uses the data.  It is actually good that the save completes, because it creates
      // evidence that the current function is behaving consistently at the current version.
      saveObject(savePath, "")
    }

    // Optinally do additional post-save validation.
    if (checkForResultAfterSave(result)) {
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest) match {
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

    // Return the fully deflated result that compliments the incoming result parameter.
    deflated
  }

  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] = {
    val r1 = saveCallImpl(call)
    val r2 = saveCallImpl(call)
    if (r1 == r2) {
      // This never happens.
      r1
    } else {
      val r3 = saveCallImpl(call)
      if (r3 == r2)
        // The "first save problem", as yet unresolved.
        r2
      else
        throw new RuntimeException("Inconsistent serialization error.")
    }
  }

  private def saveCallImpl[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this
    implicit val outputClassTag: ClassTag[O] = call.outputClassTag
    implicit val outputEncoder: io.circe.Encoder[O] = call.outputEncoder
    implicit val outputDecoder: io.circe.Decoder[O] = call.outputDecoder

    // Since the call is in Circe JSON format, isolate enough real scala type information
    // to fully transform that entirely with its ID.
    implicit val encoderEncoder: Encoder[Encoder[O]] = new BinaryEncoder[Encoder[O]]
    implicit val decoderEncoder: Encoder[Decoder[O]] = new BinaryEncoder[Decoder[O]]

    implicit val encoderDecoder: Decoder[Encoder[O]] = new BinaryDecoder[Encoder[O]]
    implicit val decoderDecoder: Decoder[Decoder[O]] = new BinaryDecoder[Decoder[O]]

    val outputClassName = outputClassTag.toString
    val (encoderBytes, encoderDigest) = Util.getBytesAndDigest(outputEncoder)
    val (decoderBytes, decoderDigest) = Util.getBytesAndDigest(outputDecoder)

    saveBytes(f"types/Encoder/${encoderDigest.id}", encoderBytes)
    saveBytes(f"types/Decoder/${decoderDigest.id}", decoderBytes)

    call match {

      case unknown: UnknownProvenance[O] =>
        // Skip saving calls with unknown provenance.
        // Whatever uses them can re-constitute the value from the input values.
        // But ensure the raw input value is saved as data.
        val valueDigest = saveValue(unknown.value)
        saveObject(f"data-provenance/${valueDigest.id}/as/$outputClassName/from/-", "")
        // Return a deflated object.
        FunctionCallWithUnknownProvenanceDeflated[O](
          outputClassName = outputClassName,
          valueDigest = valueDigest
        )

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
        val versionValue: Version = known.versionValueAlreadyResolved match {
          case Some(value) =>
            value
          case None =>
            // While this call cannot be saved directly, any downstream call will be allowed to wrap it.
            // (This exception is intercepted in the block above of the downstream call.)
            throw UnresolvedVersionException(known)
        }

        val functionName = known.functionName
        val versionId = versionValue.id

        // Save the provenance object w/ the inputs deflated, but the type known.
        // Re-constituting the tree can happen one layer at a time.
        val (inflatedBytes, inflatedDigest) = Util.getBytesAndDigestRaw(inflatedCallWithDeflatedInputs)

        // Generate and save a fully "deflated" call, referencing the non-deflated one we just saved.
        // In this, even the return type is just a string, so it will work in foreign apps.
        val deflatedCall = FunctionCallWithKnownProvenanceDeflated[O](
          functionName = known.functionName,
          functionVersion = versionValue,
          inflatedCallDigest = inflatedDigest,
          outputClassName = outputClassName,
          encoderDigest = encoderDigest,
          decoderDigest = decoderDigest
        )

        val (deflatedCallBytes, deflatedCallDigest) = Util.getBytesAndDigest(deflatedCall)

        saveBytes(
          f"functions/$functionName/$versionId/calls-inflated/${inflatedDigest.id}",
          inflatedBytes
        )

        saveBytes(
          f"functions/$functionName/$versionId/calls-deflated/${deflatedCallDigest.id}",
          deflatedCallBytes
        )

        deflatedCall
    }
  }

  def saveValue[T : ClassTag : Encoder : Decoder](obj: T): Digest = {
    val (bytes, digest) = Util.getBytesAndDigest(obj, checkForInconsistentSerialization(obj))
    if (!hasValue(digest)) {
      val path = f"data/${digest.id}"
        logger.info(f"Saving raw $obj to $path")
      saveBytes(path, bytes)
    }
    digest
  }

  def hasValue[T : ClassTag : Encoder : Decoder](obj: T): Boolean = {
    val digest = Util.digestObject(obj)
    hasValue(digest)
  }

  def hasValue(digest: Digest): Boolean =
    pathExists(f"data/${digest.id}")

  def hasOutputForCall[O](call: FunctionCallWithProvenance[O]): Boolean =
    loadOutputIdsForCallOption(call).nonEmpty

  def loadCallByDigestOption[O : ClassTag : Encoder : Decoder](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]] =
    loadCallSerializedDataOption(functionName, version, digest) map {
      bytes =>
        Util.deserializeRaw[FunctionCallWithProvenance[O]](bytes)
    }

  protected def loadCallSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadBytesOption(f"functions/$functionName/${version.id}/calls-inflated/${digest.id}")

  def loadCallByDigestDeflatedOption[O : ClassTag : Encoder : Decoder](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenanceDeflated[O]] =
    loadCallDeflatedSerializedDataOption(functionName, version, digest) map {
      bytes =>
        // NOTE: Calls with UnknownProvenance save into a different subclass, but are not saved directly.
        // They are only used when representing deflated inputs in some subsequent call.
        // See the notes on the deflation flow in the scaladoc.
        Util.deserialize[FunctionCallWithKnownProvenanceDeflated[O]](bytes)
    }

  protected def loadCallDeflatedSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadBytesOption(f"functions/$functionName/${version.id}/calls-deflated/${digest.id}")

  def loadCallByDigestDeflatedUntypedOption(functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenanceDeflated[_]] =
    loadCallDeflatedSerializedDataOption(functionName, version, digest) map {
      bytes =>
        // NOTE: Calls with UnknownProvenance save into a different subclass, but are not saved directly.
        // They are only used when representing deflated inputs in some subsequent call.
        // See the notes on the deflation flow in the scaladoc.
        import io.circe.generic.auto._
        val untypedObj = Util.deserialize[FunctionCallWithKnownProvenanceDeflatedUntyped](bytes)
        val cn = untypedObj.outputClassName
        val clazz: Class[_] = Try(Class.forName(cn)).getOrElse(Class.forName("scala." + cn))
        deserializeAndTypifyCall(bytes, clazz, untypedObj.encoderDigest, untypedObj.decoderDigest)
    }

  private def deserializeAndTypifyCall[O : ClassTag](bytes: Array[Byte], clazz: Class[O], encoderId: Digest, decoderId: Digest): FunctionCallWithKnownProvenanceDeflated[O] = {
    implicit val classTagEncoder: Encoder[ClassTag[O]] = new BinaryEncoder[ClassTag[O]]
    implicit val encoderEncoder: Encoder[Encoder[O]] = new BinaryEncoder[Encoder[O]]
    implicit val decoderEncoder: Encoder[Decoder[O]] = new BinaryEncoder[Decoder[O]]

    implicit val classTagDecoder: Decoder[ClassTag[O]] = new BinaryDecoder[ClassTag[O]]
    implicit val encoderDecoder: Decoder[Encoder[O]] = new BinaryDecoder[Encoder[O]]
    implicit val decoderDecoder: Decoder[Decoder[O]] = new BinaryDecoder[Decoder[O]]

    implicit val en: Encoder[O] = loadObject[Encoder[O]](f"types/Encoder/${encoderId.id}")
    implicit val de: Decoder[O] = loadObject[Decoder[O]](f"types/Decoder/${decoderId.id}")
    Util.deserialize[FunctionCallWithKnownProvenanceDeflated[O]](bytes)
  }

  private def extractDigest[Z](i: ValueWithProvenance[Z]) = {
    val iCall = i.unresolve(this)
    val iResult = i.resolve(this)
    val iValueDigested = iResult.outputAsVirtualValue.resolveDigest(iCall.outputEncoder, iCall.outputDecoder)
    iValueDigested.digestOption.get
  }

  def loadResultForCallOption[O](call: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    loadOutputIdsForCallOption(call).map {
      case (outputId, commitId, buildId) =>
        implicit val c: ClassTag[O] = call.outputClassTag
        implicit val e: Encoder[O] = call.outputEncoder
        implicit val d: Decoder[O] = call.outputDecoder

        val output: O = try {
          loadValue[O](outputId)
        } catch {
          case e: Exception =>
            // TODO: Remove debug code
            println(e.toString)
            loadValue[O](outputId)(call.outputClassTag, call.outputEncoder, call.outputDecoder)
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
    loadBytesOption(f"data/${digest.id}")

  def loadOutputIdsForCallOption[O](call: FunctionCallWithProvenance[O]): Option[(Digest, String, String)] = {
    val digest1 = call.getInputGroupValuesDigest(this)
    val inputGroupValuesDigest = {
      val digests = call.inputs.map(i => extractDigest(i)).toList
      Digest(Util.digestObject(digests).id)
    }
    if (digest1 != inputGroupValuesDigest) {
      throw new RuntimeException("")
    }

    implicit val outputClassTag: ClassTag[O] = call.outputClassTag
    implicit val e: Encoder[O] = call.outputEncoder
    implicit val d: Decoder[O] = call.outputDecoder
    loadOutputCommitAndBuildIdForInputGroupIdOption(call.functionName, call.versionValue(this), inputGroupValuesDigest) match {
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
        throw new RuntimeException(f"Multiple objects saved for build $commitId/$buildId?: $many")
    }
  }

  // private methods

  // This is called only once, and only rigth before a given build tries to actually save anything.
  private lazy val ensureBuildInfoIsSaved: Digest = {
    val bi = currentAppBuildInfo
    val (bytes, digest) = Util.getBytesAndDigest(bi)
    saveBytes(s"commits/${bi.commitId}/builds/${bi.buildId}/${digest.id}", bytes)
    digest
  }

  private def loadOutputCommitAndBuildIdForInputGroupIdOption[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest): Option[(Digest,String, String)] = {
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
        flagConflict[O](
          fname,
          fversion,
          inputGroupId,
          listOfIds
        )

        // Do some verbose logging.  If there are problems logging don't raise those, just the real error.
        Try {
          val objects = listOfIds.map {
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
        }

        throw new InconsistentVersionException(fname, fversion, listOfIds, Some(inputGroupId))
    }
  }

  def loadInputIds[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest): Seq[Digest] = {
    loadObject[List[Digest]](f"functions/$fname/${fversion.id}/input-group-values/${inputGroupId.id}")
  }

  def loadInputs[O : ClassTag : Encoder : Decoder](fname: String, fversion: Version, inputGroupId: Digest): Seq[Any] = {
    val digests = loadObject[List[Digest]](f"functions/$fname/${fversion.id}/input-group-values/${inputGroupId.id}")
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

  def flagConflict[O : ClassTag : Encoder : Decoder](
    functionName: String,
    functionVersion: Version,
    inputGroupId: Digest,
    conflictingOutputKeys: Seq[String]
  ): Unit = {
    // When this happens we recognize that there was, previously, a failure to set the version correctly.
    // This hopefully happens during testing, and the error never gets committed.
    // If it ends up in production data, we can compensate after the fact.
    saveObject(f"functions/$functionName/${functionVersion.id}/conflicted", "")
    saveObject(f"functions/$functionName/${functionVersion.id}/conflict/$inputGroupId", "")
    val inputSeq: Seq[Any] = loadInputs(functionName, functionVersion, inputGroupId)
    conflictingOutputKeys.foreach {
      s =>
        val words = s.split("/")
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        val output: O = loadValue(Digest(outputId))
        logger.error(f"Inconsistent output for $functionName at $functionVersion: at $commitId/$buildId inputs ($inputSeq) return $output.")
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

  protected val s3db: S3DB = S3DB.fromSyncablePath(basePath)
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
  private val lightCache =
    GCache[String, Unit]().maximumSize(lightCacheSize).buildWith[String, Unit]

  val heavyCacheSize = 500L
  private val heavyCache =
    GCache[String, Array[Byte]]().maximumSize(heavyCacheSize).buildWith[String, Array[Byte]]

  protected def saveBytes(path: String, bytes: Array[Byte]): Unit = {
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
            //
            lightCache.put(path, Unit)
            heavyCache.put(path, bytes)
        }
    }
  }

  protected def loadBytes(path: String): Array[Byte] = {
    val bytes = Option(heavyCache.getIfPresent(path)) match {
      case Some(found) =>
        found
      case None =>
        s3db.getBytesForPrefix(path)
    }
    lightCache.put(path, Unit)   // larger, lighter, prevents re-saves
    heavyCache.put(path, bytes)  // smaller, heavier, actually provides data
    bytes
  }

  protected def getListingRecursive(path: String): List[String] = {
    s3db.getSuffixesForPrefix(path).toList
  }

  protected def pathExists(path: String): Boolean = {
    Option(lightCache.getIfPresent(path)) match {
      case Some(_) =>
        true
      case None =>
        Option(heavyCache.getIfPresent(path)) match {
          case Some(_) => true
          case None =>
            if (basePath.extendPath(path).exists) {
              lightCache.put(path, Unit)
              true
            } else {
              // We don't cache anything for false b/c the path may be added later
              false
            }
        }
    }
  }

  // Wrappers

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

  protected def loadObject[T : ClassTag : Encoder : Decoder](path: String): T = {
    val bytes = loadBytes(path)
    Util.deserialize[T](bytes)
  }

  protected def saveObject[T : ClassTag: Encoder: Decoder](path: String, obj: T): String = {
    obj match {
      case _ : Array[Byte] =>
        throw new RuntimeException("Attempt to save pre-serialized data?")
      case _ =>
        val (bytes, digest) = Util.getBytesAndDigest(obj, checkForInconsistentSerialization(obj))
        saveBytes(path, bytes)
        digest.id
    }
  }

  private def saveObjectToSubPathByDigest[T : ClassTag : Encoder : Decoder](path: String, obj: T): Digest = {
    val (bytes, digest) = Util.getBytesAndDigest(obj)
    saveBytes(f"$path/${digest.id}", bytes)
    digest
  }

}


object ResultTrackerSimple {
  def apply(basePath: SyncablePath)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    new ResultTrackerSimple(basePath)(currentAppBuildInfo)

  def apply(basePath: String)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    apply(SyncablePath(basePath))(currentAppBuildInfo)
}
