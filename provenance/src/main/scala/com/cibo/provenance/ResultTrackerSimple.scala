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

class ResultTrackerSimple(baseSyncablePath: SyncablePath)(implicit val currentBuildInfo: BuildInfo) extends ResultTracker with LazyLogging {

  import scala.reflect.ClassTag

  private val s3db = S3DB.fromSyncablePath(baseSyncablePath)

  protected def checkForOverwrite: Boolean = false
  protected def checkForInconsistentSerialization(newResult: FunctionCallResultWithProvenance[_]): Boolean = false
  protected def checkForConflictedOutputBeforeSave(newResult: FunctionCallResultWithProvenance[_]): Boolean = false
  protected def checkForResultAfterSave(newResult: FunctionCallResultWithProvenance[_]): Boolean = false

  protected def blockSavingConflicts(newResult: FunctionCallResultWithProvenance[_]): Boolean = false

  // public interface

  val basePath = baseSyncablePath

  def getCurrentBuildInfo: BuildInfo = currentBuildInfo

  override def resolve[O](f: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    val callWithInputDigests = f.resolveInputs(rt=this)
    loadResultForCallOption[O](callWithInputDigests) match {
      case Some(existingResult) =>
        existingResult
      case None =>
        val newResult = callWithInputDigests.run(this)
        if (checkForInconsistentSerialization(newResult)) {
          /*
          implicit val e = f.getEncoder
          implicit val d = f.getDecoder
          val problems = Util.checkSerialization(newResult.output(this))
          if (problems.nonEmpty) {
            throw new FailedSave(f"New result does not serialize consistently!: $newResult: $problems")
          }
          */
        }
        saveResult(newResult)
        newResult
    }
  }

  def saveResult[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    saveResultImpl(result)
  }

  //scalastyle:off
  def saveResultImpl[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this

    // TODO: This function should be idempotent for a given result and tracker pair.
    // A result could be in different states in different tracking systems.
    // In practice, we can prevent a lot of re-saves if a result remembers the last place it was saved and doesn't
    // re-save there.

    // The is broken down into its constituent parts: the call, the output, and the build info (including commit),
    // and these three identities are saved.
    val call: FunctionCallWithProvenance[O] = result.call
    val output: O = result.output
    val buildInfo: BuildInfo = result.getOutputBuildInfoBrief

    call match {
      case _: UnknownProvenance[O] =>
        throw new RuntimeException("Attempting to save the result of a dummy stub for unknown provenance!")
      case _ =>
    }

    // This is a lazy value that ensures we only save the BuildInfo once for this ResultTracker.
    ensureBuildInfoIsSaved

    // Unpack the call and build information for clarity below.
    val functionName = call.functionName
    val version = call.getVersionValue
    val versionId = version.id
    val commitId = buildInfo.commitId
    val buildId = buildInfo.buildId

    // While the raw data goes under data/, and a master index goes into data-provenance,
    // the rest of the data saved for the result is function-specific.
    val prefix = f"functions/$functionName/$versionId"

    // Save the output.
    implicit val outputClassTag = result.getOutputClassTag
    implicit val outputEncoder = result.call.getEncoder
    implicit val outputDecoder = result.call.getDecoder

    val outputClassName = outputClassTag.runtimeClass.getName
    val outputDigest = saveValue(output)
    val outputKey = outputDigest.id

    // TODO: This is for testing.  Remove if it matches code below.
    val callWithDeflatedInputs: FunctionCallWithProvenance[O] = call.deflateInputs(this)
    val callWithDeflatedInputsExcludingVersion: FunctionCallWithProvenance[O] = callWithDeflatedInputs

    val expectedInputsDeflated: Vector[FunctionCallResultWithProvenanceDeflated[_]] =
      callWithDeflatedInputs.getInputs.toVector.asInstanceOf[Vector[FunctionCallResultWithProvenanceDeflated[_]]]

    // Make a deflated copy of the inputs.  These are used further below, and also have a digest
    // value used now.
    val inputsDeflated: Vector[FunctionCallResultWithProvenanceDeflated[_]] =
      call.getInputsDeflated(this) // TODO: Rename this

    // Save the sequence of input digests.
    // TODO: This should be unnecessary now, since the inputs are resolved and deflating them saves them.
    val inputsDeflatedNewlySaved = (call.getVersion +: call.getInputs).map {
      case u: UnknownProvenance[_] =>
        saveCall(u)
      case u: UnknownProvenanceValue[_] =>
        saveCall(u.call)
      case _ =>
    }

    // This is what prevents us from re-running the same code on the same data,
    // even when the inputs have different provenance.
    val inputDigests = inputsDeflated.map(_.outputDigest).toList //.mkString("\n").toCharArray
    val inputGroupDigest = saveObjectToSubPathByDigest(f"$prefix/input-group-values", inputDigests)
    val inputGroupKey = inputGroupDigest.id

    import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

    // Save the deflated version of the all.  This recursively decomposes the call into DeflatedCall objects.
    val provenanceDeflated: FunctionCallWithProvenanceDeflated[O] = saveCall(callWithDeflatedInputsExcludingVersion) // TODO: Pass-in the callWithDeflatedInputs?
    val provenanceDeflatedSerialized = Util.serialize(provenanceDeflated)
    val provenanceDeflatedKey = Util.digestBytes(provenanceDeflatedSerialized).id

    /*  In production, this flag is NOT set.  We skip the slow check for a possible conflict, and
        if one is found later, we it records a taint in the data fabric.

        During development, it is more helpful to throw an exception before recording the conflict.  It
        is most likely that the inconsistency is present only in the developer's environment, and the conflict
        can be remedied before the code is published.
    */

    if (blockSavingConflicts(result))
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest) match {
          case Some((prevOutputId, prevCommitId, prevBuildId)) =>
            if (prevOutputId != outputDigest) {
              val prevOutput = loadValueOption(prevOutputId)
              throw new FailedSave(
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
            val ee = e
            throw new FailedSave(f"Error checking for conflicting output before saving! $e")
      }

    // Link each of the above.
    saveObjectToPath(f"$prefix/provenance-to-inputs/$provenanceDeflatedKey/$inputGroupKey", "")
    saveObjectToPath(f"$prefix/output-to-provenance/$outputKey/$inputGroupKey/$provenanceDeflatedKey", "")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    saveObjectToPath(f"data-provenance/$outputKey/as/$outputClassName/from/$functionName/$versionId/with-inputs/$inputGroupKey/with-provenance/$provenanceDeflatedKey/at/$commitId/$buildId", "")

    // Make each of the up-stream functions behind the inputs link to this one as progeny.
    inputsDeflated.indices.map {
      n =>
        val inputResult: FunctionCallResultWithProvenanceDeflated[_] = inputsDeflated(n)
        val inputCall = inputResult.deflatedCall
        inputCall match {
          case _: FunctionCallWithUnknownProvenanceDeflated[_] =>
            // Identity values do not link to all places they are used.
            // If we did this, values like "true" and "1" would point to every function that took them as inputs.
          case i: FunctionCallWithKnownProvenanceDeflated[_] =>
            val path =
              f"functions/${i.functionName}/${i.functionVersion.id}/output-uses/" +
              f"${inputResult.outputDigest.id}/from-input-group/${inputGroupDigest.id}/with-prov/${i.inflatedCallDigest.id}/" +
              f"went-to/$functionName/$versionId/input-group/$inputGroupKey/arg/$n"
            saveObjectToPath(path, "")
        }
    }

    // Return a deflated result.  This should re-constitute to match the original.
    val deflated = FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = provenanceDeflated,
      inputGroupDigest = inputGroupDigest,
      outputDigest = outputDigest,
      buildInfo = buildInfo
    )(outputClassTag, outputEncoder, outputDecoder)

    // Save the final link from inputs to outputs after the others.
    // If a save partially completes, this will not be present, the next attempt will run again, and some of the data
    // it saves will already be there.  This effectively completes the transaction, though the partial states above
    // are not incorrect/invalid.

    val savePath = f"$prefix/inputs-to-output/$inputGroupKey/$outputKey/$commitId/$buildId"

    if (checkForConflictedOutputBeforeSave(result)) {
      // Perform extra checks before saving to the tracking system.
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
              throw new FailedSave(
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
      // Save without checking.  This is the default, since presumably the test suite checks.
      saveObjectToPath(savePath, "")
    }

    if (checkForResultAfterSave(result)) {
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest) match {
          case Some(ids) =>
            logger.debug(f"Found ${ids}")
          case None =>
            throw new FailedSave(f"No data saved for the current inputs?")
        }
      } catch {
        case f: FailedSave =>
          throw f
        case e: Exception =>
          throw new FailedSave(f"Error finding a single output for result after save!: $e")
          loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest)
      }
    }

    deflated
  }

  class FailedSave(m: String) extends RuntimeException(m)

  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this
    implicit val ct: ClassTag[O] = call.getOutputClassTag
    implicit val ec: io.circe.Encoder[O] = call.getEncoder
    implicit val dc: io.circe.Decoder[O] = call.getDecoder

    call match {

      case unknown: UnknownProvenance[O] =>
        // Skip saving calls with unknown provenance.
        // Whatever uses them can re-constitute the value from the input values.
        // But ensure the raw input value is saved as data.
        val digest = Util.digestObject(unknown.value)
        if (!hasValue(digest)) {
          val digest = saveValue(unknown.value)
          saveObjectToPath(f"data-provenance/${digest.id}/from/-", "")
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

  val recheckSaves: Boolean = true

  def getBytesAndDigest[T : io.circe.Encoder : io.circe.Decoder](obj: T): (Array[Byte], Digest) = {
    val bytes1 = Util.serialize(obj)
    val digest1 = Util.digestBytes(bytes1)
    if (recheckSaves) {
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

  def hasResultForCall[O](f: FunctionCallWithProvenance[O]): Boolean =
    loadOutputIdsForCallOption(f).nonEmpty

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
    loadSerializedDataForPath(f"functions/$functionName/${version.id}/provenance-values-typed/${digest.id}")

  def loadCallDeflatedSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"functions/$functionName/${version.id}/provenance-values-deflated/${digest.id}")


  private def extractDigest[Z](i: ValueWithProvenance[Z]) = {
    val iCall = i.unresolve(this)
    val iResult = i.resolve(this)
    val iValueDigested = iResult.outputAsVirtualValue.resolveDigest(iCall.getEncoder, iCall.getDecoder)
    iValueDigested.digestOption.get
  }

  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    // TODO: Remove debug code

    val inputDigests: Seq[Digest] = f.getInputs.map {
      i => extractDigest(i)
    }

    // Real code
    loadOutputIdsForCallOption(f).map {
      case (outputId, commitId, buildId) =>
        implicit val c: ClassTag[O] = f.getOutputClassTag
        implicit val e: Encoder[O] = f.getEncoder
        implicit val d: Decoder[O] = f.getDecoder

        val output: O = try {
          loadValue[O](outputId)
        } catch {
          case e: Exception =>
            // TODO: Remove debug code
            println(e.toString)
            loadValue[O](outputId)(f.getOutputClassTag, f.getEncoder, f.getDecoder)
        }
        val outputId2 = Util.digestObject(output)
        if (outputId2 != outputId) {
          throw new RuntimeException(f"Output value saved as $outputId reloads with a digest of $outputId2: $output")
        }
        val outputWrapped = VirtualValue(valueOption = Some(output), digestOption = Some(outputId), serializedDataOption = None)
        val bi = BuildInfoBrief(commitId, buildId)
        f.newResult(outputWrapped)(bi)
    }
  }

  def loadValueOption[T : ClassTag : Encoder : Decoder](digest: Digest): Option[T] = {
    loadValueSerializedDataOption(digest) map {
      bytes =>
        Util.deserialize[T](bytes)
    }
  }

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"data/${digest.id}")

  private def loadSerializedDataForPath(path: String) = {
    try {
      val bytes = s3db.getBytesForPrefix(path)
      Some(bytes)
    } catch {
      case e: Exception =>
        logger.error(f"Failed to load data from $path: $e")
        None
    }
  }

  def loadOutputIdsForCallOption[O](f: FunctionCallWithProvenance[O]): Option[(Digest, String, String)] = {
    f.getInputGroupValuesDigest(this)
    val inputGroupValuesDigest = {
      val digests = f.getInputs.map(i => extractDigest(i)).toList
      Digest(Util.digestObject(digests).id)
    }

    implicit val outputClassTag: ClassTag[O] = f.getOutputClassTag
    implicit val e: Encoder[O] = f.getEncoder
    implicit val d: Decoder[O] = f.getDecoder
    loadOutputCommitAndBuildIdForInputGroupIdOption(f.functionName, f.getVersionValue(this), inputGroupValuesDigest) match {
      case Some(ids) =>
        Some(ids)
      case None =>
        logger.debug(f"Failed to find value for $f")
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
    val bi = currentBuildInfo
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

class UnresolvedVersionException[O](call: FunctionCallWithProvenance[O])
  extends RuntimeException(f"Cannot deflate calls with an unresolved version: $call") {

  def getCall: FunctionCallWithProvenance[O] = call
}

