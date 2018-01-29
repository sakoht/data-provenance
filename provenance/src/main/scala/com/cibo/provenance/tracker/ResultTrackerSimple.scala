package com.cibo.provenance.tracker

import java.io.{ByteArrayInputStream, ObjectInputStream}

import com.cibo.io.s3.{S3DB, SyncablePath}
import com.cibo.provenance._
import com.cibo.provenance.monadics.GatherWithProvenance
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable

/**
  * Created by ssmith on 5/16/17.
  *
  * This ResultTracker can use Amazon S3 or the local filesystem for storage.
  *
  * It is shaped to be idempotent, contention-free, and to retroactively correct versioning errors.
  *
  * All data is at paths like this, stored as serialized objects.
  *   data/VALUE_DIGEST
  *
  * A master index of provenance data is stored in this form:
  *   data-provenance/VALUE_DIGEST/from/FUNCTION_NAME/VERSION/with-inputs/INPUT_GROUP_DIGEST/with-provenance/PROVENANCE_DIGEST/at/COMMIT_ID/BUILD_ID
  *
  * Per-function provenance metadata lives under:
  *   functions/FUNCTION_NAME/VERSION/
  *
  * These paths under a function/version hold serialized data:
  *   - input-group-values/INPUT_GROUP_DIGEST
  *   - provenance/PROVENANCE_DIGEST
  *
  * These these paths under a function/version record associations as they are made (zero-size: info is in the placement):
  *   - provenance-to-inputs/PROVENANCE_DIGEST/INPUT_GROUP_DIGEST
  *   - inputs-to-output/INPUT_GROUP_DIGEST/OUTPUT_DIGEST/COMMIT_ID/BUILD_ID
  *   - output-to-provenance/OUTPUT_DIGEST/INPUT_GROUP_DIGEST/PROVENANCE_DIGEST
  *
  */

class ResultTrackerSimple(baseSyncablePath: SyncablePath)(implicit val currentBuildInfo: BuildInfo) extends ResultTracker with LazyLogging {

  import scala.reflect.ClassTag
  import org.apache.commons.codec.digest.DigestUtils

  private val s3db = S3DB.fromSyncablePath(baseSyncablePath)
  protected val overwrite: Boolean = false

  val recheck: Boolean = true // during testing re-verify each save

  // public interface

  val basePath = baseSyncablePath

  def getCurrentBuildInfo: BuildInfo = currentBuildInfo

  //scalastyle:off
  def saveResult[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this

    // TODO: This function should be idempotent for a given result and tracker pair.
    // A result could be in different states in different tracking systems.
    // In practice, we can prevent a lot of re-saves if a result remembers the last place it was saved and doesn't
    // re-save there.

    // The is broken down into its constituent parts: the call, the output, and the build info (including commit),
    // and these three identities are saved.
    val call: FunctionCallWithProvenance[O] = result.provenance
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
    val outputClassTag = result.getOutputClassTag
    val outputClassName = outputClassTag.runtimeClass.getName
    val outputDigest = saveValue(output)(outputClassTag)
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
        saveCall(u.provenance)
      case _ =>
    }

    // This is what prevents us from re-running the same code on the same data,
    // even when the inputs have different provenance.
    val inputDigests = inputsDeflated.map(_.outputDigest).toList //.mkString("\n").toCharArray
    val inputGroupDigest = saveObjectToSubPathByDigest(f"$prefix/input-group-values", inputDigests)
    val inputGroupKey = inputGroupDigest.id

    // Save the deflated version of the all.  This recursively decomposes the call into DeflatedCall objects.
    val provenanceDeflated: FunctionCallWithProvenanceDeflated[O] = saveCall(callWithDeflatedInputsExcludingVersion) // TODO: Pass-in the callWithDeflatedInputs?
    val provenanceDeflatedSerialized = Util.serialize(provenanceDeflated)
    val provenanceDeflatedKey = Util.digestBytes(provenanceDeflatedSerialized).id

    if (recheck) {
      val callDeflatedOption = loadCallDeflatedOption(functionName, version, Digest(provenanceDeflatedKey))
      callDeflatedOption match {
        case Some(callDeflated) =>
          if (callDeflated != provenanceDeflated)
            logger.error("Mismatch!")
          else
            logger.warn("ok")
          val callInflatedWithDeflatedInputs = provenanceDeflated match {
            case u: FunctionCallWithUnknownProvenanceDeflated[O] =>
              val i1 = u.inflate
              if (i1 != call)
                logger.error("Mismatch!")
              else
                logger.debug("ok")
            case k: FunctionCallWithKnownProvenanceDeflated[O] =>
              val i1Option: Option[FunctionCallWithProvenance[Nothing]] = loadCallOption(functionName, version, k.inflatedCallDigest)
              i1Option match {
                case Some(i1) =>
                  val i1i = i1.inflate
                  val i1ii = i1i.inflateInputs
                  if (i1ii != call)
                    logger.error("Mismatch!")
                  else
                    logger.debug("ok")
                  try {
                    val ki: FunctionCallWithProvenance[O] = k.inflate
                    val kii = ki.inflateInputs
                    if (kii != i1ii)
                      logger.error("Inflation mismatch!")
                    else if (i1ii.resolveInputs != call.unresolveInputs)
                      logger.error("Mismatch!")
                    else
                      logger.debug("ok")
                  }
                  catch {
                    case e: Exception =>
                      logger.error("Error inflating?")
                      throw e
                  }
                  i1
                case None =>
                  throw new FailedSave("Failed to find inflated call?")
              }
          }
        case None =>
          throw new FailedSave("failed to find call after saving?!")
      }
    }


    // For debugging: see if the output already exists.
    val prevIds: Option[(Digest, String, String)] =
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest)(outputClassTag) match {
          case Some((prevOutputId, prevCommitId, prevBuildId)) =>
            if (prevOutputId != outputDigest) {
              throw new FailedSave(f"Saving a second output for the same input!")
              //logger.error(f"Saving a second output for the same input!")
              //Some(ids)
            } else {
              logger.warn(f"Re-saving the same output for the same input!")
              Some((prevOutputId, prevCommitId, prevBuildId))
            }
          case None =>
            logger.debug(f"No previous output found.")
            None
        }
      } catch {
          case e: Exception =>
            logger.debug(f"Error checking for previous results!")
            loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest)(outputClassTag)
      }

    // Link each of the above.
    saveObjectToPath(f"$prefix/provenance-to-inputs/$provenanceDeflatedKey/$inputGroupKey", "")
    saveObjectToPath(f"$prefix/output-to-provenance/$outputKey/$inputGroupKey/$provenanceDeflatedKey", "")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    saveObjectToPath(f"data-provenance/$outputKey/from/$functionName/$versionId/with-inputs/$inputGroupKey/with-provenance/$provenanceDeflatedKey/at/$commitId/$buildId", "")

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
    )(outputClassTag)

    // Save the final link from inputs to outputs after the others.
    // If a save partially completes, this will not be present, the next attempt will run again, and some of the data
    // it saves will already be there.  This effectively completes the transaction, though the partial states above
    // are not incorrect/invalid.
    saveObjectToPath(f"$prefix/inputs-to-output/$inputGroupKey/$outputKey/$commitId/$buildId", "")

    // TODO: For debugging: verify that we have exactly one
    if (recheck) {
      try {
        loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest)(outputClassTag) match {
          case Some(ids) =>
            logger.debug(f"Found ${ids}")
          case None =>
            throw new FailedSave(f"No data saved for the current inputs?")
        }
      } catch {
        case f: FailedSave =>
          throw f
        case e: Exception =>
          logger.warn("Error finding a single result for the inputs!")
          loadOutputCommitAndBuildIdForInputGroupIdOption(functionName, version, inputGroupDigest)(outputClassTag)
      }
    }

    deflated
  }

  class FailedSave(m: String) extends RuntimeException(m)

  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] =
    call match {

      case unknown: UnknownProvenance[O] =>
        // Skip saving calls with unknown provenance.
        // Whatever uses them can re-constitute the value from the input values.
        // But ensure the raw input value is saved as data.
        val digest = Util.digestObject(unknown.value)(unknown.getOutputClassTag)
        if (!hasValue(digest)) {
          val digest = saveValue(unknown.value)(unknown.getOutputClassTag)
          saveObjectToPath(f"data-provenance/${digest.id}/from/-", "")
        }
        // Return a deflated object.
        FunctionCallWithProvenanceDeflated(call)(rt=this)

      case known =>
        // Save and let the saver produce the deflated version it saves.
        implicit val rt: ResultTracker = this
        implicit val ct: ClassTag[O] = known.getOutputClassTag

        // Deflate the current call, saving upstream calls as needed.
        // This will stop deflating when it encounters an already deflated call.
        val inflatedCallWithDeflatedInputs: FunctionCallWithProvenance[O] =
          try {
            known.deflateInputs
          } catch {
            case e : UnresolvedVersionException[_] =>
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
        val inflatedProvenanceWithDeflatedInputsBytes: Array[Byte] = Util.serialize(inflatedCallWithDeflatedInputs)
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

  def saveValue[T : ClassTag](obj: T): Digest = {
    val bytes = Util.serialize(obj)
    val digest = Util.digestBytes(bytes)
    if (!hasValue(digest)) {
      val path = f"data/${digest.id}"
        logger.debug(f"Saving raw $obj to $path")
      s3db.putObject(path, bytes)
    }
    digest
  }

  def hasValue[T : ClassTag](obj: T): Boolean = {
    val digest = Util.digestObject(obj)
    hasValue(digest)
  }

  def hasValue(digest: Digest): Boolean = {
    val path: SyncablePath = baseSyncablePath.extendPath(f"data/${digest.id}")
    path.exists
  }

  def hasResultForCall[O](f: FunctionCallWithProvenance[O]): Boolean =
    loadOutputIdsForCallOption(f).nonEmpty

  def loadCallDeflatedOption[O : ClassTag](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenanceDeflated[O]] =
    loadCallDeflatedSerializedDataOption(functionName, version, digest) map {
      bytes => bytesToObject[FunctionCallWithProvenanceDeflated[O]](bytes)
    }

  def loadCallOption[O : ClassTag](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]] =
    loadCallSerializedDataOption(functionName, version, digest) map {
      bytes =>
        bytesToObject[FunctionCallWithProvenance[O]](bytes)
    }

  def loadCallSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"functions/$functionName/${version.id}/provenance-values-typed/${digest.id}")

  def loadCallDeflatedSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"functions/$functionName/${version.id}/provenance-values-deflated/${digest.id}")

  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] =
    loadOutputIdsForCallOption(f).map {
      case (outputId, commitId, buildId) =>
        val ct = f.getOutputClassTag
        val output: O = loadValue[O](outputId)(ct)
        val outputWrapped = VirtualValue(valueOption = Some(output), digestOption = Some(outputId), serializedDataOption = None)(ct)
        val bi = BuildInfoBrief(commitId, buildId)
        f.newResult(outputWrapped)(bi)
    }

  def loadValueOption[T : ClassTag](digest: Digest): Option[T] = {
    loadValueSerializedDataOption(digest) map {
      bytes => bytesToObject[T](bytes)
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
    val inputGroupValuesDigest = {//f.getInputGroupValuesDigest(this)
      val digests = f.getInputs.map(_.resolve(this).getOutputVirtual.resolveDigest.digestOption.get).toList
      Digest(Util.digestObject(digests).id)
    }

    implicit val outputClassTag: ClassTag[O] = f.getOutputClassTag
    loadOutputCommitAndBuildIdForInputGroupIdOption(f.functionName, f.getVersionValue(this), inputGroupValuesDigest)(f.getOutputClassTag) match {
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

  private def saveObjectToPath[T : ClassTag](path: String, obj: T): String = {
    obj match {
      case _ : Array[Byte] =>
        throw new RuntimeException("Attempt to save pre-serialized data?")
      case _ =>
        val bytes = Util.serialize(obj)
        val digest = DigestUtils.sha1Hex(bytes)
        saveSerializedDataToPath(path, bytes)
        digest

    }
  }

  private def saveSerializedDataToPath(path: String, serializedData: Array[Byte]): Unit = {
    val fullPath: SyncablePath = baseSyncablePath.extendPath(path)
    if (overwrite || !fullPath.exists)
      Util.saveBytes(serializedData, fullPath.getFile)
  }

  private def saveObjectToSubPathByDigest[T : ClassTag](path: String, obj: T): Digest = {
    val bytes = Util.serialize(obj)
    val digest = Util.digestBytes(bytes)
    s3db.putObject(f"$path/${digest.id}", bytes)
    digest
  }

  private def loadObjectFromPath[T : ClassTag](path: String): T =
    loadObjectFromFile[T](baseSyncablePath.extendPath(path).getFile)

  private def loadOutputCommitAndBuildIdForInputGroupIdOption[O : ClassTag](fname: String, fversion: Version, inputGroupId: Digest): Option[(Digest,String, String)] = {
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
        flagConflict[O](
          fname,
          fversion,
          inputGroupId,
          tooMany
        )
        throw new InconsistentVersionException(fname, fversion, tooMany, Some(inputGroupId))
    }
  }

  def loadInputs[O : ClassTag](fname: String, fversion: Version, inputGroupId: Digest): Seq[Any] = {
    val digests = loadObjectFromPath[List[Digest]](f"functions/$fname/${fversion.id}/input-group-values/${inputGroupId.id}")
    digests.map {
      digest =>
        loadValueSerializedDataOption(digest) match {
          case Some(bytes) =>
            val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
            ois.readObject
          case None =>
            throw new RuntimeException(f"Failed to find data for input digest $digest for $fname $fversion!")
        }
    }
  }

  def flagConflict[O : ClassTag](fname: String, fversion: Version, inputGroupId: Digest, conflictingOutputKeys: Seq[String]): Unit = {
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

