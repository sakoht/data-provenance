package com.cibo.provenance.tracker

import com.cibo.io.s3.{S3DB, SyncablePath}
import com.cibo.provenance._
import com.typesafe.scalalogging.LazyLogging

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

case class ResultTrackerSimple(basePath: SyncablePath)(implicit val currentBuildInfo: BuildInfo) extends ResultTracker with LazyLogging {

  import scala.reflect.ClassTag
  import org.apache.commons.codec.digest.DigestUtils

  private val s3db = S3DB.fromSyncablePath(basePath)
  protected val overwrite: Boolean = false

  // public interface

  def getCurrentBuildInfo: BuildInfo = currentBuildInfo

  def saveResult[O](result: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    implicit val rt: ResultTracker = this

    // The result links the provenance to an output produced at a given build.
    val provenance: FunctionCallWithProvenance[O] = result.getProvenanceValue
    val outputValue: O = result.getOutputValue
    val buildInfo: BuildInfo = result.getOutputBuildInfo

    // Unpack the provenance and build information for clarity below.
    val functionName = provenance.functionName
    val version = provenance.getVersionValue
    val versionId = version.id
    val commitId = buildInfo.commitId
    val buildId = buildInfo.buildId

    val prefix = f"functions/$functionName/$versionId"

    // Save the output.
    val outputClassTag = result.getOutputClassTag
    val outputClassName = outputClassTag.runtimeClass.getName
    val outputDigest = saveValue(outputValue)(outputClassTag)
    val outputKey = outputDigest.id

    // Save the sequence of input digests.
    // This is what prevents us from re-running the same code on the same data,
    // even when the inputs have different provenance.
    val inputDigestsWithSource: Vector[FunctionCallResultWithProvenanceDeflated[_]] = provenance.getInputsDigestWithSourceFunctionAndVersion(this)
    val inputDigests = inputDigestsWithSource.map(_.outputDigest).toList
    val inputGroupDigest = saveObjectToSubPathByDigest(f"$prefix/input-group-values", inputDigests)
    val inputGroupKey = inputGroupDigest.id

    // Save the provenance.  This recursively decomposes the call into DeflatedCall objects.
    val provenanceDeflated: FunctionCallWithProvenanceDeflated[O] = saveCall(provenance)
    val provenanceDeflatedSerialized = Util.serialize(provenanceDeflated)
    val provenanceDeflatedKey = Util.digest(provenanceDeflatedSerialized).id

    // Link each of the above.
    saveObjectToPath(f"$prefix/provenance-to-inputs/$provenanceDeflatedKey/$inputGroupKey", "")
    saveObjectToPath(f"$prefix/inputs-to-output/$inputGroupKey/$outputKey/$commitId/$buildId", "")
    saveObjectToPath(f"$prefix/output-to-provenance/$outputKey/$inputGroupKey/$provenanceDeflatedKey", "")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    saveObjectToPath(f"data-provenance/$outputKey/from/$functionName/$versionId/with-inputs/$inputGroupKey/with-provenance/$provenanceDeflatedKey/at/$commitId/$buildId", "")

    // Make each of the up-stream functions behind the inputs link to this one as progeny.
    inputDigestsWithSource.indices.map {
      n =>
        val inputResult: FunctionCallResultWithProvenanceDeflated[_] = inputDigestsWithSource(n)
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
    FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = provenanceDeflated,
      inputGroupDigest = inputGroupDigest,
      outputDigest = outputDigest,
      buildInfo = buildInfo
    )(outputClassTag)
  }

  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] =
    call match {
      case unknown: UnknownProvenance[O] =>
        // Skip saving calls with unknown provenance.
        // Whatever uses them can re-constitute the value from the input values.
        // But ensure the raw input value is saved as data.
        if (!hasValue(unknown))
          saveValue(unknown.value)(unknown.getOutputClassTag)
        // Return a deflated object.
        FunctionCallWithProvenanceDeflated(call)(rt=this)

      case known =>
        // Save and let the saver produce the deflated version it saves.
        implicit val rt: ResultTracker = this
        implicit val ct: ClassTag[O] = known.getOutputClassTag

        // Deflate the current call, saving upstream calls as needed.
        // This will stop decomposing when it encounters an already deflated call.
        val deflatedInputs: FunctionCallWithProvenance[O] =
          try {
            known.deflateInputs
          } catch {
            case _ : AmbiguousVersionException[_] =>
              // An upstream call used a version that was not a raw value.  Don't collapse further in this corner case.
              known
          }
        val deflatedInputsBlob: Array[Byte] = Util.serialize(deflatedInputs)
        val deflatedInputsDigest = Util.digest(deflatedInputsBlob)

        // Extract
        val versionValue: Version = known.getVersionValueAlreadyResolved match {
          case Some(value) => value
          case None => throw new AmbiguousVersionException(known)
        }

        val functionName = known.functionName
        val versionId = versionValue.id
        val prefix = f"functions/$functionName/$versionId"

        saveSerializedDataToPath(f"$prefix/provenance-values/${deflatedInputsDigest.id}", deflatedInputsBlob)

        // A fully deflated call, referencing the non-deflated one.
        // This is suitable as input in the current app/lib and downstream apps that understand the return type.
        FunctionCallWithKnownProvenanceDeflated[O](
          functionName = known.functionName,
          functionVersion = versionValue,
          inflatedCallDigest = deflatedInputsDigest,
          outputClassName = known.getOutputClassTag.runtimeClass.getName
        )
    }

  def saveValue[T : ClassTag](obj: T): Digest = {
    val bytes = Util.serialize(obj)
    val digest = Util.digestBytes(bytes)
    s3db.putObject(f"data/${digest.id}", bytes)
    digest
  }

  def hasValue[T : ClassTag](obj: T): Boolean = {
    val bytes = Util.serialize(obj)
    val digest = Util.digest(bytes)
    hasValue(digest)
  }

  def hasValue(digest: Digest): Boolean = {
    val path: SyncablePath = basePath.extendPath(f"data/${digest.id}")
    path.exists
  }

  def hasResultFor[O](f: FunctionCallWithProvenance[O]): Boolean =
    loadOutputIdForCallOption(f).nonEmpty

  def loadCallDeflatedOption[O : ClassTag](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenanceDeflated[O]] =
    loadCallSerializedDataOption(functionName, version, digest) map {
      bytes => bytesToObject[FunctionCallWithProvenanceDeflated[O]](bytes)
    }

  def loadCallOption[O : ClassTag](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]] =
    loadCallSerializedDataOption(functionName, version, digest) map {
      bytes =>
        bytesToObject[FunctionCallWithProvenance[O]](bytes)
    }

  def loadCallSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"functions/$functionName/${version.id}/provenance-values/${digest.id}")

  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] =
    loadOutputIdForCallOption(f).map {
      outputId =>
        val ct = f.getOutputClassTag
        val output: O = loadValue[O](outputId)(ct)
        val tobj = Deflatable(valueOption = Some(output), digestOption = Some(outputId), serializedDataOption = None)(ct)
        f.newResult(tobj)
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
        val ee = e
        logger.error(f"Failed to load data from $path")
        None
    }
  }

  def loadOutputIdForCallOption[O](f: FunctionCallWithProvenance[O]): Option[Digest] = {
    val inputGroupValuesDigest = f.getInputGroupValuesDigest(this)
    implicit val outputClassTag: ClassTag[O] = f.getOutputClassTag
    loadOutputIdForInputGroupIdOption(f.functionName, f.getVersionValue(this), inputGroupValuesDigest)(f.getOutputClassTag) match {
      case Some(outputId) =>
        Some(outputId)
      case None =>
        logger.debug(f"Failed to find value for $f")
        None
    }
  }

  // private methods

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
    val fullPath: SyncablePath = basePath.extendPath(path)
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
    loadObjectFromFile[T](basePath.extendPath(path).getFile)

  private def loadOutputIdForInputGroupIdOption[O : ClassTag](fname: String, fversion: Version, inputGroupId: Digest): Option[Digest] = {
      s3db.getSuffixesForPrefix(f"functions/$fname/${fversion.id}/inputs-to-output/${inputGroupId.id}/").toList match {
      case Nil =>
        None
      case head :: Nil =>
        val words = head.split('/')
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        logger.info(f"Got $outputId at commit $commitId from $buildId")
        Some(Digest(outputId))
      case tooMany =>
        flagConflict(
          fname,
          fversion,
          inputGroupId,
          tooMany
        )
        throw new InconsistentVersionException(fname, fversion, tooMany, Some(inputGroupId))
    }
  }

  def flagConflict(fname: String, fversion: Version, inputGroupId: Digest, conflictingOutputKeys: Seq[String]): Unit = {
    // When this happens we recognize that there was, previously, a failure to set the version correctly.
    // This hopefully happens during testing, and the error never gets committed.
    // If it ends up in production data, we can compensate after the fact.
    saveObjectToPath(f"functions/$fname/${fversion.id}/conflicted", "")
    saveObjectToPath(f"functions/$fname/${fversion.id}/conflict/$inputGroupId", "")
    conflictingOutputKeys.foreach {
      s =>
        val words = s.split("/")
        val outputId = words.head
        val commitId = words(1)
        val buildId = words(2)
        logger.error(f"Found $outputId at $commitId/$buildId for input $inputGroupId for $fname at $fversion!")
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
  def apply(basePath: String)(implicit currentAppBuildInfo: BuildInfo): ResultTrackerSimple =
    ResultTrackerSimple(SyncablePath(basePath))(currentAppBuildInfo)
}

class AmbiguousVersionException[O](call: FunctionCallWithProvenance[O])
  extends RuntimeException(f"Cannot deflate calls with an unresolved version: $call") {

  def getCall = call
}
