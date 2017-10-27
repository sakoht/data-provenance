package com.cibo.provenance.tracker

import com.cibo.io.s3.{S3DB, SyncablePath}
import com.cibo.provenance._
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

/**
  * Created by ssmith on 5/16/17.
  *
  * This ResultTracker can use Amazon S3 or the local filesystem for storage.
  *
  * It is shaped to be idempotent, contention-free, and to retroactively correct versioning errors.
  *
  * All data goes under:
  *   objects/$digest
  *
  * Per-function provenance metadata goes under:
  *   functions/$function-name/$version/
  *
  * An index from data backward is at:
  *   object-provenance/
  */

case class ResultTrackerSimple(basePath: SyncablePath)(implicit val currentBuildInfo: BuildInfo) extends ResultTracker with LazyLogging {

  def getCurrentBuildInfo: BuildInfo = currentBuildInfo

  import org.apache.commons.codec.digest.DigestUtils

  private val s3db = S3DB.fromSyncablePath(basePath)
  protected val  overwrite: Boolean = false

  // public interface

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
    val outputKey = outputDigest.value

    // Save the sequence of input digests.
    // This is what prevents us from re-running the same code on the same data,
    // even when the inputs have different provenance.
    val inputDigestsWithSource: Vector[CollapsedValue[_]] = provenance.getInputsDigestWithSourceFunctionAndVersion(this)
    val inputDigests = inputDigestsWithSource.map(_.outputDigest).toList
    val inputGroupDigest = saveObjectToSubPathByDigest(f"$prefix/input-group-values", inputDigests)
    val inputGroupKey = inputGroupDigest.value

    // Save the provenance.  This recursively decomposes the call into DeflatedCall objects.
    val provenanceClean: FunctionCallWithProvenance[O] = provenance.unresolve
    val provenanceDeflated = saveCall(provenanceClean)
    val provenanceKey = provenanceDeflated.callDigest.value

    // Link each of the above.
    saveObjectToPath(f"$prefix/provenance-to-inputs/$provenanceKey/$inputGroupKey", "")
    saveObjectToPath(f"$prefix/inputs-to-output/$inputGroupKey/$outputKey/$commitId/$buildId", "")
    saveObjectToPath(f"$prefix/output-to-provenance/$outputKey/$inputGroupKey/$provenanceKey", "")

    // Offer a primary entry point on the value to get to the things that produce it.
    // This key could be shorter, but since it is immutable and zero size, we do it just once here now.
    // Refactor to be more brief as needed.
    saveObjectToPath(f"data-provenance/$outputKey/from/$functionName/$versionId/with-inputs/$inputGroupKey/with-provenance/$provenanceKey/at/$commitId/$buildId", "")

    // Make each of the up-stream functions behind the inputs link to this one as progeny.
    inputDigestsWithSource.indices.map {
      n =>
        inputDigestsWithSource(n) match {
          case _: CollapsedIdentityValue[_] =>
            // Identity values do not link to all places they are used.
            // If we did this, values like "true" and "1" would point to every function that took them as inputs.
          case i: CollapsedCallResult[_] =>
            val path =
              f"functions/${i.functionName}/${i.functionVersion.id}/output-uses/" +
              f"${i.outputDigest.value}/from-input-group/${inputGroupDigest.value}/with-prov/${i.functionCallDigest.value}/" +
              f"went-to/$functionName/$versionId/input-group/$inputGroupKey/arg/$n"
            saveObjectToPath(path, "")
        }
    }

    // Return a deflated result.  This should re-constitute to match the original.
    FunctionCallResultWithProvenanceDeflated(
      deflatedCall = provenanceDeflated,
      outputClassName = outputClassName,
      outputDigest = outputDigest,
      buildInfo = buildInfo
    )(ct = result.getOutputClassTag)
  }

  def saveCall[O](call: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] = {
    val functionName = call.functionName
    val version = call.getVersionValue(rt = this)
    val versionId = version.id
    val prefix = f"functions/$functionName/$versionId"
    val provenanceCleanDigest = saveObjectToSubPathByDigest(f"$prefix/provenance-values", call)
    FunctionCallWithProvenanceDeflated(functionName, version, provenanceCleanDigest)(call.getOutputClassTag)
  }

  def saveValue[T : ClassTag](obj: T): Digest = {
    val bytes = Util.serialize(obj)
    val digest = Util.digestBytes(bytes)
    s3db.putObject(f"data/${digest.value}", bytes)
    digest
  }

  def hasValue[T : ClassTag](obj: T): Boolean = {
    val bytes = Util.serialize(obj)
    val digest = Util.digest(bytes)
    hasValue(digest)
  }

  def hasValue(digest: Digest): Boolean = {
    val path: SyncablePath = basePath.extendPath(f"data/${digest.value}")
    path.exists
  }

  def hasResultFor[O](f: FunctionCallWithProvenance[O]): Boolean =
    loadResultIdOption(f).nonEmpty

  def loadCallOption[O : ClassTag](functionName: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]] =
    loadCallSerializedDataOption(functionName, version, digest) map {
      bytes => loadValue[FunctionCallWithProvenance[O]](bytes)
    }

  def loadCallSerializedDataOption(functionName: String, version: Version, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"functions/$functionName/${version.id}/provenance-values/${digest.value}")

  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] =
    loadResultIdOption(f).map {
      outputId =>
        val ct = f.getOutputClassTag
        val output: O = loadValue[O](outputId)(ct)
        val tobj = Deflatable(valueOption = Some(output), digestOption = Some(outputId), serializedDataOption = None)(ct)
        f.newResult(tobj)
    }

  def loadValueOption[T : ClassTag](digest: Digest): Option[T] = {
    loadValueSerializedDataOption(digest) map {
      bytes => loadValue[T](bytes)
    }
  }

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]] =
    loadSerializedDataForPath(f"data/${digest.value}")

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


  // private methods

  private def loadResultIdOption[O](f: FunctionCallWithProvenance[O]): Option[Digest] = {
    val callDigest = f.getCallDigest(this)
    implicit val outputClassTag: ClassTag[O] = f.getOutputClassTag
    loadOutputDigest(f.functionName, f.getVersionValue(this), callDigest)(f.getOutputClassTag) match {
      case Some(outputId) =>
        Some(outputId)
      case None =>
        logger.debug(f"Failed to find value for $f")
        None
    }
  }

  private def saveObjectToPath[T : ClassTag](path: String, obj: T): String = {
    val bytes = Util.serialize(obj)
    val digest = DigestUtils.sha1Hex(bytes)
    val fullPath: SyncablePath = basePath.extendPath(path)
    if (overwrite || !fullPath.exists)
      Util.serialize(obj, fullPath.getFile)
    digest
  }

  private def saveObjectToSubPathByDigest[T : ClassTag](path: String, obj: T): Digest = {
    val bytes = Util.serialize(obj)
    val digest = Util.digestBytes(bytes)
    s3db.putObject(f"$path/${digest.value}", bytes)
    digest
  }

  private def loadObjectFromPath[T : ClassTag](path: String): T =
    loadValue[T](basePath.extendPath(path).getFile)

  private def loadOutputDigest[O : ClassTag](fname: String, fversion: Version, inputGroupId: Digest): Option[Digest] = {
      s3db.getSuffixesForPrefix(f"functions/$fname/${fversion.id}/inputs-to-output/${inputGroupId.value}/").toList match {
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

