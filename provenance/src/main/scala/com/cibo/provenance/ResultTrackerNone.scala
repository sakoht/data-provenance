package com.cibo.provenance

import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag

/**
  * Created by ssmith on 5/16/17.
  *
  * This null ResultTracker never saves anything, and always returns a value for a query
  * by running the function in question.
  *
  * @param currentAppBuildInfo: The BuildInfo to use for new results. (Use DummyBuildInfo for test data.)
  */
case class ResultTrackerNone()(implicit val currentAppBuildInfo: BuildInfo) extends ResultTracker {

  implicit val bi: BuildInfo = currentAppBuildInfo

  def saveResult(
    resultInSaveableForm: FunctionCallResultWithKnownProvenanceSerializable,
    inputResultsAlreadySaved: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceSaved[_] = {
    FunctionCallResultWithProvenanceSaved(resultInSaveableForm)
  }

  def saveCall[O](v: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceSaved[O] =
    FunctionCallWithProvenanceSaved(v)

  def saveOutputValue[T : ClassTag: Encoder : Decoder](obj: T): Digest = {
    // also a no-op that just calculates the ID and returns it
    Util.digestObject(obj)
  }

  lazy val saveBuildInfo: Digest = {
    val bi = currentAppBuildInfo
    val (bytes, digest) = Util.getBytesAndDigest(bi)
    digest
  }

  def hasOutputForCall[O](v: FunctionCallWithProvenance[O]): Boolean = true // always

  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    // just re-run anything we need to "load"
    Some(f.run(this))
  }

  def hasValue[T : ClassTag : Encoder : Decoder](obj: T): Boolean = false // never

  def hasValue(digest: Digest): Boolean = false // never

  def loadCallMetaByDigest(
    functionName: String,
    functionVersion: Version,
    digest: Digest
  ): Option[FunctionCallWithKnownProvenanceSerializableWithInputs] = None // never

  def loadValueOption[O : ClassTag : Encoder : Decoder](digest: Digest): Option[O] = None // never

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]] = None // never

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo] = None // never
}

object ResultTrackerNone {
  //def apply(implicit val buildInfo: BuildInfo) = new ResultTrackerNone(buildInfo)
}
