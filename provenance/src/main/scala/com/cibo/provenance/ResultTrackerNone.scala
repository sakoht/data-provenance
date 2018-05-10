package com.cibo.provenance

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag


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

  protected[provenance] def saveResultSerializable(
    resultInSaveableForm: FunctionCallResultWithKnownProvenanceSerializable,
    inputResultsAlreadySaved: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceDeflated[_] = {
    FunctionCallResultWithProvenanceDeflated(resultInSaveableForm)
  }

  def saveCallSerializable[O](v: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceDeflated[O] =
    FunctionCallWithProvenanceDeflated(v)

  def saveOutputValue[T : Codec](obj: T)(implicit tt: universe.TypeTag[Codec[T]], ct: ClassTag[Codec[T]]): Digest = {
    // also a no-op that just calculates the ID and returns it
    Codec.digestObject(obj)
  }

  lazy val saveBuildInfo: Digest = {
    val bi = currentAppBuildInfo
    val (bytes, digest) = Codec.serialize(bi)
    digest
  }

  def hasOutputForCall[O](v: FunctionCallWithProvenance[O]): Boolean = true // always

  def loadResultByCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    // just re-run anything we need to "load"
    Some(f.run(this))
  }

  def hasValue[T : Codec](obj: T): Boolean = false // never

  def hasValue(digest: Digest): Boolean = false // never

  def loadValueOption[O : Codec](digest: Digest): Option[O] = None // never

  def loadValueSerializedDataByClassNameAndDigestOption(className: String, digest: Digest): Option[Array[Byte]] = None // never

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo] = None // never

  def loadCodecByType[T : Codec](implicit cdcd: Codec[Codec[T]]): Codec[T] =
    throw new UnavailableData("No codec data with this tracker")

  def loadCodecByClassNameAndCodecDigest[T : ClassTag](valueClassName: String, codecDigest: Digest)(implicit tt: universe.TypeTag[Codec[T]], ct: ClassTag[Codec[T]]): Codec[T] =
    throw new UnavailableData("No codec data with this tracker")

  def loadCodecsByValueDigest[T : ClassTag](valueDigest: Digest)(implicit tt: universe.TypeTag[Codec[T]], ct: ClassTag[Codec[T]]): Seq[Codec[T]] =
    Seq.empty

  def loadCallById(callId: Digest): Option[FunctionCallWithProvenanceDeflated[_]] = None

  def loadResultById(resultId: Digest): Option[FunctionCallResultWithProvenanceDeflated[_]] = None

  def saveOutputValue[T: Codec](obj: T)(implicit cdcd: Codec[Codec[T]]) = ???

  def loadCodecByType[T: ClassTag : universe.TypeTag](implicit cdcd: Codec[Codec[T]]) = ???

  def loadCodecByClassNameAndCodecDigest[T: ClassTag](valueClassName: String, codecDigest: Digest)(implicit cdcd: Codec[Codec[T]]) = ???

  def loadCodecsByValueDigest[T: ClassTag](valueDigest: Digest)(implicit cdcd: Codec[Codec[T]]) = ???
}


class UnavailableData(msg: String) extends RuntimeException(f"Unavailable for ResultTrackerNone: $msg")


