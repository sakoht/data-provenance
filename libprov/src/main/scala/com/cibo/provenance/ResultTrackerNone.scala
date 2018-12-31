package com.cibo.provenance

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.Try


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

  def saveResultSerializable(
    resultInSaveableForm: FunctionCallResultWithKnownProvenanceSerializable,
    inputResultsAlreadySaved: Vector[FunctionCallResultWithProvenanceSerializable]
  ): FunctionCallResultWithProvenanceDeflated[_] = {
    FunctionCallResultWithProvenanceDeflated(resultInSaveableForm)
  }

  def saveCallSerializable[O](callSerializable: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceDeflated[O] =
    FunctionCallWithProvenanceDeflated(callSerializable)

  def saveCallSerializable[O](callSerializable: FunctionCallWithUnknownProvenanceSerializable): FunctionCallWithProvenanceDeflated[O] =
    FunctionCallWithProvenanceDeflated(callSerializable)

  def saveOutputValue[T : Codec](obj: T)(implicit ct: ClassTag[Codec[T]]): Digest = {
    // also a no-op that just calculates the ID and returns it
    Codec.digestObject(obj)(implicitly[Codec[T]], implicitly[Codec[T]].classTag)
  }

  lazy val saveBuildInfo: Digest = {
    val bi = currentAppBuildInfo
    val bytes = Codec.serialize(bi)
    val digest = Codec.digestBytes(bytes)
    digest
  }

  def hasOutputForCall[O](v: FunctionCallWithProvenance[O]): Boolean = true // always

  def loadResultByCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    // just re-run anything we need to "load"
    Some(f.run(this))
  }

  def loadResultByCallOptionAsync[O](f: FunctionCallWithProvenance[O])(implicit ec: ExecutionContext): Future[Option[FunctionCallResultWithProvenance[O]]] = {
    // just re-run anything we need to "load"
    Future { Some(f.run(this)) }
  }

  def hasValue[T : Codec](obj: T): Boolean = false // never

  def hasValue(digest: Digest): Boolean = false // never

  def loadValueOption[O : Codec](digest: Digest): Option[O] = None // never

  def loadValueSerializedDataByClassNameAndDigestOption(className: String, digest: Digest): Option[Array[Byte]] = None // never

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo] = None // never

  def loadCodecByType[T : Codec]: Codec[T] =
    throw new UnavailableData("No codec data with this tracker")

  def loadCodecByClassNameAndCodecDigest[T : ClassTag](valueClassName: String, codecDigest: Digest)(implicit ct: ClassTag[Codec[T]]): Codec[T] =
    throw new UnavailableData("No codec data with this tracker")

  def loadCodecsByValueDigest[T : ClassTag](valueDigest: Digest)(implicit ct: ClassTag[Codec[T]]): Seq[Codec[T]] =
    Seq.empty

  def loadCallById(callId: Digest): Option[FunctionCallWithProvenanceDeflated[_]] = None

  def loadResultById(resultId: Digest): Option[FunctionCallResultWithProvenanceDeflated[_]] = None

  def saveOutputValue[T: Codec](obj: T): Digest =
    throw new RuntimeException(f"Values cannot be be savedf with $this")

  def loadCodec[T: ClassTag] =
    throw new RuntimeException(f"Codecs cannot be loaded from $this")

  def loadCodecsByClassName(valueClassName: String): Seq[Try[Codec[_]]] =
    throw new RuntimeException(f"Codecs cannot be loaded from $this")

  def loadCodecs: Map[String, Seq[Try[Codec[_]]]] =
    throw new RuntimeException(f"Codecs cannot be loaded from $this")

  def loadCodecByClassNameAndCodecDigest(valueClassName: String, codecDigest: Digest): Codec[_] =
    throw new RuntimeException(f"Codecs cannot be loaded from $this")

  def loadCodecByClassNameCodecDigestClassTagAndSelfCodec[T: ClassTag](valueClassName: String, codecDigest: Digest): Codec[T] =
    throw new RuntimeException(f"Codecs cannot be loaded from $this")

  def loadCodecsByValueDigest[T: ClassTag](valueDigest: Digest): Seq[Codec[T]] = Nil

  def findFunctionNames: Iterable[String] = Iterable.empty

  def findFunctionVersions(functionName: String): Iterable[Version] = Iterable.empty

  def findCallData: Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs] = Iterable.empty

  def findCallData(functionName: String): Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs] = Iterable.empty

  def findCallData(functionName: String, version: Version): Iterable[FunctionCallWithKnownProvenanceSerializableWithInputs] = Iterable.empty

  def findResultData: Iterable[FunctionCallResultWithKnownProvenanceSerializable] = Iterable.empty

  def findResultData(functionName: String): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = Iterable.empty

  def findResultData(functionName: String, version: Version): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = Iterable.empty

  def findResultDataByOutput(outputDigest: Digest): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = Iterable.empty

  def findUsesOfResultWithIndex(functionName: String, version: Version, resultDigest: Digest): Iterable[(FunctionCallResultWithProvenanceSerializable, Int)] = Iterable.empty
}

class UnavailableData(msg: String) extends RuntimeException(f"Unavailable for ResultTrackerNone: $msg")

