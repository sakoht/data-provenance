package com.cibo.provenance

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

/**
  * This ResultTracker performs all writes and reads to a pair of underlying ResultTrackers,
  * and verifies identical return values.
  *
  * It is used for testing, and can also be used for data migration.
  *
  * @param a: The first underlying tracker.
  * @param b: The second underlying tracker.
  */
class ResultTrackerDuplex[T <: ResultTracker, U <: ResultTracker](val a: T, val b: U) extends ResultTracker {

  def countUnderlying: Int = {
    val aCount = a match {
      case other: ResultTrackerDuplex[_, _] => other.countUnderlying
      case _ => 1
    }
    val bCount = b match {
      case other: ResultTrackerDuplex[_, _] => other.countUnderlying
      case _ => 1
    }
    aCount + bCount
  }

  def loadCallById(callId: Digest): Option[FunctionCallWithProvenanceDeflated[_]] = {
    val aa = Try(a.loadCallById(callId))
    val bb = Try(b.loadCallById(callId))
    cmp(aa, bb, s"loadCallById for $callId returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadResultById(resultId: Digest): Option[FunctionCallResultWithProvenanceDeflated[_]] = {
    val aa = Try(a.loadResultById(resultId))
    val bb = Try(b.loadResultById(resultId))
    cmp(aa, bb, s"loadResultById for $resultId returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def currentAppBuildInfo: BuildInfo = {
    val aa = Try(a.currentAppBuildInfo)
    val bb = Try(b.currentAppBuildInfo)
    cmp(aa, bb, s"currentAppBuildInfo returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def saveCallSerializable[O](callInSerializableForm: FunctionCallWithKnownProvenanceSerializableWithInputs): FunctionCallWithProvenanceDeflated[O] = {
    val aa = Try(a.saveCallSerializable[O](callInSerializableForm))
    val bb = Try(b.saveCallSerializable[O](callInSerializableForm))
    cmp(aa, bb, s"saveCallSerializable for $callInSerializableForm returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def saveResultSerializable(resultInSerializableForm: FunctionCallResultWithKnownProvenanceSerializable, inputResults: Vector[FunctionCallResultWithProvenanceSerializable]): FunctionCallResultWithProvenanceDeflated[_] = {
    import scala.language.existentials
    val aa = Try(a.saveResultSerializable(resultInSerializableForm, inputResults))
    val bb = Try(b.saveResultSerializable(resultInSerializableForm, inputResults))
    require(aa.toOption == bb.toOption, s"saveResultSerializable for $resultInSerializableForm returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def saveOutputValue[T: Codec](obj: T)(implicit cdcd: Codec[Codec[T]]): Digest = {
    val aa = Try(a.saveOutputValue[T](obj))
    val bb = Try(b.saveOutputValue(obj))
    cmp(aa, bb, s"saveOutputValue for $obj returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def saveBuildInfo: Digest = {
    val aa = Try(a.saveBuildInfo)
    val bb = Try(b.saveBuildInfo)
    cmp(aa, bb, s"saveBuildInfo returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def hasOutputForCall[O](call: FunctionCallWithProvenance[O]): Boolean = {
    val aa = Try(a.hasOutputForCall[O](call))
    val bb = Try(b.hasOutputForCall[O](call))
    cmp(aa, bb, s"hasOutputForCall for $call returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def hasValue[T: Codec](obj: T): Boolean = {
    val aa = Try(a.hasValue[T](obj))
    val bb = Try(b.hasValue[T](obj))
    cmp(aa, bb, s"hasValue for $obj returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def hasValue(digest: Digest): Boolean = {
    val aa = Try(a.hasValue(digest))
    val bb = Try(b.hasValue(digest))
    cmp(aa, bb, s"hasValue for $digest returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadResultByCallOption[O](call: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    val aa = Try(a.loadResultByCallOption[O](call))
    val bb = Try(b.loadResultByCallOption[O](call))
    cmp(aa, bb, s"loadResultByCallOption for $call returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadResultByCallOptionAsync[O](call: FunctionCallWithProvenance[O])(implicit ec: ExecutionContext): Future[Option[FunctionCallResultWithProvenance[O]]] = {
    val aa = Try(a.loadResultByCallOptionAsync[O](call))
    val bb = Try(b.loadResultByCallOptionAsync[O](call))
    cmp(aa, bb, s"loadResultByCallOptionAsync for $call returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadValueOption[T: Codec](digest: Digest): Option[T] = {
    val aa = Try(a.loadValueOption(digest))
    val bb = Try(b.loadValueOption(digest))
    cmp(aa, bb, s"loadValueOption for $digest returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadCodecByType[T: ClassTag : universe.TypeTag](implicit cdcd: Codec[Codec[T]]): Codec[T] = {
    val aa = Try(a.loadCodecByType[T])
    val bb = Try(b.loadCodecByType[T])
    cmpSeqCodec(aa.map(v => Seq(v)), bb.map(v => Seq(v)), s"loadCodecByType for ${implicitly[ClassTag[T]]} returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadCodecByClassNameAndCodecDigest[T: ClassTag](valueClassName: String, codecDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Codec[T] = {
    val aa = Try(a.loadCodecByClassNameAndCodecDigest[T](valueClassName, codecDigest))
    val bb = Try(b.loadCodecByClassNameAndCodecDigest[T](valueClassName, codecDigest))
    cmpSeqCodec(aa.map(v => Seq(v)), bb.map(v => Seq(v)), s"loadCodecByClassNameAndCodecDigest for $valueClassName, $codecDigest returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadCodecsByValueDigest[T: ClassTag](valueDigest: Digest)(implicit cdcd: Codec[Codec[T]]): Seq[Codec[T]] = {
    val aa = Try(a.loadCodecsByValueDigest[T](valueDigest))
    val bb = Try(b.loadCodecsByValueDigest[T](valueDigest))
    cmpSeqCodec(aa, aa, s"loadCodecsByValueDigest for $valueDigest returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadBuildInfoOption(commitId: String, buildId: String): Option[BuildInfo] = {
    val aa = Try(a.loadBuildInfoOption(commitId, buildId))
    val bb = Try(b.loadBuildInfoOption(commitId, buildId))
    cmp(aa, bb, s"loadBuildInfoOption for $commitId, $buildId returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def loadValueSerializedDataByClassNameAndDigestOption(className: String, digest: Digest): Option[Array[Byte]] = {
    val aa = Try(a.loadValueSerializedDataByClassNameAndDigestOption(className, digest))
    val bb = Try(b.loadValueSerializedDataByClassNameAndDigestOption(className, digest))
    cmp(aa, bb, s"loadValueSerializedDataByClassNameAndDigestOption for ($className, $digest) returned different values for sync vs. async: $aa vs $bb")
    aa.get
  }

  def findFunctionNames: Iterable[String] = {
    val aa = Try(a.findFunctionNames)
    val bb = Try(b.findFunctionNames)
    cmp(aa, bb, s"findFunctionNames")
    aa.get
  }

  def findFunctionVersions(functionName: String): Iterable[Version] = {
    val aa = Try(a.findFunctionVersions(functionName))
    val bb = Try(b.findFunctionVersions(functionName))
    cmp(aa, bb, s"findFunctionVersions for $functionName")
    aa.get
  }

  def findCalls: Iterable[FunctionCallWithKnownProvenanceSerializableWithoutInputs] = {
    val aa = Try(a.findCalls)
    val bb = Try(b.findCalls)
    cmp(aa, bb, s"findCalls")
    aa.get
  }

  def findCalls(functionName: String): Iterable[FunctionCallWithKnownProvenanceSerializableWithoutInputs] = {
    val aa = Try(a.findCalls(functionName))
    val bb = Try(b.findCalls(functionName))
    cmp(aa, bb, s"findCalls for $functionName")
    aa.get
  }

  def findCalls(functionName: String, version: Version): Iterable[FunctionCallWithKnownProvenanceSerializableWithoutInputs] = {
    val aa = Try(a.findCalls(functionName, version))
    val bb = Try(b.findCalls(functionName, version))
    cmp(aa, bb, s"findCalls for $functionName and $version")
    aa.get
  }

  def findResults: Iterable[FunctionCallResultWithKnownProvenanceSerializable] = {
    val aa = Try(a.findResults)
    val bb = Try(b.findResults)
    cmp(aa, bb, s"findResults")
    aa.get
  }

  def findResults(functionName: String): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = {
    val aa = Try(a.findResults(functionName))
    val bb = Try(b.findResults(functionName))
    cmp(aa, bb, s"findResults for $functionName")
    aa.get
  }

  def findResults(functionName: String, version: Version): Iterable[FunctionCallResultWithKnownProvenanceSerializable] = {
    val aa = Try(a.findResults(functionName, version))
    val bb = Try(b.findResults(functionName, version))
    cmp(aa, bb, s"findResults for $functionName and $version")
    aa.get
  }
  

  // Compare two Try[T] and require that they match.
  // Try[T] does not by default support ==.
  private def cmp[T](a: Try[T], b: Try[T], msg: String): Unit = {
    a match {
      case Success(avalue) =>
        b match {
          case Success(bvalue) =>
            require(avalue == bvalue, msg)
          case Failure(berror) =>
            throw new RuntimeException(s"Result a succeeeded but b failed! $avalue vs $berror")
        }
      case Failure(aerror) =>
        b match {
          case Success(bvalue) =>
            throw new RuntimeException(s"Result a failed but b succeeded! $aerror vs $bvalue")
          case Failure(berror) =>
            require(aerror == berror, msg)
        }
    }
  }

  // Special hndling for a Seq of Codecs.  Expect only the class tags to match.
  private def cmpSeqCodec[T](a: Try[Seq[Codec[T]]], b: Try[Seq[Codec[T]]], msg: String): Unit = {
    a match {
      case Success(avalue) =>
        b match {
          case Success(bvalue) =>
            val act = avalue.map(_.classTag)
            val bct = bvalue.map(_.classTag)
            require(act == bct, msg)
          case Failure(berror) =>
            throw new RuntimeException(s"Result a succeeded but b faied! $avalue vs $berror")
        }
      case Failure(aerror) =>
        b match {
          case Success(bvalue) =>
            throw new RuntimeException(s"Result a failed but b succeeded! $aerror vs $bvalue")
          case Failure(berror) =>
            require(aerror == berror, msg)
        }
    }
  }
}
