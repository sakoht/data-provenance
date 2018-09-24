package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  *
  * The primary class hierarchy in this file:
  *
  *   ValueWithProvenance                             sealed trait
  *     Call                                          sealed trait
  *       FunctionCallWithProvenance                  abstract class with N abstract subclasses
  *         UnknownProvenance                         case class derived from Function0CallWithProvenance
  *     Result                                        sealed trait
  *       FunctionCallResultWithProvenance            abstract class with N abstract subclasses
  *         UnknownProvenanceValue                    case class derived from Function0CallResultWithProvenance
  *
  * A secondary hierarchy allowing for "deflated" objects:
  *
  *   ValueWithProvenance                             sealed trait (above)
  *     ValueWithProvenanceDeflated                   sealed trait
  *       Call                                        sealed trait (above)
  *         FunctionCallWithProvenanceDeflated        case class
  *       Result                                      sealed trait (above)
  *         FunctionCallResultWithProvenanceDeflated  case class
  *
  * Each Function{n}WithProvenance has internal subclasses .Call and .Result, which extend
  * FunctionCall{,Result}WithProvenance, and ensure that the types returned are as specific as possible.
  *
  * The type parameter O refers to the return/output type of the function in question.  Each class above has
  * a single type parameter, though subclasses may have 0-21 I* parameters representing input types.
  *
  * There is an implicit conversion from T -> UnknownProvenance[T] allowing for an
  * entry-points into the history.
  *
  * Inputs to a FunctionWithProvenance are themselves each some ValueWithProvenance, so a call
  * can be arbitrarily composed of other calls, results of other calls, or raw values.
  *
  * Each of the Function* classes has multiple implementations numbered for the input count.
  * Currently only 0-4 have been written, though traditionally scala expects 0-22.
  *
  * Other members of the sealed trait:
  *
  * The *Deflated versions of FunctionCall{,Result}WithProvenance hold only text strings,
  * and can represent parts of the provenance tree in external applications.  The ResultTracker
  * API returns these as it saves.
  *
  * NOTE: Function calls/results that are not deflated and that have an output type that typically
  * supports monadic functions (.map, etc.) have those added by implicits in the companion objects.
  *
  */

import java.io.Serializable

import com.cibo.provenance.implicits.Traversable
import com.cibo.provenance.monadics.GatherWithProvenance

import scala.language.implicitConversions
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import io.circe._

import scala.concurrent.{ExecutionContext, Future}


sealed trait ValueWithProvenance[O] extends Serializable {
  def outputClassTag: ClassTag[O]

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]

  def resolveAsync(implicit rt: ResultTracker, ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]]

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def save(implicit rt: ResultTracker): ValueWithProvenanceDeflated[O]

  def load(implicit rt: ResultTracker): ValueWithProvenance[O]

  def loadRecurse(implicit rt: ResultTracker): ValueWithProvenance[O]

  protected def nocopy[T](newObj: T, prevObj: T): T =
    if (newObj == prevObj)
      prevObj
    else
      newObj

  def resolveAndExtractDigest(implicit rt: ResultTracker): Digest = {
    val call = unresolve(rt)
    val result = resolve(rt)
    val valueDigested = result.outputAsVirtualValue.resolveDigest
    valueDigested.digestOption.get
  }
}


object ValueWithProvenance {
  import com.cibo.provenance.monadics.GatherWithProvenance

  // Convert any value T to an UnknownProvenance[T] wherever a ValueWithProvenance is expected.
  // This is how "normal" data is passed into FunctionWithProvenance transparently.
  implicit def convertValueWithNoProvenance[T: Codec, U <: T](value: U): ValueWithProvenance[T] = {
    UnknownProvenance(value.asInstanceOf[T])
  }

  // Wherever an input Traversable is supplied and all members have provenance tracking,
  // turn it "inside out", giving the whole Traversable provenance tracking,
  // and using those members as inputs.  This wedges in a GatherWithProvenance call.
  // It works with any Seq supported by Traversable.
  implicit def convertTraversableWithMembersWithProvenance[S[_], E](seq: S[ValueWithProvenance[E]])
    (implicit
      rt: ResultTracker,
      hok: Traversable[S],
      ct: ClassTag[E],
      cd: Codec[E],
      cts: ClassTag[S[E]],
      cds: Codec[S[E]]
    ): GatherWithProvenance[S, E]#Call = {
    val gatherer: GatherWithProvenance[S, E] = GatherWithProvenance[S, E]
    val call = gatherer(seq)
    call
  }
}


object FunctionCallWithProvenance {
  import com.cibo.provenance.implicits._

  implicit class OptionCallExt[A](call: FunctionCallWithProvenance[Option[A]])
    (implicit
      cdsa: Codec[Option[A]],
      cda: Codec[A]
    ) extends OptionCall[A](call)(cdsa, cda)

  implicit class TraversableCallExt[S[_], A](call: FunctionCallWithProvenance[S[A]])
    (implicit
      hok: Traversable[S],
      cda: Codec[A],
      cdsa: Codec[S[A]],
      cdsi: Codec[S[Int]]
    ) extends TraversableCall[S, A](call)(hok, cda, cdsa, cdsi)
}


object FunctionCallResultWithProvenance {
  import com.cibo.provenance.implicits._

  implicit class OptionResultExt[A](result: FunctionCallResultWithProvenance[Option[A]])
    (implicit
      cdsa: Codec[Option[A]],
      cda: Codec[A]
    ) extends OptionResult[A](result: FunctionCallResultWithProvenance[Option[A]])(cdsa, cda)

  implicit class TraversableResultExt[S[_], A](result: FunctionCallResultWithProvenance[S[A]])
    (implicit
      hok: Traversable[S],
      cda: Codec[A],
      cdsa: Codec[S[A]],
      cdsi: Codec[S[Int]]
    ) extends TraversableResult[S, A](result)(hok, cda, cdsa, cdsi)
}


// The primary 2 types of value are the Call and Result.

trait Call[O] extends ValueWithProvenance[O] with Serializable

trait Result[O] extends ValueWithProvenance[O] with Serializable


// The regular pair of Call/Result work for normal functions.

abstract class FunctionCallWithProvenance[O : Codec](var vv: ValueWithProvenance[Version]) extends Call[O] with Serializable {
  self =>

  def outputCodec: Codec[O] = implicitly[Codec[O]]

  def outputClassTag: ClassTag[O] = outputCodec.classTag

  def outputTypeTag: TypeTag[O] = outputCodec.typeTag

  // Abstract interface.  These are implemented in each Function{n}CallSignatureWithProvenance subclass.

  def functionName: String

  def inputs: Seq[ValueWithProvenance[_]]

  def resolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def resolveInputsAsync(implicit rt: ResultTracker): Future[FunctionCallWithProvenance[O]] = ???

  def unresolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def saveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def loadInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def loadRecurse(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def run(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]

  protected[provenance] def newResult(value: VirtualValue[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]

  // Common methods.

  def version: ValueWithProvenance[Version] = vv

  def versionValue(implicit rt: ResultTracker): Version = version.resolve.output

  def versionValueAlreadyResolved: Option[Version] = {
    // This returns a version but only if it is already resolved.
    version match {
      case u: UnknownProvenance[Version] => Some(u.value)
      case r: FunctionCallResultWithProvenance[Version] => r.outputAsVirtualValue.valueOption
      case _ => None
    }
  }

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    rt.resolve(this)

  def resolveAsync(implicit rt: ResultTracker, ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]] =
    rt.resolveAsync(this)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    unresolveInputs(rt)

  def getInputGroupValuesDigest(implicit rt: ResultTracker): Digest = {
    val digests = inputs.map(_.resolveAndExtractDigest(rt).id).toList
    Codec.digestObject(digests)
  }

  def save(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] =
    FunctionCallWithProvenanceSerializable.save(this).wrap[O]

  def load(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this
}


abstract class FunctionCallResultWithProvenance[O](
  pCall: FunctionCallWithProvenance[O],
  pOutputAsVirtualValue: VirtualValue[O],
  pOutputBuildInfo: BuildInfo
) extends Result[O] with Serializable {

  def call: FunctionCallWithProvenance[O] = pCall

  def outputAsVirtualValue: VirtualValue[O] = pOutputAsVirtualValue

  def output(implicit rt: ResultTracker): O =
    pOutputAsVirtualValue.resolveValue(rt).valueOption.get

  def outputClassTag: ClassTag[O] = pCall.outputClassTag

  def outputBuildInfo: BuildInfo = pOutputBuildInfo

  def outputBuildInfoBrief: BuildInfoBrief = pOutputBuildInfo.abbreviate

  override def toString: String =
    pCall match {
      case _: UnknownProvenance[O] =>
        // When there is no provenance info, just stringify the data itself.
        outputAsVirtualValue.valueOption match {
          case Some(value) => value.toString
          case None =>
            throw new RuntimeException("No outputValue for value with unknown provenance???")
        }
      case f: FunctionCallWithProvenance[O] =>
        outputAsVirtualValue.valueOption match {
          case Some(outputValue) =>
            f"($outputValue <- ${f.toString})"
          case None =>
            f"(? <- ${f.toString})"
        }
    }

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    this

  def resolveAsync(implicit rt: ResultTracker, ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]] =
    Future.successful(this)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this.call.unresolve

  def save(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    FunctionCallResultWithKnownProvenanceSerializable.save(this).wrap[O]

  def load(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    this

  def loadRecurse(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = {
    val call2 = this.call.loadRecurse(rt)
    if (call2 != call) {
      call2.newResult(outputAsVirtualValue)(outputBuildInfo)
    } else {
      this
    }
  }
}


/**
  * UnknownProvenance represents data with no history.
  *
  * @param value  The value with unknown provenance to be added into tracking.
  * @tparam O     The type of hte value
  */
case class UnknownProvenance[O : Codec](value: O)
  extends Function0CallWithProvenance[O](null)(null) with Serializable {

  // NOTE: The `null` version and implVersion parameters prevent a bootstrapping problem.
  // The version is NoVerison, but that value is itself a value with UnknownProvenance.
  // The null impl/implVersion prevent manufacturing lots of small lambdas to wrap primitives.

  def impl: O = value

  def implVersion(v: Version): O = value

  override def version: ValueWithProvenance[Version] = NoVersionProvenance

  def functionName: String = toString

  @transient
  private lazy implicit val rt: ResultTracker = ResultTrackerNone()(BuildInfoNone)

  @transient
  private lazy val cachedResult: UnknownProvenanceValue[O] =
    UnknownProvenanceValue(this, VirtualValue(value).resolveDigest)

  def newResult(o: VirtualValue[O])(implicit bi: BuildInfo): UnknownProvenanceValue[O] =
    cachedResult

  def duplicate(vv: ValueWithProvenance[Version]): Function0CallWithProvenance[O] =
    UnknownProvenance(value)

  override def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = cachedResult

  override def run(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = cachedResult

  override def unresolve(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def load(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def loadInputs(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def loadRecurse(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def toString: String = f"raw($value)"
}


case class UnknownProvenanceValue[O : Codec](
  override val call: UnknownProvenance[O],
  override val outputAsVirtualValue: VirtualValue[O]
) extends Function0CallResultWithProvenance[O](call, outputAsVirtualValue)(BuildInfoNone) with Serializable {

  // Note: This class is present to complete the API, but nothing in the system instantiates it.
  // The newResult method is never called for an UnknownProvenance[T].

  override def save(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    FunctionCallResultWithUnknownProvenanceSerializable.save(this).wrap[O]

  override def toString: String =
    f"raw($outputAsVirtualValue)"
}

/*
 * "*Saved" equivalents of the call and result are still functional as ValueWithProvenance[O],
 * but require conversion to instantiate fully.
 *
 * This allows history to extends across software made by disconnected libs,
 * and also lets us save an object graph with small, incrementally appended pieces.
 *
 */
sealed trait ValueWithProvenanceDeflated[O] extends ValueWithProvenance[O] with Serializable {
  def id: Digest
}


case class FunctionCallWithProvenanceDeflated[O](data: FunctionCallWithProvenanceSerializable)
  extends ValueWithProvenanceDeflated[O] with Call[O] with Serializable {

  def outputClassTag = ClassTag(Class.forName(data.outputClassName))

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    load.resolve(rt)

  def resolveAsync(implicit rt: ResultTracker, ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]] =
    load.resolveAsync(rt, ec)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    load.unresolve(rt)

  def save(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] =
    this

  def load(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    data.load(rt).asInstanceOf[FunctionCallWithProvenance[O]]

  def loadRecurse(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this.load(rt).loadRecurse(rt)

  @transient
  lazy val id: Digest = data.toDigest
}


case class FunctionCallResultWithProvenanceDeflated[O](data: FunctionCallResultWithProvenanceSerializable)
  extends ValueWithProvenanceDeflated[O] with Result[O] with Serializable {

  def outputDigest: Digest = data.outputDigest

  // Implementation of base API:

  def outputClassTag: ClassTag[O] = ClassTag(Class.forName(data.call.outputClassName))

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    load.resolve(rt)

  def resolveAsync(implicit rt: ResultTracker, ec: ExecutionContext): Future[FunctionCallResultWithProvenance[O]] =
    load.resolveAsync(rt, ec)


  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    load.unresolve(rt)

  def save(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    this

  def load(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    data.load(rt).asInstanceOf[FunctionCallResultWithProvenance[O]]

  def loadRecurse(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    load.loadRecurse(rt)

  @transient
  lazy val id: Digest = data.toDigest
}
