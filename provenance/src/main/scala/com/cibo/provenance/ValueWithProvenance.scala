package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  *
  * The following classes and traits are in this file:
  *
  *  ValueWithProvenance                             sealed trait
  *    Call                                          sealed trait
  *      FunctionCallWithProvenance                  abstract class with N abstract subclasses
  *        UnknownProvenance                         case class derived from Function0CallWithProvenance
  *      FunctionCallWithProvenanceDeflated          sealed trait
  *        FunctionCallWithKnownProvenanceDeflated   case class
  *        FunctionCallWithUnknownProvenanceDeflated case class
  *
  *    Result                                        sealed trait
  *      FunctionCallResultWithProvenance            abstract class with N abstract subclasses
  *        UnknownProvenanceValue                    case class derived from Function0CallResultWithProvenance
  *      FunctionCallResultWithProvenanceDeflated    case class
  *
  *    ValueWithProvenanceDeflated                   sealed trait applying to both deflated types above
  *
  * Each Function{n}WithProvenance has internal subclasses .Call and .Result, which extend
  * FunctionCall{,Result}WithProvenance, and ensure that the types returned are as specific as possible.
  *
  *
  * ValueWithProvenance[O] is a sealed trait with the following primary implementations:
  * - FunctionCallWithProvenance[O]: a function, its version, and its inputs (each a ValueWithProvenance[I*], etc)
  * - FunctionCallResultWithProvenance[O]: adds output (the return value) plus the BuildInfo (commit and build metadata)
  * - UnknownProvenance[O]: a special case of Function0CallWithProvenance[O] for data w/o history.
  *
  * The type parameter O refers to the return/output type of the function in question.
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

import scala.reflect.ClassTag
import scala.language.implicitConversions
import scala.language.higherKinds
import io.circe._
import io.circe.generic.auto._


sealed trait ValueWithProvenance[O] extends Serializable {
  def outputClassTag: ClassTag[O]
  def isResolved: Boolean
  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]
  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O]
  def deflate(implicit rt: ResultTracker): ValueWithProvenanceSaved[O]
  def inflate(implicit rt: ResultTracker): ValueWithProvenance[O]
  def inflateRecurse(implicit rt: ResultTracker): ValueWithProvenance[O]

  protected def nocopy[T](newObj: T, prevObj: T): T =
    if (newObj == prevObj)
      prevObj
    else
      newObj

  def resolveAndExtractDigest(implicit rt: ResultTracker): Digest = {
    val call = unresolve(rt)
    val result = resolve(rt)
    val valueDigested = result.outputAsVirtualValue.resolveDigest(call.outputEncoder, call.outputDecoder)
    valueDigested.digestOption.get
  }
}


object ValueWithProvenance {
  import com.cibo.provenance.monadics.GatherWithProvenance

  // Convert any value T to an UnknownProvenance[T] wherever a ValueWithProvenance is expected.
  // This is how "normal" data is passed into FunctionWithProvenance transparently.
  implicit def convertValueWithNoProvenance[T: ClassTag : Encoder : Decoder, U <: T](value: U): ValueWithProvenance[T] = {
    UnknownProvenance(value.asInstanceOf[T])
  }

  implicit def convertSeqWithProvenance[A](seq: Seq[ValueWithProvenance[A]])
    (implicit
      rt: ResultTracker,
      ct: ClassTag[Seq[A]],
      en: Encoder[Seq[A]],
      dc: Decoder[Seq[A]],
      en2: Encoder[Seq[ValueWithProvenance[A]]]
    ): GatherWithProvenance[A, Seq[A], Seq[ValueWithProvenance[A]]]#Call = {
    val gatherer: GatherWithProvenance[A, Seq[A], Seq[ValueWithProvenance[A]]] = GatherWithProvenance[A]
    val call: gatherer.Call = gatherer(seq)
    call
  }
}


object FunctionCallWithProvenance {
  import com.cibo.provenance.implicits._

  implicit class OptionalCallExt[A](call: FunctionCallWithProvenance[Option[A]])
    (implicit
      ctsa: ClassTag[Option[A]],
      cta: ClassTag[A],
      esa: Encoder[Option[A]],
      dsa: Decoder[Option[A]],
      ea: Encoder[A],
      da: Decoder[A],
      ca: Codec[A]
    ) extends OptionalCall[A](call)(ctsa, cta, esa, dsa, ea, da, ca)

  implicit class TraversableCallExt[S[_], A](call: FunctionCallWithProvenance[S[A]])
    (implicit
      hok: Traversable[S],
      cta: ClassTag[A],
      ctsa: ClassTag[S[A]],
      ctsi: ClassTag[S[Int]],
      ea: Encoder[A],
      esa: Encoder[S[A]],
      esi: Encoder[S[Int]],
      da: Decoder[A],
      dsa: Decoder[S[A]],
      dsi: Decoder[S[Int]],
      ca: Codec[A],
      csa: Codec[S[A]],
      csi: Codec[S[Int]]
    ) extends TraversableCall[S, A](call)(hok, cta, ctsa, ctsi, ea, esa, esi, da, dsa, dsi, ca, csa, csi)


  implicit def createDecoder[O]: Decoder[FunctionCallWithProvenance[O]] =
    ??? //new BinaryDecoder[FunctionCallWithProvenance[O]]

  implicit def createEncoder[O]: Encoder[FunctionCallWithProvenance[O]] =
    ??? //new BinaryEncoder[FunctionCallWithProvenance[O]]
}


object FunctionCallResultWithProvenance {
  import com.cibo.provenance.implicits._

  implicit class OptionalResultExt[A](result: FunctionCallResultWithProvenance[Option[A]])
    (implicit
      ctsa: ClassTag[Option[A]],
      cta: ClassTag[A],
      esa: Encoder[Option[A]],
      dsa: Decoder[Option[A]],
      ea: Encoder[A],
      da: Decoder[A],
      ca: Codec[A]
    ) extends OptionalResult[A](result: FunctionCallResultWithProvenance[Option[A]])(ctsa, cta, esa, dsa, ea, da, ca)

  implicit class TraversableResultExt[S[_], A](result: FunctionCallResultWithProvenance[S[A]])
    (implicit
      hok: Traversable[S],
      cta: ClassTag[A],
      ctsa: ClassTag[S[A]],
      ctsi: ClassTag[S[Int]],
      ea: Encoder[A],
      esa: Encoder[S[A]],
      esi: Encoder[S[Int]],
      da: Decoder[A],
      dsa: Decoder[S[A]],
      dsi: Decoder[S[Int]],
      ca: Codec[A],
      csa: Codec[S[A]],
      csi: Codec[S[Int]]
    ) extends TraversableResult[S, A](result)(hok, cta, ctsa, ctsi, ea, esa, esi, da, dsa, dsi, ca, csa, csi)
}


// The primary 2 types of value are the Call and Result.

trait Call[O] extends ValueWithProvenance[O] with Serializable {
  val isResolved: Boolean = false
}

trait Result[O] extends ValueWithProvenance[O] with Serializable {
  val isResolved: Boolean = true
}


// The regular pair of Call/Result work for normal functions.

abstract class FunctionCallWithProvenance[O : ClassTag : io.circe.Encoder : io.circe.Decoder](var vv: ValueWithProvenance[Version]) extends Call[O] with Serializable {
  self =>

  def outputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  def outputEncoder: io.circe.Encoder[O] = implicitly[io.circe.Encoder[O]]

  def outputDecoder: io.circe.Decoder[O] = implicitly[io.circe.Decoder[O]]

  // Abstract interface.  These are implemented in each Function{n}CallSignatureWithProvenance subclass.

  def functionName: String

  def inputs: Seq[ValueWithProvenance[_]]

  def resolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def unresolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def deflateInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def inflateInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def inflateRecurse(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

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

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    unresolveInputs(rt)

  protected[provenance] def getNormalizedDigest(implicit rt: ResultTracker): Digest =
    Util.digestObject(unresolve(rt))

  protected[provenance] def getInputGroupDigest(implicit rt: ResultTracker): Digest =
    Util.digestObject(getInputDigests(rt))

  protected[provenance] def getInputDigests(implicit rt: ResultTracker): List[String] = {
    inputs.toList.map {
      input =>
        val resolvedInput = input.resolve
        type Z = Any
        val inputValue: Z = resolvedInput.output
        implicit val e: Encoder[Z] = resolvedInput.call.outputEncoder.asInstanceOf[Encoder[Z]]
        implicit val d: Decoder[Z] = resolvedInput.call.outputDecoder.asInstanceOf[Decoder[Z]]
        val id = Util.digestObject(inputValue)
        val inputValueDigest = id.id
        inputValueDigest
    }
  }

  def getInputsDeflated(implicit rt: ResultTracker): Vector[FunctionCallResultWithProvenanceSaved[_]] = {
    val inputSeq = inputs.toVector
    inputSeq.indices.map {
      i => inputSeq(i).resolve.deflate
    }.toVector
  }

  def getInputGroupValuesDigest(implicit rt: ResultTracker): Digest = {
    val digests = inputs.map(_.resolveAndExtractDigest(rt).id).toList
    Util.digestObject(digests)
  }

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceSaved[O] =
    FunctionCallWithProvenanceSaved(FunctionCallWithProvenanceSerializable.save(this))

  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
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
    pOutputAsVirtualValue.resolveValue(rt, pCall.outputEncoder, pCall.outputDecoder).valueOption.get

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

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this.call.unresolve

  def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceSaved[O] =
    FunctionCallResultWithProvenanceSaved(FunctionCallResultWithKnownProvenanceSerializable.save(this))

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    this

  def inflateRecurse(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = {
    val call2 = this.call.inflateRecurse(rt)
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
case class UnknownProvenance[O : ClassTag : Encoder : Decoder](value: O)
  extends Function0CallWithProvenance[O](null)(null) with Serializable {

  // NOTE: The `null` version and implVersion parameters prevent a bootstrapping problem.
  // The version is NoVerison, but that value is itself a value with UnknownProvenance.
  // The null impl/implVersion prevent manufacturing lots of small lambdas to wrap primitives.

  def impl: O = value

  def implVersion(v: Version): O = value

  override def version: ValueWithProvenance[Version] = NoVersionProvenance

  def functionName: String = toString

  @transient
  private lazy implicit val rt: ResultTracker = ResultTrackerNone()(NoBuildInfo)

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

  override def inflate(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def inflateInputs(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def inflateRecurse(implicit rt: ResultTracker): UnknownProvenance[O] = this

  override def toString: String = f"raw($value)"
}


case class UnknownProvenanceValue[O : ClassTag](
  override val call: UnknownProvenance[O],
  override val outputAsVirtualValue: VirtualValue[O]
) extends Function0CallResultWithProvenance[O](call, outputAsVirtualValue)(NoBuildInfo) with Serializable {

  // Note: This class is present to complete the API, but nothing in the system instantiates it.
  // The newResult method is never called for an UnknownProvenance[T].

  override def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceSaved[O] =
    FunctionCallResultWithProvenanceSaved(FunctionCallResultWithKnownProvenanceSerializable.save(this))

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
sealed trait ValueWithProvenanceSaved[O] extends ValueWithProvenance[O] with Serializable


case class FunctionCallWithProvenanceSaved[O](data: FunctionCallWithProvenanceSerializable)
  extends ValueWithProvenanceSaved[O] with Call[O] with Serializable {

  def outputClassTag = ClassTag(Class.forName(data.outputClassName))

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.resolve(rt)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflate.unresolve(rt)

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceSaved[O] =
    this

  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    data.load(rt).asInstanceOf[FunctionCallWithProvenance[O]]

  def inflateRecurse(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this.inflate(rt).inflateRecurse(rt)
}


case class FunctionCallResultWithProvenanceSaved[O](data: FunctionCallResultWithKnownProvenanceSerializable)
  extends ValueWithProvenanceSaved[O] with Result[O] with Serializable {

  def outputDigest: Digest = data.outputDigest

  // Implementation of base API:

  def outputClassTag: ClassTag[O] = ClassTag(Class.forName(data.call.outputClassName))

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.resolve(rt)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflate.unresolve(rt)

  def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceSaved[O] =
    this

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    data.load(rt).asInstanceOf[FunctionCallResultWithProvenance[O]]

  def inflateRecurse(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.inflateRecurse(rt)
}
