package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  *
  * The following classes and traits are in this file:
  *
  *  ValueWithProvenance                             sealed trait
  *    Call                                          sealed trait
  *      FunctionCallWithProvenance                  abstract class with N abstract subclasses
  *        UnknownProvenance                         case class
  *      FunctionCallWithProvenanceDeflated          sealed trait
  *        FunctionCallWithKnownProvenanceDeflated   case class
  *        FunctionCallWithUnknownProvenanceDeflated case class
  *
  *    Result                                        sealed trait
  *      FunctionCallResultWithProvenance            abstract class with N abstract subclasses
  *        UnknownProvenanceValue                    case class
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

import com.cibo.provenance.exceptions.{UnknownVersionException, UnrunnableVersionException}

import scala.reflect.ClassTag
import scala.language.implicitConversions
import scala.language.higherKinds
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.reflect


sealed trait ValueWithProvenance[O] extends Serializable {
  def getOutputClassTag: ClassTag[O]
  def isResolved: Boolean
  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]
  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O]
  def deflate(implicit rt: ResultTracker): ValueWithProvenanceDeflated[O]
  def inflate(implicit rt: ResultTracker): ValueWithProvenance[O]

  protected def nocopy[T](newObj: T, prevObj: T): T =
    if (newObj == prevObj)
      prevObj
    else
      newObj

  def resolveAndExtractDigest(implicit rt: ResultTracker) = {
    val call = unresolve(rt)
    val result = resolve(rt)
    val valueDigested = result.getOutputVirtual.resolveDigest(call.getEncoder, call.getDecoder)
    valueDigested.digestOption.get
  }
}

object ValueWithProvenance {
  import com.cibo.provenance.monadics.GatherWithProvenance

  // Convert any value T to an UnknownProvenance[T] wherever a ValueWithProvenance is expected.
  // This is how "normal" data is passed into FunctionWithProvenance transparently.
  implicit def convertValueWithNoProvenance[T: ClassTag : Encoder : Decoder](v: T): ValueWithProvenance[T] =
    UnknownProvenance(v)

  // Convert Seq[ValueWithProvenance[T]] into a ValueWithProvenance[Seq[T]] implicitly.
  implicit def convertSeqWithProvenance[A: ClassTag, S <: Seq[ValueWithProvenance[A]]]
    (seq: S)
    (implicit rt: ResultTracker): GatherWithProvenance[A, Seq[A], Seq[ValueWithProvenance[A]]]#Call =
    //GatherWithProvenance[A].apply(seq)
    ???
}


trait FunctionWithProvenance[O] extends Serializable {

  val currentVersion: Version

  lazy val loadableVersions: Seq[Version] = Seq(currentVersion)
  lazy val runnableVersions: Seq[Version] = Seq(currentVersion)

  lazy val loadableVersionSet: Set[Version] = loadableVersions.toSet
  lazy val runnableVersionSet: Set[Version] = runnableVersions.toSet

  def name = getClass.getName.stripSuffix("$")

  override def toString = f"$name@$currentVersion"

  protected def throwInvalidVersionException(v: Version): Unit = {
    if (runnableVersions.contains(v)) {
      throw new RuntimeException(
        f"Version $v of $this is in the runnableVersions list, but implVersion is not overridden to handle it!"
      )
    } else if (loadableVersions.contains(v)) {
      throw UnrunnableVersionException(v, this)
    } else {
      throw UnknownVersionException(v, this)
    }
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
      da: Decoder[A]
    ) extends OptionalCall[A](call)(ctsa, cta, esa, dsa, ea, da)

  implicit class TraversableCallExt[S[_], A](call: FunctionCallWithProvenance[S[A]])
    (implicit
      hok: Traversable[S],
      cta: ClassTag[A],
      ctsa: ClassTag[S[A]],
      ctsi: ClassTag[S[Int]],
      ctr: ClassTag[Range],
      ea: Encoder[A],
      esa: Encoder[S[A]],
      esi: Encoder[S[Int]],
      er: Encoder[Range],
      da: Decoder[A],
      dsa: Decoder[S[A]],
      dsi: Decoder[S[Int]],
      dr: Decoder[Range]
    ) extends TraversableCall[S, A](call)(hok, cta, ctsa, ctsi, ctr, ea, esa, esi, er, da, dsa, dsi, dr)

  implicit def createDecoder[O]: Decoder[FunctionCallWithProvenance[O]] =
    Util.rawDecoder[FunctionCallWithProvenance[O]]

  implicit def createEncoder[O]: Encoder[FunctionCallWithProvenance[O]] =
    Util.rawEncoder[FunctionCallWithProvenance[O]]
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
      da: Decoder[A]
    ) extends OptionalResult[A](result: FunctionCallResultWithProvenance[Option[A]])(ctsa, cta, esa, dsa, ea, da)

  implicit class TraversableResultExt[S[_], A](result: FunctionCallResultWithProvenance[S[A]])
    (implicit
      hok: Traversable[S],
      cta: ClassTag[A],
      ctsa: ClassTag[S[A]],
      ctsi: ClassTag[S[Int]],
      ctr: ClassTag[Range],
      ea: Encoder[A],
      esa: Encoder[S[A]],
      esi: Encoder[S[Int]],
      er: Encoder[Range],
      da: Decoder[A],
      dsa: Decoder[S[A]],
      dsi: Decoder[S[Int]],
      dr: Decoder[Range]
    ) extends TraversableResult[S, A](result)(hok, cta, ctsa, ctsi, ctr, ea, esa, esi, er, da, dsa, dsi, dr)
}

// The primary 2 types of value are the Call and Result.

trait Call[O] extends ValueWithProvenance[O] with Serializable {
  val isResolved: Boolean = false
}

trait Result[O] extends ValueWithProvenance[O] with Serializable {
  val isResolved: Boolean = true
}


// The regular pair of Call/Result work for normal functions.

abstract class FunctionCallWithProvenance[O : ClassTag : io.circe.Encoder : io.circe.Decoder](var version: ValueWithProvenance[Version]) extends Call[O] with Serializable {
  self =>

  @transient
  lazy val getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]
  lazy val getEncoder: io.circe.Encoder[O] = implicitly[io.circe.Encoder[O]]
  lazy val getDecoder: io.circe.Decoder[O] = implicitly[io.circe.Decoder[O]]

  // Abstract interface.  These are implemented in each Function{n}CallSignatureWithProvenance subclass.

  def functionName: String

  def impl: AnyRef // The subclasses are specific Function{N}.

  def getInputs: Seq[ValueWithProvenance[_]]

  def resolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def unresolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def deflateInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def inflateInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def run(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]

  protected[provenance] def newResult(value: VirtualValue[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]

  // Common methods.

  def getVersion: ValueWithProvenance[Version] = version

  def getVersionValue(implicit rt: ResultTracker): Version = getVersion.resolve.output

  def getVersionValueAlreadyResolved: Option[Version] = {
    // This returns a version but only if it is already resolved.
    getVersion match {
      case u: UnknownProvenance[Version] => Some(u.value)
      case r: FunctionCallResultWithProvenance[Version] => r.getOutputVirtual.valueOption
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
    getInputs.toList.map {
      input =>
        val resolvedInput = input.resolve
        type Z = Any
        val inputValue: Z = resolvedInput.output
        implicit val e: Encoder[Z] = resolvedInput.provenance.getEncoder.asInstanceOf[Encoder[Z]]
        implicit val d: Decoder[Z] = resolvedInput.provenance.getDecoder.asInstanceOf[Decoder[Z]]
        val id = Util.digestObject(inputValue)
        val inputValueDigest = id.id
        inputValueDigest
    }
  }

  def getInputsDeflated(implicit rt: ResultTracker): Vector[FunctionCallResultWithProvenanceDeflated[_]] = {
    val inputs = getInputs.toVector
    inputs.indices.map {
      i => inputs(i).resolve.deflate
    }.toVector
  }

  def getInputGroupValuesDigest(implicit rt: ResultTracker): Digest = {
    val digests = getInputs.map(_.resolveAndExtractDigest(rt)).toList
    Util.digestObject(digests)
  }

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] = {
    rt.saveCall(this)
    val version: Version = getVersionValueAlreadyResolved match {
      case Some(version) =>
        version
      case None =>
        throw new RuntimeException("Attempt to save a call with an unresolved version!")
    }
    rt.loadCallDeflatedOption[O](functionName, version, this.getNormalizedDigest) match {
      case Some(deflated) =>
        deflated
      case None =>
        rt.saveCall(this) // consider not
    }
  }


  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this
}


abstract class FunctionCallResultWithProvenance[O](
  call: FunctionCallWithProvenance[O],
  outputVirtual: VirtualValue[O],
  outputBuildInfo: BuildInfo
) extends Result[O] with Serializable {

  def provenance: FunctionCallWithProvenance[O]

  def output(implicit rt: ResultTracker): O =
    outputVirtual.resolveValue(rt, call.getEncoder, call.getDecoder).valueOption.get

  def getOutputVirtual: VirtualValue[O] = outputVirtual

  def getOutputClassTag: ClassTag[O] = call.getOutputClassTag

  def getOutputBuildInfo: BuildInfo = outputBuildInfo

  def getOutputBuildInfoBrief: BuildInfoBrief = outputBuildInfo.abbreviate

  override def toString: String =
    call match {
      case _: UnknownProvenance[O] =>
        // When there is no provenance info, just stringify the data itself.
        getOutputVirtual.valueOption match {
          case Some(value) => value.toString
          case None =>
            throw new RuntimeException("No outputValue for value with unknown provenance???")
        }
      case f: FunctionCallWithProvenance[O] =>
        getOutputVirtual.valueOption match {
          case Some(outputValue) =>
            f"($outputValue <- ${f.toString})"
          case None =>
            f"(? <- ${f.toString})"
        }
    }

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    this

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this.provenance.unresolve

  def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    rt.saveResult(this)

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    this

}

/*
 * UnknownProvenance represents data with no history.
 *
 * Note that `null` version is required in the inheritance constructor to solve a ciruclarity problem.
 * The version is NoVerison, but that value is itself a value with UnknownProvenance.
 */

//scalastyle:off
case class UnknownProvenance[O : ClassTag : Encoder : Decoder](value: O) extends Function0CallWithProvenance[O](null)((_) => value) with Serializable {

  val functionName: String = toString

  // Note: this takes a version of "null" and explicitly sets getVersion to NoVersion.
  // Using NoVersion in the signature directly causes problems with Serialization.
  override def getVersion: ValueWithProvenance[Version] = NoVersionProvenance

  private lazy implicit val rt: ResultTracker = ResultTrackerNone()(NoBuildInfo)


  private lazy val cachedResult: UnknownProvenanceValue[O] =
    UnknownProvenanceValue(this, VirtualValue(value).resolveDigest)

  def newResult(o: VirtualValue[O])(implicit bi: BuildInfo): UnknownProvenanceValue[O] =
    cachedResult

  def duplicate(vv: ValueWithProvenance[Version]): Function0CallWithProvenance[O] =
    UnknownProvenance(value)

  private lazy val cachedDigest = Util.digestObject(value)

  override def getInputGroupValuesDigest(implicit rt: ResultTracker): Digest =
    cachedDigest

  private lazy val cachedCollapsedDigests: Vector[FunctionCallResultWithProvenanceDeflated[O]] =
    Vector(cachedResultDeflated)

  private lazy val cachedResultDeflated = {
    FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = FunctionCallWithKnownProvenanceDeflated[O](
        functionName = functionName,
        functionVersion = getVersionValue,
        inflatedCallDigest = Util.digestObject(this),
        outputClassName = getOutputClassTag.runtimeClass.getName
      ),
      inputGroupDigest = getInputGroupDigest,
      outputDigest = cachedResult.getOutputVirtual.resolveDigest.digestOption.get,
      buildInfo = cachedResult.getOutputBuildInfoBrief
    )
  }

  override def getInputsDeflated(implicit rt: ResultTracker): Vector[FunctionCallResultWithProvenanceDeflated[_]] =
    cachedCollapsedDigests

  override def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = cachedResult

  override def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] = this

  override def toString: String = f"raw($value)"
}


case class UnknownProvenanceValue[O : ClassTag](
  call: UnknownProvenance[O],
  output: VirtualValue[O]
) extends Function0CallResultWithProvenance[O](call, output)(NoBuildInfo) with Serializable {

  // Note: This class is present to complete the API, but nothing in the system instantiates it.
  // The newResult method is never called for an UnknownProvenance[T].

  override def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    FunctionCallResultWithProvenanceDeflated(this)

  override def toString: String = f"rawv($output)"
}

/*
 * "Deflated" equivalents of the call and result are still functional as ValueWithProvenance[O],
 * but require conversion to instantiate fully.
 *
 * This allows history to extends across software made by disconnected libs,
 * and also lets us save an object graph with small incremental pieces.
 *
 */


sealed trait ValueWithProvenanceDeflated[O] extends ValueWithProvenance[O] with Serializable


sealed trait FunctionCallWithProvenanceDeflated[O] extends ValueWithProvenanceDeflated[O] with Call[O] with Serializable {

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.resolve(rt)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflate.unresolve(rt)

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] = this

  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflateOption match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(f"Failed to inflate serialized data in $rt for $this")
    }

  def inflateOption(implicit rt: ResultTracker): Option[FunctionCallWithProvenance[O]]

}

// Sub-divide deflated calls into those for known/unknown provenance.

object FunctionCallWithProvenanceDeflated {

  def apply[O](call: FunctionCallWithProvenance[O])(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] = {
    implicit val outputClassTag: ClassTag[O] = call.getOutputClassTag
    implicit val e: Encoder[O] = call.getEncoder
    implicit val d: Decoder[O] = call.getDecoder
    val outputClassName: String = outputClassTag.runtimeClass.getName
    call match {
      case valueWithUnknownProvenance : UnknownProvenance[O] =>
        FunctionCallWithUnknownProvenanceDeflated[O](
          outputClassName = outputClassName,
          Util.digestObject(valueWithUnknownProvenance.value)
        )
      case _ =>
        FunctionCallWithKnownProvenanceDeflated[O](
          functionName = call.functionName,
          functionVersion = call.getVersionValue,
          inflatedCallDigest = Util.digestObject(call.deflateInputs(rt)),
          outputClassName = outputClassName
        )
    }
  }
}

case class FunctionCallWithKnownProvenanceDeflated[O](
  functionName: String,
  functionVersion: Version,
  outputClassName: String,
  inflatedCallDigest: Digest
)(implicit ct: ClassTag[O], en: Encoder[O], dc: Decoder[O]) extends FunctionCallWithProvenanceDeflated[O] with Serializable {

  def getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  def inflateOption(implicit rt: ResultTracker): Option[FunctionCallWithProvenance[O]] = {
    inflateNoRecurse.map {
      inflated => inflated.inflateInputs(rt)
    }
  }

  def inflateNoRecurse(implicit rt: ResultTracker): Option[FunctionCallWithProvenance[O]] = {
    rt.loadCallOption[O](functionName, functionVersion, inflatedCallDigest)
  }
}

/*
object FunctionCallWithKnownProvenanceDeflated {
  import io.circe._, io.circe.generic.semiauto._

  import io.circe._
  implicit def decoder[O]: Decoder[FunctionCallWithKnownProvenanceDeflated[O]] = Decoder.forProduct1("bytes")(Util.deserializeRaw)
  implicit def encoder[O]: Encoder[FunctionCallWithKnownProvenanceDeflated[O]] = Encoder.forProduct1("bytes") {
    obj => Tuple1(Util.serializeRaw(obj))
  }
  //implicit def fooDecoder[O]: Decoder[FunctionCallWithKnownProvenanceDeflated[O]] = deriveDecoder
  //implicit def fooEncoder[O]: Encoder[FunctionCallWithKnownProvenanceDeflated[O]] = deriveEncoder
}
*/

case class FunctionCallWithUnknownProvenanceDeflated[O : ClassTag : Encoder : Decoder](
  outputClassName: String,
  valueDigest: Digest
) extends FunctionCallWithProvenanceDeflated[O] {

  def getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  def inflateOption(implicit rt: ResultTracker): Option[FunctionCallWithProvenance[O]] =
    rt.loadValueOption[O](valueDigest).map(v => UnknownProvenance(v))
}

// The deflated version of Result has only one implementation.

object FunctionCallResultWithProvenanceDeflated {

  def apply[O](result: FunctionCallResultWithProvenance[O])(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] = {
    val provenance = result.provenance
    implicit val outputClassTag: ClassTag[O] = provenance.getOutputClassTag
    implicit val enc = provenance.getEncoder
    implicit val dec = provenance.getDecoder
    FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = FunctionCallWithProvenanceDeflated(provenance),
      inputGroupDigest = provenance.getInputGroupDigest,
      outputDigest = result.getOutputVirtual.resolveDigest.digestOption.get,
      buildInfo = result.getOutputBuildInfoBrief
    )
  }

  def apply[O : ClassTag : Encoder : Decoder](
    functionName: String,
    functionVersion: Version,
    functionCallDigest: Digest,
    inputGroupDigest: Digest,
    outputDigest: Digest,
    outputClassName: String,
    buildInfo: BuildInfo
  ): FunctionCallResultWithProvenanceDeflated[O] = {

    FunctionCallResultWithProvenanceDeflated(
      deflatedCall = FunctionCallWithKnownProvenanceDeflated[O](
        functionName = functionName,
        functionVersion = functionVersion,
        outputClassName = outputClassName,
        inflatedCallDigest = functionCallDigest
      ),
      inputGroupDigest = inputGroupDigest,
      outputDigest = outputDigest,
      buildInfo = buildInfo
    )
  }
}

case class FunctionCallResultWithProvenanceDeflated[O : reflect.ClassTag : Encoder : Decoder](
  deflatedCall: FunctionCallWithProvenanceDeflated[O],
  inputGroupDigest: Digest,
  outputDigest: Digest,
  buildInfo: BuildInfo
) extends ValueWithProvenanceDeflated[O] with Result[O] with Serializable {

  def getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.resolve(rt)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflate.unresolve(rt)

  def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    this

  def inflate(implicit rt: ResultTracker): ValueWithProvenance[O] = {
    val output = rt.loadValue[O](outputDigest)
    deflatedCall.inflate match {
      case unk: UnknownProvenance[O] =>
        unk
      case call =>
        call.newResult(VirtualValue(output))(buildInfo)
    }
  }
}
