package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  *
  * ValueWithProvenance[O] is a sealed trait with the following primary implementations:
  * - FunctionCallWithProvenance[O]: a function, its version, and its inputs (each a ValueWithProvenance[I*], etc)
  * - FunctionCallResultWithProvenance[O]: adds a return value to the above signature
  * - UnknownProvenance[O]: a special case of FunctionCallWithProvenance[O] for data w/o history.
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
  * The IdentityCall and IdentityResult classes are behind UnknownProvenance and NoVersion,
  * and are required to prevent circular deps and boostrap the system.
  *
  */

import com.cibo.provenance.mappable.GatherWithProvenance
import com.cibo.provenance.tracker.{ResultTracker, ResultTrackerNone}

import scala.collection.immutable
import scala.language.implicitConversions
import scala.reflect.ClassTag


sealed trait ValueWithProvenance[O] extends Serializable {
  def getOutputClassTag: ClassTag[O]
  def isResolved: Boolean
  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]
  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O]
  def deflate(implicit rt: ResultTracker): ValueWithProvenance[O]
  def inflate(implicit rt: ResultTracker): ValueWithProvenance[O]

  protected def nocopy[T](newObj: T, prevObj: T): T =
    if (newObj == prevObj)
      prevObj
    else
      newObj
}


object ValueWithProvenance {

  // Convert any value T to an UnknownProvenance[T] wherever a ValueWithProvenance is expected.
  // This is how "normal" data is passed into FunctionWithProvenance transparently.
  implicit def convertValueWithNoProvenance[T : ClassTag](v: T): ValueWithProvenance[T] =
    UnknownProvenance(v)

  // Convert Seq[ValueWithProvenance[T]] into a ValueWithProvenance[Seq[T]] implicitly.
  implicit def convertSeqWithProvenance[E : ClassTag, S <: Seq[ValueWithProvenance[E]]](seq: S)(implicit rt: ResultTracker): GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]#Call = {
    val gather: GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]#Call = GatherWithProvenance[E].apply(seq)
    gather
  }

  // Add methods to a Call where the output is a Seq.
  implicit class MappableCall[E: ClassTag](seq: FunctionCallWithProvenance[Seq[E]]) {
    import com.cibo.provenance.mappable._

    def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[E]#Call = {
      ApplyWithProvenance[E](seq, n)
    }

    // TODO: make map work w/o scatter, and be sure that chained .map calls do not gather in-between.
  }

  // Add methods to a Result where the output is a Seq.
  implicit class MappableResult[E: ClassTag](seq: FunctionCallResultWithProvenance[Seq[E]]) {
    import com.cibo.provenance.mappable._

    def scatter(implicit rt: ResultTracker): Seq[ApplyWithProvenance[E]#Result] = {
      seq.output.indices.map {
        n => ApplyWithProvenance[E](seq, n).resolve
      }
    }

    def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[E]#Call = {
      ApplyWithProvenance[E](seq, n)
    }

    def map[E2: ClassTag](f: Function1WithProvenance[E2, E])(implicit rt: ResultTracker): Seq[f.Call] =
      seq.scatter.map(e => f(e))
  }
}


// The primary 2 types of value are the Call and Result.

abstract class FunctionCallWithProvenance[O : ClassTag](var version: ValueWithProvenance[Version]) extends ValueWithProvenance[O] with Serializable {
  self =>

  val isResolved: Boolean = false

  lazy val getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  /*
   * Abstract interface.  These are implemented by Function{n}CallSignatureWithProvenance.
   */

  val functionName: String

  // The subclasses are specific Function{N}.
  val impl: AnyRef

  def getInputs: Seq[ValueWithProvenance[_]]

  def resolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def unresolveInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def deflateInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def inflateInputs(implicit rt: ResultTracker): FunctionCallWithProvenance[O]

  def run(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]

  protected[provenance] def newResult(value: VirtualValue[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]

  def getVersion: ValueWithProvenance[Version] = Option(version) match {
    case Some(value) => value
    case None => NoVersionProvenance // NoVersion breaks some serialization, so we, sadly, allow a null version for internal things.
  }

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

  protected[provenance]def getNormalizedDigest(implicit rt: ResultTracker): Digest =
    Util.digestObject(unresolve(rt))

  protected[provenance]def getInputGroupDigest(implicit rt: ResultTracker): Digest =
    Util.digestObject(getInputDigests(rt))

  protected[provenance]def getInputDigests(implicit rt: ResultTracker): List[String] = {
    getInputs.toList.map {
      input =>
        val resolvedInput = input.resolve
        val inputValue = resolvedInput.output
        val id = Util.digestObject(inputValue)
        val inputValueDigest = id.id
        inputValueDigest
    }
  }

  def getInputsDigestWithSourceFunctionAndVersion(implicit rt: ResultTracker): Vector[FunctionCallResultWithProvenanceDeflated[_]] = {
    val inputs = getInputs.toVector
    inputs.indices.map {
      i => inputs(i).resolve.deflate
    }.toVector
  }

  def getInputGroupValuesDigest(implicit rt: ResultTracker): Digest = {
    val inputsDeflated: immutable.Seq[FunctionCallResultWithProvenanceDeflated[_]] = getInputsDigestWithSourceFunctionAndVersion
    val digests = inputsDeflated.map(_.outputDigest).toList
    Digest(Util.digestObject(digests).id)
  }

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] =
    rt.saveCall(this)

  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    this
}


abstract class FunctionCallResultWithProvenance[O](

  call: FunctionCallWithProvenance[O],
  outputVirtual: VirtualValue[O],
  outputBuildInfo: BuildInfo
) extends ValueWithProvenance[O] with Serializable {

  def provenance: FunctionCallWithProvenance[O]

  def output(implicit rt: ResultTracker): O = outputVirtual.resolveValue(rt).valueOption.get


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

  val isResolved: Boolean = true

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
 * IdentityCall and IdentityResult are used to bootstrap parts of the system (UnknownProvenance and NoVersion).
 * They must be in this source file because to be part of the VirtualValue sealed trait.
 * They use a null to avoid circulatrity/serialization problems with the NoVersion Version itself being an IdentityValue.
 *
 */

//scalastyle:off
class IdentityCall[O : ClassTag](value: O) extends Function0CallWithProvenance[O](null)((_) => value) with Serializable {
  val functionName: String = toString

  // Note: this takes a version of "null" and explicitly sets getVersion to NoVersion.
  // Using NoVersion in the signature directly causes problems with Serialization.
  override def getVersion: ValueWithProvenance[Version] = NoVersionProvenance

  private lazy implicit val rt: ResultTracker = ResultTrackerNone()(NoBuildInfo)


  private lazy val cachedResult: IdentityResult[O] = new IdentityResult(this, VirtualValue(value).resolveDigest)

  def newResult(o: VirtualValue[O])(implicit bi: BuildInfo): IdentityResult[O] =
    cachedResult

  def duplicate(vv: ValueWithProvenance[Version]): Function0CallWithProvenance[O] =
    new IdentityCall(value)(implicitly[ClassTag[O]])

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

  override def getInputsDigestWithSourceFunctionAndVersion(implicit rt: ResultTracker): Vector[FunctionCallResultWithProvenanceDeflated[_]] =
    cachedCollapsedDigests

  override def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = cachedResult

  override def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] = this

}


class IdentityResult[O : ClassTag](
  call: IdentityCall[O],
  output: VirtualValue[O]
) extends Function0CallResultWithProvenance[O](call, output)(NoBuildInfo) with Serializable {
  override def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    FunctionCallResultWithProvenanceDeflated(this)
}


class IdentityValueForBootstrap[O : ClassTag](v: O) extends ValueWithProvenance[O] with Serializable {
  // This bootstraps the system by wrapping a value as a virtual value.
  // There are two uses: UnknownProvenance[O] and NoVersion.

  def value: O = v

  val isResolved: Boolean = true

  val getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  private lazy val resultVar: IdentityResult[O] = new IdentityResult(callVar, VirtualValue(v))
  def resolve(implicit s: ResultTracker): IdentityResult[O] = resultVar

  private lazy val callVar: IdentityCall[O] = new IdentityCall(v)
  def unresolve(implicit s: ResultTracker): IdentityCall[O] = callVar

  override def deflate(implicit rt: ResultTracker): ValueWithProvenanceDeflated[O] =
    resultVar.deflate(rt)

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = this.resolve
}


/*
 * UnknownProvenance[T] represents raw values that lack provenance tracking.
 *
 */

case class UnknownProvenance[O : ClassTag](value: O) extends IdentityCall[O](value) {

  override def toString: String = f"raw($value)"

  override def newResult(value: VirtualValue[O])(implicit bi: BuildInfo): UnknownProvenanceValue[O] =
    new UnknownProvenanceValue[O](this, value)

}


case class UnknownProvenanceValue[O : ClassTag](prov: UnknownProvenance[O], output: VirtualValue[O]) extends IdentityResult[O](prov, output) {
  // Note: This class is present to complete the API, but nothing in the system instantiates it.
  // THe newResult method is never called for an UnknownProvenance[T].
  override def toString: String = f"rawv($output)"
}


/*
 * The second type of IdentityValue is used to represent a null version number.
 *
 */

object NoVersion extends Version("-") with Serializable

object NoVersionProvenance extends IdentityValueForBootstrap[Version](NoVersion) with Serializable


/*
 * "Deflated" equivalents of the call and result are still functional as ValueWithProvenance[O],
 * but require conversion to instantiate fully.
 *
 * This allows history to extends across software made by disconnected libs,
 * and also lets us save an object graph with small incremental pieces.
 *
 */


sealed trait ValueWithProvenanceDeflated[O] extends ValueWithProvenance[O] with Serializable


sealed trait FunctionCallWithProvenanceDeflated[O] extends ValueWithProvenanceDeflated[O] with Serializable {

  def isResolved: Boolean = false

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
)(implicit ct: ClassTag[O]) extends FunctionCallWithProvenanceDeflated[O] with Serializable {

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

case class FunctionCallWithUnknownProvenanceDeflated[O : ClassTag](
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
    FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = FunctionCallWithProvenanceDeflated(provenance),
      inputGroupDigest = provenance.getInputGroupDigest,
      outputDigest = result.getOutputVirtual.resolveDigest.digestOption.get,
      buildInfo = result.getOutputBuildInfoBrief
    )
  }

  def apply[O : ClassTag](
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

case class FunctionCallResultWithProvenanceDeflated[O](
  deflatedCall: FunctionCallWithProvenanceDeflated[O],
  inputGroupDigest: Digest,
  outputDigest: Digest,
  buildInfo: BuildInfo
)(implicit ct: ClassTag[O]) extends ValueWithProvenanceDeflated[O] with Serializable {

  def getOutputClassTag: ClassTag[O] = ct

  def isResolved: Boolean =
    true

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

