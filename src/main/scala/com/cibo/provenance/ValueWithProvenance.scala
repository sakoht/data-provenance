package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  *
  * ValueWithProvenance[O] is a sealed trait with the following primary implementations:
  * - FunctionCallWithProvenance[O]: a function, its version, and its inputs (also with provenance)
  * - FunctionCallResultWithProvenance[O]: adds a return value to the above signature
  * - UnknownProvenance[O]: a special case of FunctionCallWithProvenance[O] for data w/o history.
  *
  * The type parameter O refers to the return/output type of the function in question.
  *
  * There is an implicit conversion from T -> UnknownProvenance[T] allowing for an
  * entrypoints into the history.
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

import com.cibo.provenance.tracker.ResultTracker

import scala.language.implicitConversions
import scala.reflect.ClassTag


sealed trait ValueWithProvenance[O] extends Serializable {
  def getOutputClassTag: ClassTag[O]
  def isResolved: Boolean
  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]
  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O]
  def deflate(implicit rt: ResultTracker): ValueWithProvenance[O]
  def inflate(implicit rt: ResultTracker): ValueWithProvenance[O]
  def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O]
}


object ValueWithProvenance {
  // Convert any value T to an UnknownProvenance[T] wherever a ValueWithProvenance is expected.
  // This is how "normal" data is passed into FunctionWithProvenance transparently.
  implicit def convertValueWithNoProvenance[T : ClassTag](v: T): ValueWithProvenance[T] =
    UnknownProvenance(v)
}


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

  def run(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O]

  protected[provenance] def newResult(value: Deflatable[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]

  def getVersion: ValueWithProvenance[Version] = Option(version) match {
    case Some(value) => value
    case None => NoVersion // NoVersion breaks some serialization, so we, sadly, allow a null version for internal things.
  }

  def getVersionValue(implicit rt: ResultTracker): Version = getVersion.resolve.getOutputValue

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    rt.resolve(this)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    unresolveInputs(rt)

  protected[provenance]def getNormalizedDigest(implicit rt: ResultTracker): Digest =
    Util.digest(unresolve(rt))

  protected[provenance]def getInputGroupDigest(implicit rt: ResultTracker): Digest =
    Util.digest(getInputDigests(rt))

  protected[provenance]def getInputDigests(implicit rt: ResultTracker): List[String] = {
    getInputs.toList.map {
      input =>
        val resolvedInput = input.resolve
        val inputValue = resolvedInput.getOutputValue
        val id = Util.digest(inputValue)
        val inputValueDigest = id.value
        inputValueDigest
    }
  }

  def getInputsDigestWithSourceFunctionAndVersion(implicit rt: ResultTracker): Vector[CollapsedValue[_]] = {
    val inputs = getInputs.toVector
    inputs.indices.map {
      i => inputs(i).resolve.getCollapsedSummary
    }.toVector
  }

  def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] = resolve.getCollapsedSummary

  def getCallDigest(implicit rt: ResultTracker): Digest = {
    val digests = getInputsDigestWithSourceFunctionAndVersion.map(_.outputDigest).toList
    Digest(Util.digest(digests).value)
  }

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] = rt.saveCall(this)

  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] = this
}


abstract class FunctionCallResultWithProvenance[O](
  provenance: FunctionCallWithProvenance[O],
  output: Deflatable[O],
  outputBuildDetail: BuildInfo
) extends ValueWithProvenance[O] with Serializable {

  def getOutput: Deflatable[O] = output

  def getOutputValueOption: Option[O] = output.valueOption

  def getOutputValue(implicit rt: ResultTracker): O = output.resolveValue(rt).valueOption.get

  def getProvenanceValue: FunctionCallWithProvenance[O]

  def getOutputClassTag: ClassTag[O] = provenance.getOutputClassTag

  def getOutputBuildInfo: BuildInfo = outputBuildDetail

  override def toString: String =
    provenance match {
      case _: UnknownProvenance[O] =>
        // When there is no provenance info, just stringify the data itself.
        getOutputValueOption match {
          case Some(value) => value.toString
          case None =>
            throw new RuntimeException("No outputValue for value with unknown provenance???")
        }
      case f: FunctionCallWithProvenance[O] =>
        getOutputValueOption match {
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
    this.getProvenanceValue.unresolve

  def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] = rt.saveResult(this)

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = this

  def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] = {
    implicit val ct: ClassTag[O] = getOutputClassTag
    val value = getOutputValue
    val digest = Util.digest(value)
    val provenance = getProvenanceValue
    val provenanceClean = provenance.unresolve
    val provenanceCleanDigest: Digest = provenanceClean.getNormalizedDigest
    val provenanceInputDigest = provenance.getInputGroupDigest
    CollapsedCallResult(
      digest,
      provenance.functionName,
      provenance.getVersionValue(rt),
      provenanceCleanDigest,
      provenanceInputDigest
    )
  }
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
  override def getVersion: ValueWithProvenance[Version] = NoVersion

  private lazy val cachedResult = new IdentityResult(this, Deflatable(value).resolveDigest)
  def newResult(o: Deflatable[O])(implicit bi: BuildInfo): IdentityResult[O] = cachedResult

  def duplicate(vv: ValueWithProvenance[Version]): Function0CallWithProvenance[O] =
    new IdentityCall(value)(implicitly[ClassTag[O]])

  private lazy val cachedDigest = Util.digest(value)
  override def getCallDigest(implicit db: ResultTracker): Digest = {
    cachedDigest
  }

  private lazy val cachedCollapsedDigests = Vector(CollapsedIdentityValue(cachedDigest))
  override def getInputsDigestWithSourceFunctionAndVersion(implicit rt: ResultTracker): Vector[CollapsedValue[_]] = {
    cachedCollapsedDigests
  }

  override def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = cachedResult

  override def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] = this

  override def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] =
    cachedResult.getCollapsedSummary(rt)
}


class IdentityResult[O : ClassTag](sig: IdentityCall[O], output: Deflatable[O]) extends Function0CallResultWithProvenance[O](sig, output)(NoBuildInfo) with Serializable {
  private lazy val digest: Digest = Util.digest(output)

  override def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] =
    CollapsedIdentityValue(digest)
}

class IdentityValueForBootstrap[O : ClassTag](value: O) extends ValueWithProvenance[O] with Serializable {
  // This bootstraps the system by wrapping a value as a virtual value.
  // There are two uses: UnknownProvenance[O] and NoVersion.

  val isResolved: Boolean = true

  val getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  private lazy val resultVar: IdentityResult[O] = new IdentityResult(callVar, Deflatable(value))
  def resolve(implicit s: ResultTracker): IdentityResult[O] = resultVar

  private lazy val callVar: IdentityCall[O] = new IdentityCall(value)
  def unresolve(implicit s: ResultTracker): IdentityCall[O] = callVar

  override def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] =
    resultVar.getCollapsedSummary(rt)

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = resolve.inflate(rt)

  def deflate(implicit rt: ResultTracker): IdentityValueForBootstrap[O] = this
}

object NoVersion extends IdentityValueForBootstrap[Version](Version("-")) with Serializable


case class UnknownProvenance[O : ClassTag](value: O) extends IdentityCall[O](value) {
  override def toString: String = f"raw($value)"
  override def newResult(value: Deflatable[O])(implicit bi: BuildInfo): UnknownProvenanceValue[O] = new UnknownProvenanceValue[O](this, value)
}

case class UnknownProvenanceValue[O : ClassTag](prov: UnknownProvenance[O], output: Deflatable[O]) extends IdentityResult[O](prov, output) {
  // This is typically never "seen".
  override def toString: String = f"rawv($output)"
}

case class FunctionCallWithProvenanceDeflated[O](
  functionName: String,
  functionVersion: Version,
  callDigest: Digest
)(implicit ct: ClassTag[O]) extends ValueWithProvenance[O] {

  def getOutputClassTag: ClassTag[O] = ct

  def isResolved: Boolean = false

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.resolve(rt)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflate.unresolve(rt)

  def deflate(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] = this

  def inflate(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflateOption match {
      case Some(value) => value
      case None => throw new RuntimeException(f"Failed to serialized data in $rt for $this")
    }

  def inflateOption(implicit rt: ResultTracker): Option[FunctionCallWithProvenance[O]] =
    rt.loadCallOption[O](functionName, functionVersion, callDigest)

  def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] =
    inflate.resolve(rt).getCollapsedSummary // todo: replace this
}

case class FunctionCallResultWithProvenanceDeflated[O](
  deflatedCall: FunctionCallWithProvenanceDeflated[O],
  outputClassName: String,
  outputDigest: Digest,
  buildInfo: BuildInfo
)(implicit ct: ClassTag[O]) extends ValueWithProvenance[O] {

  def getOutputClassTag: ClassTag[O] = ct

  def isResolved: Boolean =
    true

  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    inflate.resolve(rt)

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    inflate.unresolve(rt)

  def deflate(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] =
    this

  def inflate(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] = {
    val call = deflatedCall.inflate
    val output = rt.loadValue[O](outputDigest)
    call.newResult(Deflatable(output))(buildInfo)
  }


  def getCollapsedSummary(implicit rt: ResultTracker): CollapsedValue[O] =
    inflate.resolve(rt).getCollapsedSummary // todo: replace this
}


object FunctionCallWithProvenanceDeflated {
  lazy val unresolvedVersion = new RuntimeException("Cannot deflate a function call with an unresolved version!")
  def apply[O](call: FunctionCallWithProvenance[O])(implicit rt: ResultTracker): FunctionCallWithProvenanceDeflated[O] = {
    implicit val ct: ClassTag[O] = call.getOutputClassTag
    FunctionCallWithProvenanceDeflated[O](
      functionName = call.functionName,
      functionVersion = call.getVersionValue,
      callDigest = call.getCallDigest
    )
  }
}

object FunctionCallResultWithProvenanceDeflated {
  def apply[O](result: FunctionCallResultWithProvenance[O])(implicit rt: ResultTracker): FunctionCallResultWithProvenanceDeflated[O] = {
    val provenance = result.getProvenanceValue
    implicit val outputClassTag: ClassTag[O] = provenance.getOutputClassTag
    FunctionCallResultWithProvenanceDeflated[O](
      deflatedCall = FunctionCallWithProvenanceDeflated(provenance),
      outputClassName = outputClassTag.runtimeClass.getName,
      outputDigest = result.getOutput.resolveDigest.digestOption.get,
      buildInfo = result.getOutputBuildInfo
    )
  }
}


