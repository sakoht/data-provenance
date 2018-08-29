package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  *  Transform `S[ ValueWithProvenance[T] ]` into `ValueWithProvenance[ S[T] ]`,
  *  and preserve history of each underlying value.
  *
  */

import java.io.Serializable

import scala.language.higherKinds
import scala.language.implicitConversions
import io.circe.{Decoder, Encoder}
import com.cibo.provenance._
import com.cibo.provenance.implicits.Traversable


class GatherWithProvenance[S[_], E](
  implicit hok: Traversable[S],
  cda: Codec[E],
  cdsa: Codec[S[E]]
) extends FunctionWithProvenance[S[E]] with Serializable {
  self =>

  override lazy val typeParameterTypeNames: Seq[String] = {
    Seq(cda, cdsa).map(_.classTag).map(ct => Codec.classTagToSerializableName(ct))
  }

  val currentVersion: Version = NoVersion

  def impl(inputs: S[E]): S[E] = inputs

  def apply(inputs: S[ValueWithProvenance[E]], v: ValueWithProvenance[Version] = currentVersion): Call =
    Call(inputs, v)

  protected def implVersion(inputs: S[E], v: Version): S[E] = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(inputs)
  }

  protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    val inputs: S[ValueWithProvenance[E]] = call.gatheredInputs
    def v2r(a: ValueWithProvenance[E]): FunctionCallResultWithProvenance[E] = a.resolve
    val inputsResolved: S[FunctionCallResultWithProvenance[E]] = hok.map(v2r)(inputs)
    def r2o(a: FunctionCallResultWithProvenance[E]): E = a.output
    val outputs: S[E] = hok.map(r2o)(inputsResolved)
    Result(call, VirtualValue(outputs))(rt.currentAppBuildInfo)
  }

  def deserializeCall(s: FunctionCallWithProvenanceSerializable)(implicit rt: ResultTracker): Call = {
    val serializedTyped = s.asInstanceOf[FunctionCallWithKnownProvenanceSerializableWithInputs]
    val untypedInputs: List[ValueWithProvenanceSerializable] = serializedTyped.inputList
    val typedInputs: List[ValueWithProvenance[E]] = untypedInputs.map(e => e.load.asInstanceOf[ValueWithProvenance[E]])
    val convertedToHigherKind: S[ValueWithProvenance[E]] = hok.create(typedInputs)
    Call(convertedToHigherKind, serializedTyped.functionVersion)
  }

  object Call {
    implicit def encoder(implicit rt: ResultTracker): Encoder[Call] = new ValueWithProvenanceEncoder[S[E], Call]
    implicit def decoder(implicit rt: ResultTracker): Decoder[Call] = new ValueWithProvenanceDecoder[S[E], Call]
  }

  case class Call(in: S[ValueWithProvenance[E]], v: ValueWithProvenance[Version])
    extends GatherCallWithProvenance[S, E](in, v)(implVersion) with Serializable {

    val function: GatherWithProvenance[S, E] = self

    val functionName: String = self.name

    override def toString: String =
      self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString

    override def run(implicit rt: ResultTracker): Result =
      runCall(this)(rt)

    override def resolve(implicit rt: ResultTracker): Result =
      rt.resolve(this).asInstanceOf[Result]

    override def unresolve(implicit rt: ResultTracker): Call =
      unresolveInputs(rt).asInstanceOf[Call]

    def newResult(output: VirtualValue[S[E]])(implicit bi: BuildInfo): Result =
      Result(this, output)(bi)

    def duplicate(inputs: S[ValueWithProvenance[E]], v: ValueWithProvenance[Version]): Call =
      copy(inputs, v)
  }

  case class Result(override val call: Call, override val outputAsVirtualValue: VirtualValue[S[E]])(implicit bi: BuildInfo)
    extends GatherCallResultWithProvenance[S, E](call, outputAsVirtualValue)(bi)

  implicit private def convertCall(call: GatherCallWithProvenance[S, E]): Call =
    Call(call.gatheredInputs, call.version)

  implicit private def convertResult(r: GatherCallResultWithProvenance[S, E]): Result =
    Result(r.call, r.outputAsVirtualValue)(r.outputBuildInfoBrief)
}

object GatherWithProvenance {
  // Factory for type-specific Gatherers
  def apply[S[_], E]()(
    implicit hok: implicits.Traversable[S],
    e: Codec[E],
    cse: Codec[S[E]]
  ): GatherWithProvenance[S, E] = {
    new GatherWithProvenance[S, E]
  }

  // Gather.Call in one step.
  def apply[S[_], E](
    inputs: S[ValueWithProvenance[E]]
  )(implicit hok: implicits.Traversable[S],
    ce: Codec[E],
    cse: Codec[S[E]]
  ): GatherWithProvenance[S, E]#Call = {
    val gatherer = new GatherWithProvenance[S, E]
    gatherer(inputs)
  }
}

abstract class GatherCallWithProvenance[S[_], E](
  val gatheredInputs: S[ValueWithProvenance[E]],
  v: ValueWithProvenance[Version]
)(implVersion: (S[E], Version) => S[E]
)(implicit val hok: implicits.Traversable[S],
  elc: Codec[S[E]],
  ec: Codec[E]
) extends FunctionCallWithProvenance[S[E]](v) with Serializable {

  def inputs: Seq[ValueWithProvenance[E]] = hok.toSeq(gatheredInputs)

  def inputTuple: Tuple1[S[ValueWithProvenance[E]]] = Tuple1(gatheredInputs)

  // NOTE: In most cases these methods are overridden by FunctionNWithProvenance.Call to return
  // the same object, but with a more specific known type.

  def resolveInputs(implicit rt: ResultTracker): GatherCallWithProvenance[S, E] = {
    def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.resolve
    nocopy(duplicate(hok.map(f)(gatheredInputs), v.resolve), this)
  }

  def unresolveInputs(implicit rt: ResultTracker): GatherCallWithProvenance[S, E] = {
    def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.unresolve
    nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
  }

  def loadInputs(implicit rt: ResultTracker): GatherCallWithProvenance[S, E] = {
    def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.load
    nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
  }

  def loadRecurse(implicit rt: ResultTracker): GatherCallWithProvenance[S, E] = {
    def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.loadRecurse
    nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
  }

  def saveInputs(implicit rt: ResultTracker): GatherCallWithProvenance[S, E] = {
    def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.save
    nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
  }

  def run(implicit rt: ResultTracker): GatherCallResultWithProvenance[S, E]

  def newResult(value: VirtualValue[S[E]])(implicit bi: BuildInfo): GatherCallResultWithProvenance[S, E]

  // NOTE: This is implemented as Product.copy in the case-class subclasses.
  def duplicate(inputs: S[ValueWithProvenance[E]] = gatheredInputs, v: ValueWithProvenance[Version] = v): GatherCallWithProvenance[S, E]
}


class GatherCallResultWithProvenance[S[_], E](
  override val call: GatherCallWithProvenance[S, E],
  override val outputAsVirtualValue: VirtualValue[S[E]]
)(implicit bi: BuildInfo)
  extends FunctionCallResultWithProvenance[S[E]](call, outputAsVirtualValue, bi)
    with Serializable
