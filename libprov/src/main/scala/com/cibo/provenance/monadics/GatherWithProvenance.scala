package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * This is one of the builtin infrastructural FunctionWithProvenance classes.
  *
  * It transforms `S[ ValueWithProvenance[T] ]` into `ValueWithProvenance[S[T]\]`,
  * and preserve history of each underlying value, where S[_] is a highter-kind Trackable.
  *
  * This is usually never used directly by an app.  It is brought in by an implicit
  * conversion, and lets the S[_] of values operate basically as one would intuit as
  * an input to other functions.
  *
  * It functions a bit like `Future.sequence`, which takes a `Seq[Future[T\]\]` and returns a single Future[Seq[T]\].
  * It is basically repackaging things, adding an adaptor layer.
  */

import java.io.Serializable

import scala.language.higherKinds
import scala.language.implicitConversions
import io.circe.{Decoder, Encoder}
import com.cibo.provenance._
import com.cibo.provenance.implicits.Traversable

import scala.concurrent.{ExecutionContext, Future}

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

  // NOTE: The impl() is circumvented by runCall, and is only present for API completeness.
  def impl(inputs: S[E]): S[E] = inputs

  def apply(inputs: S[ValueWithProvenance[E]], v: ValueWithProvenance[Version] = currentVersion): Call =
    Call(inputs, v)

  protected def implVersion(inputs: S[E], v: Version): S[E] = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(inputs)
  }

  protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    // The distinctive logic of this class is here.
    // Circumvent running impl(), and actually resolve all of the members, then create a regular unified result.
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

  case class Call(
    gatheredInputs: S[ValueWithProvenance[E]],
    v: ValueWithProvenance[Version]
  ) extends FunctionCallWithProvenance[S[E]](v) with Serializable {

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

    def inputs: Seq[ValueWithProvenance[E]] = hok.toSeq(gatheredInputs)

    def inputTuple: Tuple1[S[ValueWithProvenance[E]]] = Tuple1(gatheredInputs)

    def resolveInputs(implicit rt: ResultTracker): Call = {
      def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.resolve
      nocopy(duplicate(hok.map(f)(gatheredInputs), v.resolve), this)
    }

    def resolveInputsAsync(implicit rt: ResultTracker, ec: ExecutionContext): Future[Call] = {
      def f(a: ValueWithProvenance[E]): Future[ValueWithProvenance[E]] = a.resolveAsync
      val traversableOfFutures: S[Future[ValueWithProvenance[E]]] = hok.map(f)(gatheredInputs)
      val futureOfTraversable: Future[Seq[ValueWithProvenance[E]]] = Future.sequence(hok.toSeq(traversableOfFutures))
      futureOfTraversable.map {
        gatheredInputsSeq: Seq[ValueWithProvenance[E]] =>
          val gatheredInputsWithHokS: S[ValueWithProvenance[E]] = hok.create(gatheredInputsSeq)
          nocopy(duplicate(gatheredInputsWithHokS, v.resolve), this)
      }
    }

    def unresolveInputs(implicit rt: ResultTracker): Call = {
      def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.unresolve
      nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
    }

    def loadInputs(implicit rt: ResultTracker): Call = {
      def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.load
      nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
    }

    def loadRecurse(implicit rt: ResultTracker): Call = {
      def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.loadRecurse
      nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
    }

    def saveInputs(implicit rt: ResultTracker): Call = {
      def f(a: ValueWithProvenance[E]): ValueWithProvenance[E] = a.save
      nocopy(duplicate(hok.map(f)(gatheredInputs), v.unresolve), this)
    }

    def newResult(output: VirtualValue[S[E]])(implicit bi: BuildInfo): Result =
      Result(this, output)(bi)

    def duplicate(inputs: S[ValueWithProvenance[E]], v: ValueWithProvenance[Version]): Call =
      copy(inputs, v)
  }

  case class Result(override val call: Call, override val outputAsVirtualValue: VirtualValue[S[E]])(implicit bi: BuildInfo)
    extends FunctionCallResultWithProvenance[S[E]](call, outputAsVirtualValue, bi)

  implicit private def convertCall(call: Call): Call =
    Call(call.gatheredInputs, call.version)

  implicit private def convertResult(r: Result): Result =
    Result(r.call, r.outputAsVirtualValue)(r.outputBuildInfoBrief)
}

object GatherWithProvenance {
  // Generate a GatherWithProvenance function for the specific type.
  def apply[S[_], E]()(
    implicit hok: implicits.Traversable[S],
    e: Codec[E],
    cse: Codec[S[E]]
  ): GatherWithProvenance[S, E] = {
    new GatherWithProvenance[S, E]
  }

  // Create a GatherWithProvenance.Call in one step.
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
