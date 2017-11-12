package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  *
  * The following classes and traits are in this file:
  *
  *  ValueWithProvenance                             sealed trait
  *    Call                                          sealed trait
  *      FunctionCallWithProvenance                  abstract class with N implementations
  *      FunctionCallWithProvenanceDeflated          sealed trait
  *        FunctionCallWithKnownProvenanceDeflated   case class
  *        FunctionCallWithUnknownProvenanceDeflated case class
  *      UnknownProvenance                           case class
  *
  *    Result                                        sealed trait
  *      FunctionResultCallWithProvenance            abstract class with N implementations
  *      FunctionResultCallWithProvenanceDeflated    case class
  *      UnknownProvenanceValue                      case classs
  *
  *    ValueWithProvenanceDeflated                   sealed trait applying to both deflated types above
  *
  *
  * ValueWithProvenance[O] is a sealed trait with the following primary implementations:
  * - FunctionCallWithProvenance[O]: a function, its version, and its inputs (each a ValueWithProvenance[I*], etc)
  * - FunctionCallResultWithProvenance[O]: adds output (the return value) plus the BuildInfo (commit and build metadata)
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
  * NOTE: Function calls/results that are not deflated and that have an output type that typically
  * supports monadic functions (.map, etc.) have those added by implicits in the companion objects.
  *
  */

import com.cibo.provenance.monadics._
import com.cibo.provenance.tracker.{ResultTracker, ResultTrackerNone}

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.language.implicitConversions
import scala.language.higherKinds


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
}

object ValueWithProvenance {
  // Convert any value T to an UnknownProvenance[T] wherever a ValueWithProvenance is expected.
  // This is how "normal" data is passed into FunctionWithProvenance transparently.
  implicit def convertValueWithNoProvenance[T: ClassTag](v: T): ValueWithProvenance[T] =
    UnknownProvenance(v)

  // Convert Seq[ValueWithProvenance[T]] into a ValueWithProvenance[Seq[T]] implicitly.
  implicit def convertSeqWithProvenance[A: ClassTag, S <: Seq[ValueWithProvenance[A]]](seq: S)(implicit rt: ResultTracker): GatherWithProvenance[A, Seq[A], Seq[ValueWithProvenance[A]]]#Call =
    GatherWithProvenance[A].apply(seq)
}

object FunctionCallWithProvenance {

  // Add methods like .map to a FunctionCallWithProvenance[O] where O is S[A], and an implicit Traversable[S] exists.
  implicit class TraversableCall[S[_], A](call: FunctionCallWithProvenance[S[A]])(
    implicit hok: Traversable[S],
    ctsa: ClassTag[S[A]],
    cta: ClassTag[A],
    ctsi: ClassTag[S[Int]]
  ) {
    def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[S, A]#Call =
      ApplyWithProvenance[S, A].apply(call, n)

    def indices: IndicesWithProvenance[S, A]#Call =
      IndicesWithProvenance[S, A].apply(call)

    def map[B](f: Function1WithProvenance[B, A])(implicit ctsb: ClassTag[S[B]], ctb: ClassTag[B]): MapWithProvenance[B, A, S]#Call =
      MapWithProvenance[B, A, S].apply(call, f)

    def scatter(implicit rt: ResultTracker): S[FunctionCallWithProvenance[A]] = {
      val indices: Range = this.indices.resolve.output
      indices.map {
        n => ApplyWithProvenance[S, A].apply(call, n)
      }.asInstanceOf[S[FunctionCallWithProvenance[A]]]
    }
  }
}

object FunctionCallResultWithProvenance {
  implicit class TraversableResult[S[_], A](result: FunctionCallResultWithProvenance[S[A]])(
    implicit hok: Traversable[S],
    ctsa: ClassTag[S[A]],
    cta: ClassTag[A],
    ctsi: ClassTag[S[Int]]
  ) {
    import com.cibo.provenance.FunctionCallWithProvenance.TraversableCall

    def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[S, A]#Call =
      ApplyWithProvenance[S, A].apply(result, n)

    def indices: IndicesWithProvenance[S, A]#Call =
      IndicesWithProvenance[S, A].apply(result)

    def map[B](f: Function1WithProvenance[B, A])(implicit ctsb: ClassTag[S[B]], ctb: ClassTag[B]): MapWithProvenance[B, A, S]#Call =
      new MapWithProvenance[B, A, S].apply(result, f)

    def scatter(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[A]] = {
      val prov: FunctionCallWithProvenance[S[A]] = result.provenance
      val prov2: TraversableCall[S, A] = TraversableCall[S, A](prov)
      val calls: S[FunctionCallWithProvenance[A]] = prov2.scatter
      def getResult(call: FunctionCallWithProvenance[A]): FunctionCallResultWithProvenance[A] = call.resolve
      hok.map(getResult)(calls)
    }
  }
}

// The primary 2 types of value are the Call and Result.

trait Call[O] extends ValueWithProvenance[O] with Serializable {
  val isResolved: Boolean = false
}

trait Result[O] extends ValueWithProvenance[O] with Serializable {
  val isResolved: Boolean = true
}


// The regular pair of Call/Result work for normal functions.

abstract class FunctionCallWithProvenance[O : ClassTag](var version: ValueWithProvenance[Version]) extends Call[O] with Serializable {
  self =>

  lazy val getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  // Abstract interface.  These are implemented in each Function{n}CallSignatureWithProvenance subclass.

  val functionName: String

  val impl: AnyRef // The subclasses are specific Function{N}.

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
) extends Result[O] with Serializable {

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
case class UnknownProvenance[O : ClassTag](value: O) extends Function0CallWithProvenance[O](null)((_) => value) with Serializable {

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
    UnknownProvenance(value)(implicitly[ClassTag[O]])

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
)(implicit ct: ClassTag[O]) extends ValueWithProvenanceDeflated[O] with Result[O] with Serializable {

  def getOutputClassTag: ClassTag[O] = ct

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



