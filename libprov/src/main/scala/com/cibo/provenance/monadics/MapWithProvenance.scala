package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `map(A=>B)` on a Traversable.
  *
  */

import scala.language.higherKinds
import scala.language.existentials

import com.cibo.provenance._

class MapWithProvenance[S[_], A, B](
  implicit hok: implicits.Traversable[S],
  cdb: Codec[B],
  cda: Codec[A],
  cdsb: Codec[S[B]],
  cdsa: Codec[S[A]],
  cdsi: Codec[S[Int]]
) extends Function2WithProvenance[S[A], Function1WithProvenance[A, B], S[B]] {

  val currentVersion: Version = NoVersion

  override lazy val typeParameterTypeNames: Seq[String] = {
    Seq(cdsa, cda, cdb).map(_.getClassTag).map(ct => Codec.classTagToSerializableName(ct))
  }

  override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    // Skip the bulk impl() call and construct the output result from the individual calls.
    val individualResults: S[FunctionCallResultWithProvenance[B]] = runOnEach(call)(rt)
    val unifiedOutputs: S[B] = hok.map((r: FunctionCallResultWithProvenance[B]) => r.output)(individualResults)
    call.newResult(VirtualValue(unifiedOutputs))(rt.currentAppBuildInfo)
  }

  private def runOnEach(call: Call)(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[B]] = {
    val aResolved: FunctionCallResultWithProvenance[S[A]] = call.i1.resolve.asInstanceOf[FunctionCallResultWithProvenance[S[A]]]
    val aTraversable =
      FunctionCallResultWithProvenance.TraversableResultExt[S, A](aResolved)(hok, cda, cdsa, cdsi)
    val aGranular: S[FunctionCallResultWithProvenance[A]] =
      aTraversable.scatter

    val funcResolved: FunctionCallResultWithProvenance[_ <: Function1WithProvenance[A, B]] = call.i2.resolve(rt)
    val func: Function1WithProvenance[A, B] = funcResolved.output
    def a2b(a: FunctionCallResultWithProvenance[A]): FunctionCallResultWithProvenance[B] = func(a).resolve(rt)
    val bGranular: S[FunctionCallResultWithProvenance[B]] = hok.map(a2b)(aGranular)
    bGranular
  }

  def impl(s: S[A], f: Function1WithProvenance[A, B]): S[B] =
    // The runCall method circumvents actually calling impl, but composes the same output as this function.
    // This is provided for completeness since `.impl` is externally exposed, and should be testable.
    hok.map(f.impl)(s)
}

object MapWithProvenance {
  def apply[S[_], A, B](
    implicit hok: implicits.Traversable[S],
    cdb: Codec[B],
    cda: Codec[A],
    cdsb: Codec[S[B]],
    cdsa: Codec[S[A]],
    cdsi: Codec[S[Int]]
  ) = new MapWithProvenance[S, A, B]()(hok, cdb, cda, cdsb, cdsa, cdsi)
}
