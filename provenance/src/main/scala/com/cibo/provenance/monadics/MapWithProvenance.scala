package com.cibo.provenance.monadics

import scala.language.higherKinds
import scala.reflect.ClassTag
import com.cibo.provenance._
import com.cibo.provenance.tracker.ResultTracker

class MapWithProvenance[B, A, S[_]](implicit hok: Traversable[S], ctsb: ClassTag[S[B]], cta: ClassTag[A], ctb: ClassTag[B])
  extends Function2WithProvenance[S[B], S[A], Function1WithProvenance[B, A]] {

  val currentVersion: NoVersion.type = NoVersion

  override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    // Skip the bulk impl() call and construct the output result from the individual calls.
    val individualResults: S[FunctionCallResultWithProvenance[B]] = runOnEach(call)(rt)
    val unified: S[B] = hok.map((r: FunctionCallResultWithProvenance[B]) => r.output)(individualResults)
    call.newResult(VirtualValue(unified))(rt.getCurrentBuildInfo)
  }

  def impl(s: S[A], f: Function1WithProvenance[B, A]): S[B] =
    // So runCall circumvents actually calling impl, but composes the same output as this fucntion,
    // but also generates results with provenance for each element.
    // This is provided for completeness since `.impl` is externally exposed.
    hok.map(f.impl)(s)

  protected def runOnEach(call: Call)(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[B]] = {
    val inputValuesWithProvenance: FunctionCallResultWithProvenance[S[A]] = call.v1.resolve
    val funcResult: FunctionCallResultWithProvenance[Function1WithProvenance[B, A]] = call.v2.resolve

    val inputValues: S[A] = inputValuesWithProvenance.output
    val func: Function1WithProvenance[B, A] = funcResult.output

    val indices: Range = hok.indices(inputValues)
    indices.map {
      n =>
        val callToGetA = ApplyWithProvenance[S, A].apply(inputValuesWithProvenance, n)
        val resultA = callToGetA.resolve(rt)
        val resultB = func(resultA).resolve(rt)
        resultB
    }.asInstanceOf[S[FunctionCallResultWithProvenance[B]]]
  }
}

object MapWithProvenance {
  //def apply[S[_], A](implicit converter: Applicable[S]) = new ApplyWithProvenance[S, A]
  //def apply[E2 : ClassTag, E : ClassTag] = new MapWithProvenance[E2, E]
  def apply[B, A, S[_]](
    implicit converter: Traversable[S], ct: ClassTag[S[B]], ct2: ClassTag[A], ct3: ClassTag[B]
  ) = new MapWithProvenance[B, A, S]()(converter, ct, ct2, ct3)
}
