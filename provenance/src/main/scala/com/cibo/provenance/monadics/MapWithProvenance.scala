package com.cibo.provenance.monadics

import scala.language.higherKinds
import scala.reflect.ClassTag
import com.cibo.provenance._
import com.cibo.provenance.tracker.ResultTracker

class MapWithProvenance[B, A, S[_]](implicit hok: Traversable[S], ctsb: ClassTag[S[B]], cta: ClassTag[A], ctb: ClassTag[B], ctsa: ClassTag[S[A]], ctsi: ClassTag[S[Int]])
  extends Function2WithProvenance[S[B], S[A], Function1WithProvenance[B, A]] {

  val currentVersion: NoVersion.type = NoVersion

  override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    // Skip the bulk impl() call and construct the output result from the individual calls.
    val individualResults: S[FunctionCallResultWithProvenance[B]] = runOnEach(call)(rt)
    val unifiedOutputs: S[B] = hok.map((r: FunctionCallResultWithProvenance[B]) => r.output)(individualResults)
    call.newResult(VirtualValue(unifiedOutputs))(rt.getCurrentBuildInfo)
  }

  def impl(s: S[A], f: Function1WithProvenance[B, A]): S[B] =
    // The runCall method circumvents actually calling impl, but composes the same output as this function.
    // It also generates results with provenance for each element, such that
    // This is provided for completeness since `.impl` is externally exposed.
    hok.map(f.impl)(s)

  protected def runOnEach(call: Call)(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[B]] = {
    val aResolved: FunctionCallResultWithProvenance[S[A]] = call.v1.resolve
    val aTraversable = FunctionCallResultWithProvenance.TraversableResult[S, A](aResolved)(hok, ctsa, cta, ctsi)
    val aGranular: S[FunctionCallResultWithProvenance[A]] = aTraversable.scatter

    val funcResolved: FunctionCallResultWithProvenance[Function1WithProvenance[B, A]] = call.v2.resolve
    val func: Function1WithProvenance[B, A] = funcResolved.output
    def a2b(a: FunctionCallResultWithProvenance[A]): FunctionCallResultWithProvenance[B] = func(a).resolve(rt)

    val bGranular: S[FunctionCallResultWithProvenance[B]] = hok.map(a2b)(aGranular)
    bGranular
  }
}

object MapWithProvenance {
  def apply[B, A, S[_]]
    (implicit hok: Traversable[S], ctsb: ClassTag[S[B]], cta: ClassTag[A], ctb: ClassTag[B], ctsa: ClassTag[S[A]], ctsi: ClassTag[S[Int]]) =
      new MapWithProvenance[B, A, S]()(hok, ctsb, cta, ctb, ctsa, ctsi)
}
