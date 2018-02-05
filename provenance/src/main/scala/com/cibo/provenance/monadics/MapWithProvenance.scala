package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `map(A=>B)` on a Traversable.
  *
  */

import scala.language.higherKinds
import scala.reflect.ClassTag
import com.cibo.provenance.{ResultTracker, implicits, _}

class MapWithProvenance[B, A, S[_]](implicit hok: implicits.Traversable[S], ctsb: ClassTag[S[B]], cta: ClassTag[A], ctb: ClassTag[B], ctsa: ClassTag[S[A]], ctsi: ClassTag[S[Int]])
  extends Function2WithProvenance[S[B], S[A], Function1WithProvenance[B, A]] {

  val currentVersion: Version = NoVersion

  override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    // Skip the bulk impl() call and construct the output result from the individual calls.
    val individualResults: S[FunctionCallResultWithProvenance[B]] = runOnEach(call)(rt)
    val unifiedOutputs: S[B] = hok.map((r: FunctionCallResultWithProvenance[B]) => r.output)(individualResults)
    call.newResult(VirtualValue(unifiedOutputs))(rt.getCurrentBuildInfo)
  }

  private def runOnEach(call: Call)(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[B]] = {
    val aResolved: FunctionCallResultWithProvenance[S[A]] = call.v1.resolve
    val aTraversable = FunctionCallResultWithProvenance.TraversableResultExt[S, A](aResolved)(hok, ctsa, cta, ctsi)
    val aGranular: S[FunctionCallResultWithProvenance[A]] = aTraversable.scatter

    val funcResolved: FunctionCallResultWithProvenance[Function1WithProvenance[B, A]] = call.v2.resolve
    val func: Function1WithProvenance[B, A] = funcResolved.output
    def a2b(a: FunctionCallResultWithProvenance[A]): FunctionCallResultWithProvenance[B] = func(a).resolve(rt)

    val bGranular: S[FunctionCallResultWithProvenance[B]] = hok.map(a2b)(aGranular)
    bGranular
  }

  def impl(s: S[A], f: Function1WithProvenance[B, A]): S[B] =
    // The runCall method circumvents actually calling impl, but composes the same output as this function.
    // This is provided for completeness since `.impl` is externally exposed, and should be testable.
    hok.map(f.impl)(s)
}

object MapWithProvenance {
  def apply[B, A, S[_]]
    (implicit hok: implicits.Traversable[S], ctsb: ClassTag[S[B]], cta: ClassTag[A], ctb: ClassTag[B], ctsa: ClassTag[S[A]], ctsi: ClassTag[S[Int]]) =
      new MapWithProvenance[B, A, S]()(hok, ctsb, cta, ctb, ctsa, ctsi)
}
