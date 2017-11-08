package com.cibo.provenance.monaidcs

import com.cibo.provenance._
import com.cibo.provenance.tracker.ResultTracker

import scala.reflect.ClassTag


class MapWithProvenance[B : ClassTag, A : ClassTag] extends Function2WithProvenance[Seq[B], Seq[A], Function1WithProvenance[B, A]] {

  val currentVersion: NoVersion.type = NoVersion

  def impl(seq: Seq[A], f: Function1WithProvenance[B, A]): Seq[B] =
    // Note: Since the caller can access an `impl(...)` on other FunctionWithProvenance, this is provided for completeness.
    // The runCall method is overwritten to call `f`, instead, on the seq members, and compose a result from that.
    // It, effectively, circumvents this call, but returns the same result.
    seq.map(f.impl)

  override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    // Skip impl and construct the result indirectly.
    val individualResults: Seq[FunctionCallResultWithProvenance[B]] = runOnEach(call)(rt)
    val unified: Seq[B] = individualResults.map(_.output)
    call.newResult(unified)(rt.getCurrentBuildInfo)
  }

  protected def runOnEach(call: Call)(implicit rt: ResultTracker): Seq[FunctionCallResultWithProvenance[B]] = {
    val inResult: FunctionCallResultWithProvenance[Seq[A]] = call.v1.resolve
    val funcResult: FunctionCallResultWithProvenance[Function1WithProvenance[B, A]] = call.v2.resolve

    // The "funcResult" actually accounts for the possibility that the function selection itself has provenance.
    // In most cases, we won't really have a FunctionWithProvenance returned by another FunctionWithProvenance.
    // To support this a ValueWithProvenance will need an implicit class to add
    // an .apply method much like we give .apply and .map to Seqs.

    // For now, just grab the output and run with it.  It is, likely, only wrapped in UnknownProvenance.
    val func: Function1WithProvenance[B, A] = funcResult.output


    val indicesWithProvenance: IndicesWithProvenance[A]#Result = inResult.indices.resolve(rt)
    //val indices: List[Int] = indicesWithProvenance.output.toList

    inResult.output.indices.toList.map {
      n =>
        val nWithProvenance: ApplyWithProvenance[Int]#Call = indicesWithProvenance.apply(n)
        val e1call: ApplyWithProvenance[A]#Call = ApplyWithProvenance[A](inResult, nWithProvenance)
        val e1result: ApplyWithProvenance[A]#Result = e1call.resolve(rt)
        val e2call: func.Call = func(e1result)
        val e2result: FunctionCallResultWithProvenance[B] = e2call.resolve(rt)
        e2result
    }
  }
}


object MapWithProvenance {
  def apply[E2 : ClassTag, E : ClassTag] = new MapWithProvenance[E2, E]
}
