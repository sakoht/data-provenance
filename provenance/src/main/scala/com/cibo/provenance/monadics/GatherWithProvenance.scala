package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  *  A builtin FunctionWithProvenance that turns a `Seq[ValueWithProvenance[T]]``
  *  into a `ValueWithProvenance[Seq[T]]``
  *
  */

import com.cibo.provenance._

class GatherWithProvenance[E, O <: Seq[E], I <: Seq[ValueWithProvenance[E]]] extends Function1WithProvenance[O, I]  {
  val currentVersion = NoVersion
  def impl(in: I): O = in.map {
      case v: FunctionCallResultWithProvenance[E] => v.getOutputVirtual.valueOption.get
      case c: UnknownProvenance[E] => c.value
      case _ =>
        throw new RuntimeException("Attempt to gather unsolved inputs?")
    }.asInstanceOf[O]
}


object GatherWithProvenance {
  // Return the GatherWithProvenance[T] for a given T.
  def apply[E] = new GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]

  // Actually call the above on a given Seq.
  def gather[E](seq: Seq[ValueWithProvenance[E]]) = apply[E].apply(seq)
}
