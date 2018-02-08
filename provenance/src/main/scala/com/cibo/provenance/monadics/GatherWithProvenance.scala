package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  *  A builtin FunctionWithProvenance that turns a `Seq[ValueWithProvenance[T]]``
  *  into a `ValueWithProvenance[Seq[T]]``
  *
  */

import com.cibo.provenance._
import io.circe._

import scala.reflect.ClassTag

class GatherWithProvenance[E : ClassTag, O <: Seq[E] : Encoder : Decoder, I <: Seq[ValueWithProvenance[E]]] extends Function1WithProvenance[O, I]  {
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
  def apply[E : ClassTag : Encoder : Decoder] = new GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]

  // Actually call the above on a given Seq.
  def gather[E : ClassTag : Encoder : Decoder, O <: Seq[E] : ClassTag : Encoder : Decoder](seq: O) = {
    val vseq: ValueWithProvenance[Seq[ValueWithProvenance[E]]] =
      ValueWithProvenance.convertValueWithNoProvenance(seq).asInstanceOf[ValueWithProvenance[Seq[ValueWithProvenance[E]]]]
    implicit val ct = implicitly[ClassTag[O]].asInstanceOf[ClassTag[Seq[E]]]
    implicit val enc = implicitly[Encoder[O]].asInstanceOf[Encoder[Seq[E]]]
    implicit val dec = implicitly[Decoder[O]].asInstanceOf[Decoder[Seq[E]]]
    val g: GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]] = apply[E]
    val x: g.Call = g.apply(vseq)
    x.asInstanceOf[GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]#Call]
  }

}
