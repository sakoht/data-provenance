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

class GatherWithProvenance[E, O <: Seq[E], I <: Seq[ValueWithProvenance[E]]] extends Function1WithProvenance[O, I]  {

  val currentVersion = NoVersion

  def impl(in: I): O = in.map {
      case v: FunctionCallResultWithProvenance[E] => v.outputAsVirtualValue.valueOption.get
      case c: UnknownProvenance[E] => c.value
      case _ =>
        throw new RuntimeException("Attempt to gather unsolved inputs?")
    }.asInstanceOf[O]

  def apply(in: I)
    (implicit
      cti: ClassTag[I],
      cto: ClassTag[O],
      en: Encoder[O],
      dc: Decoder[O]
    ): Call = {

      //val seq = in.asInstanceOf[Seq[ValueWithProvenance[E]]]
      implicit val e: Encoder[I] = ???
      implicit val d: Decoder[I] = ???
      val wrap = UnknownProvenance(in)
      apply(wrap, UnknownProvenance(currentVersion))
  }
}


object GatherWithProvenance {
  // Return the GatherWithProvenance[T] for a given T, where T is some element type in a sequence.
  def apply[E] = new GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]

  // Actually call the above on a given Seq.
  def gather[E, O <: Seq[E] : ClassTag : Encoder : Decoder](seq: O) = {
    implicit val ct2 = implicitly[ClassTag[O]].asInstanceOf[ClassTag[Seq[E]]]
    implicit val en2 = implicitly[Encoder[O]].asInstanceOf[Encoder[Seq[E]]]
    implicit val de2 = implicitly[Decoder[O]].asInstanceOf[Decoder[Seq[E]]]

    val gatherer: GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]] = apply[E]

    val vseq: ValueWithProvenance[Seq[ValueWithProvenance[E]]] =
      ValueWithProvenance.convertValueWithNoProvenance(seq).asInstanceOf[ValueWithProvenance[Seq[ValueWithProvenance[E]]]]

    val call: gatherer.Call = gatherer.apply(vseq)
    call.asInstanceOf[GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]#Call]
  }

}
