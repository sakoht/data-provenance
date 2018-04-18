package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  *  A builtin FunctionWithProvenance that turns a `Seq[ValueWithProvenance[T]]``
  *  into a `ValueWithProvenance[Seq[T]]``
  *
  */

import com.cibo.provenance.{Codec, _}
import io.circe._

import scala.reflect.ClassTag

class GatherWithProvenance[E, O <: Seq[E] : Codec, I <: Seq[ValueWithProvenance[E]] : Codec]
  extends Function1WithProvenance[I, O]  {

  val currentVersion = NoVersion

  def impl(in: I): O = in.map {
      case v: FunctionCallResultWithProvenance[E] => v.outputAsVirtualValue.valueOption.get
      case c: UnknownProvenance[E] => c.value
      case _ =>
        throw new RuntimeException("Attempt to gather unsolved inputs?")
    }.asInstanceOf[O]

  def apply(in: I): Call = {
      //val seq = in.asInstanceOf[Seq[ValueWithProvenance[E]]]
      val wrap = UnknownProvenance(in)
      apply(wrap, UnknownProvenance(currentVersion))
  }

  override lazy val typeParameterTypeNames: Seq[String] =
    ???


}


object GatherWithProvenance {
  // Return the GatherWithProvenance[T] for a given T, where T is some element type in a sequence.
  def apply[E](implicit e1: Codec[Seq[E]], e2: Codec[Seq[ValueWithProvenance[E]]]) =
    new GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]

  // Actually call the above on a given Seq.
  def gather[E, O <: Seq[E] : Codec](seq: O)(implicit e2: Codec[Seq[ValueWithProvenance[E]]]) = {
    //implicit val ct2 = implicitly[ClassTag[O]].asInstanceOf[ClassTag[Seq[E]]]
    implicit val cd2 = implicitly[Codec[O]].asInstanceOf[Codec[Seq[E]]]

    val gatherer: GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]] = apply[E]

    val vseq: ValueWithProvenance[Seq[ValueWithProvenance[E]]] =
      ValueWithProvenance.convertValueWithNoProvenance(seq).asInstanceOf[ValueWithProvenance[Seq[ValueWithProvenance[E]]]]

    val call: gatherer.Call = gatherer.apply(vseq)
    call.asInstanceOf[GatherWithProvenance[E, Seq[E], Seq[ValueWithProvenance[E]]]#Call]
  }
}
