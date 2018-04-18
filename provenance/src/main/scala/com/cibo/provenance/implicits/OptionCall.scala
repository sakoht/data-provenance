package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * OptionCall[A] extends FunctionCallWithProvenance[O] when O is an Option[A].
  *
  */

import com.cibo.provenance._
import scala.language.existentials

/**
  * A wrapper for a call that returns an Option[A].
  * It offers all of the standard methods an Option does, but wraps the resut of each with tracking.
  *
  * @param call   Any FunctionCallWithProvenance that returns an Option[_]
  * @param cdsa   An implicit codec for Option[A]
  * @param cda    An implicit codec for A
  * @tparam A     The type of option.
  */
class OptionCall[A](call: FunctionCallWithProvenance[Option[A]])(implicit cdsa: Codec[Option[A]], cda: Codec[A]) {
  import OptionMethods._

  def get = new GetWithProvenance[A].apply(call)

  def isEmpty = new IsEmptyWithProvenance[A].apply(call)

  def nonEmpty = new NonEmptyWithProvenance[A].apply(call)

  def map[B : Codec](f: ValueWithProvenance[Function1WithProvenance[A, B]])(implicit bc: Codec[Option[B]]) =
    new MapWithProvenance[A, B].apply(call, f)
}
