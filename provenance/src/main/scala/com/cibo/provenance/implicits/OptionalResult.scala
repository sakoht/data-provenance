package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * OptionResult[A] is extended by the implicit class OptionResultExt[A],
  * which extends FunctionCallWithProvenance[O] when O is an Option[_].
  *
  */

import com.cibo.provenance._

import scala.language.existentials
import scala.reflect.ClassTag

class OptionalResult[A]
  (result: FunctionCallResultWithProvenance[Option[A]])
  (implicit ctsa: ClassTag[Option[A]], cta: ClassTag[A]) {

  import com.cibo.provenance.ResultTracker

  def get(implicit rt: ResultTracker) = result.provenance.get.resolve
  def isEmpty(implicit rt: ResultTracker) = result.provenance.isEmpty.resolve
  def nonEmpty(implicit rt: ResultTracker) = result.provenance.nonEmpty.resolve
  def map[B : ClassTag](f: Function1WithProvenance[B, A])(implicit rt: ResultTracker) = result.provenance.map(f).resolve
}
