package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * OptionCall[A] is extended by the implicit class OptionCallExt[A],
  * which extends FunctionCallWithProvenance[O] when O is an Option[_].
  *
  */

import com.cibo.provenance._

import scala.language.existentials
import scala.reflect.ClassTag

class OptionalCall[A]
  (call: FunctionCallWithProvenance[Option[A]])
  (implicit ctsa: ClassTag[Option[A]], cta: ClassTag[A]) {

  self =>

  import com.cibo.provenance.tracker.ResultTracker

  def get = GetWithProvenance(call)
  def isEmpty = IsEmptyWithProvenance(call)
  def nonEmpty = NonEmptyWithProvenance(call)
  def map[B : ClassTag](f: Function1WithProvenance[B, A]) = new MapWithProvenance[B].apply(call, f)

  object GetWithProvenance extends Function1WithProvenance[A, Option[A]] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.get
  }

  object IsEmptyWithProvenance extends Function1WithProvenance[Boolean, Option[A]] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.isEmpty
  }

  object NonEmptyWithProvenance extends Function1WithProvenance[Boolean, Option[A]] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.nonEmpty
  }

  class MapWithProvenance[B : ClassTag] extends Function2WithProvenance[Option[B], Option[A], Function1WithProvenance[B, A]] {
    val currentVersion: Version = NoVersion
    override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
      val aOptionResolved: FunctionCallResultWithProvenance[Option[A]] = call.v1.resolve
      aOptionResolved.output match {
        case Some(a) =>
          val funcResolved: FunctionCallResultWithProvenance[Function1WithProvenance[B, A]] = call.v2.resolve
          val func: Function1WithProvenance[B, A] = funcResolved.output
          val aResolved: GetWithProvenance.Result = self.get.resolve
          def a2b(a: FunctionCallResultWithProvenance[A]): FunctionCallResultWithProvenance[B] = func(a).resolve(rt)
          val bResolved: FunctionCallResultWithProvenance[B] = a2b(aResolved)
          val b: B = bResolved.output
          val bOptionResolved: Result = call.newResult(Some(b))(rt.getCurrentBuildInfo)
          bOptionResolved
        case None =>
          call.newResult(None)(rt.getCurrentBuildInfo)
      }
    }

    def impl(s: Option[A], f: Function1WithProvenance[B, A]): Option[B] =
      s.map(f.impl) // provided for completeness
  }
}
