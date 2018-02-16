package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * OptionCall[A] extends FunctionCallWithProvenance[O] when O is an Option[A].
  *
  */

import com.cibo.provenance._
import io.circe.{Decoder, Encoder}

import scala.language.existentials
import scala.reflect.ClassTag

class OptionalCall[A](call: FunctionCallWithProvenance[Option[A]])
  (implicit
    ctsa: ClassTag[Option[A]],
    cta: ClassTag[A],
    esa: Encoder[Option[A]],
    dsa: Decoder[Option[A]],
    ea: Encoder[A],
    da: Decoder[A]
  ) {

  self =>

  import com.cibo.provenance.ResultTracker

  val currentVersion = NoVersion

  def get = GetWithProvenance(call)
  def isEmpty = IsEmptyWithProvenance(call)
  def nonEmpty = NonEmptyWithProvenance(call)
  def map[B : ClassTag : Encoder : Decoder](f: ValueWithProvenance[Function1WithProvenance[B, A]]) =
    new MapWithProvenance[B].apply(call, f, UnknownProvenance(currentVersion))

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

  class MapWithProvenance[B : ClassTag : Encoder : Decoder] extends Function2WithProvenance[Option[B], Option[A], Function1WithProvenance[B, A]] {
    val currentVersion: Version = NoVersion
    override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
      val aOptionResolved: FunctionCallResultWithProvenance[Option[A]] = call.i1.resolve
      aOptionResolved.output match {
        case Some(a) =>
          val funcResolved: FunctionCallResultWithProvenance[Function1WithProvenance[B, A]] = call.i2.resolve
          val func: Function1WithProvenance[B, A] = funcResolved.output
          val aResolved: GetWithProvenance.Result = self.get.resolve
          def a2b(a: FunctionCallResultWithProvenance[A]): FunctionCallResultWithProvenance[B] = func(a).resolve(rt)
          val bResolved: FunctionCallResultWithProvenance[B] = a2b(aResolved)
          val b: B = bResolved.output
          val bOptionResolved: Result = call.newResult(Some(b))(rt.currentAppBuildInfo)
          bOptionResolved
        case None =>
          call.newResult(None)(rt.currentAppBuildInfo)
      }
    }

    // Note: runCall fully circumvents calling this impl.  It is provided for API completeness.
    def impl(s: Option[A], f: Function1WithProvenance[B, A]): Option[B] =
      s.map(f.impl)
  }
}
