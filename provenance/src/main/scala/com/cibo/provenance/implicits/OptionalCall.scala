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
    da: Decoder[A],
    ca: Codec[A]
  ) {

  self =>

  implicit val csa: Codec[Option[A]] = new Codec[Option[A]]

  import com.cibo.provenance.ResultTracker

  val currentVersion = NoVersion

  def get = GetWithProvenance(call)
  def isEmpty = IsEmptyWithProvenance(call)
  def nonEmpty = NonEmptyWithProvenance(call)
  def map[B : ClassTag : Encoder : Decoder : Codec](
    f: ValueWithProvenance[Function1WithProvenance[A, B]]
  )(
    implicit bc: Codec[Option[B]]
  ) =
    new MapWithProvenance[B].apply(call, f, UnknownProvenance(currentVersion))

  object GetWithProvenance extends Function1WithProvenance[Option[A], A] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.get
  }

  object IsEmptyWithProvenance extends Function1WithProvenance[Option[A], Boolean] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.isEmpty
  }

  object NonEmptyWithProvenance extends Function1WithProvenance[Option[A], Boolean] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.nonEmpty
  }

  class MapWithProvenance[B : ClassTag : Encoder : Decoder : Codec](implicit ob: Codec[Option[B]]) extends Function2WithProvenance[Option[A], Function1WithProvenance[A, B], Option[B]] {
    val currentVersion: Version = NoVersion
    override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
      val aOptionResolved: FunctionCallResultWithProvenance[_ <: Option[A]] = call.i1.resolve
      aOptionResolved.output match {
        case Some(a) =>
          val funcResolved: FunctionCallResultWithProvenance[_ <: Function1WithProvenance[A, B]] = call.i2.resolve
          val func: Function1WithProvenance[A, B] = funcResolved.output
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
    def impl(s: Option[A], f: Function1WithProvenance[A, B]): Option[B] =
      s.map(f.impl)
  }
}
