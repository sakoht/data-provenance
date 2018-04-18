package com.cibo.provenance.implicits

import com.cibo.provenance._

import scala.reflect.ClassTag

/**
  * Wrappers around basic method calls for Options.
  */
object OptionMethods {
  import com.cibo.provenance.ResultTracker

  private def codecsToClassName(codecs: Codec[_]*): Seq[String] =
    codecs.map(_.fullClassName)

  class GetWithProvenance[A](implicit cdsa: Codec[Option[A]], cda: Codec[A]) extends Function1WithProvenance[Option[A], A] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.get
    override lazy val typeParameterTypeNames: Seq[String] = codecsToClassName(cda)
  }

  class IsEmptyWithProvenance[A](implicit cdsa: Codec[Option[A]], cda: Codec[A]) extends Function1WithProvenance[Option[A], Boolean] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.isEmpty
    override lazy val typeParameterTypeNames: Seq[String] = codecsToClassName(cda)
  }

  class NonEmptyWithProvenance[A](implicit cdsa: Codec[Option[A]], cda: Codec[A]) extends Function1WithProvenance[Option[A], Boolean] {
    val currentVersion: Version = NoVersion
    def impl(o: Option[A]) = o.nonEmpty
    override lazy val typeParameterTypeNames: Seq[String] = codecsToClassName(cda)
  }

  class MapWithProvenance[A, B](implicit cdsa: Codec[Option[A]], cda: Codec[A], cdob: Codec[Option[B]], cdb: Codec[B]) extends Function2WithProvenance[Option[A], Function1WithProvenance[A, B], Option[B]] {

    val currentVersion: Version = NoVersion

    override protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
      val aOptionResolved: FunctionCallResultWithProvenance[_ <: Option[A]] = call.i1.resolve
      aOptionResolved.output match {
        case Some(a) =>
          val funcResolved: FunctionCallResultWithProvenance[_ <: Function1WithProvenance[A, B]] = call.i2.resolve
          val func: Function1WithProvenance[A, B] = funcResolved.output
          val aResolved: GetWithProvenance[A]#Result = new GetWithProvenance[A].apply(aOptionResolved).resolve
          def a2b(a: FunctionCallResultWithProvenance[A]): FunctionCallResultWithProvenance[B] = func(a).resolve(rt)
          val bResolved: FunctionCallResultWithProvenance[B] = a2b(aResolved)
          val b: B = bResolved.output
          val bOptionResolved: Result = call.newResult(VirtualValue(Some(b)))(rt.currentAppBuildInfo)
          bOptionResolved
        case None =>
          call.newResult(VirtualValue(None))(rt.currentAppBuildInfo)
      }
    }

    // Note: runCall fully circumvents calling this impl.  It is provided for API completeness.
    def impl(s: Option[A], f: Function1WithProvenance[A, B]): Option[B] =
      s.map(f.impl)

    override lazy val typeParameterTypeNames: Seq[String] = codecsToClassName(cda, cdb)
  }
}
