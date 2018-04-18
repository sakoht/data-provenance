package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * TraversableCall[A] is extended by the implicit class TraversableCallExt,
  * which extends FunctionCallWithProvenance[O] when O is an Traversable[_] (Seq, List, Vector).
  *
  */

import com.cibo.provenance.monadics._
import com.cibo.provenance._

import scala.language.higherKinds
import scala.reflect.ClassTag


class TraversableCall[S[_], A](call: FunctionCallWithProvenance[S[A]])(
  implicit hok: Traversable[S],
  cda: Codec[A],
  cdsa: Codec[S[A]],
  cdsi: Codec[S[Int]]
) {
  implicit lazy val ctr: ClassTag[Range] = ClassTag(classOf[Range])

  def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[S, A]#Call =
    ApplyWithProvenance[S, A].apply(call, n)

  def indices: IndicesTraversableWithProvenance[S, A]#Call =
    IndicesTraversableWithProvenance[S, A].apply(call)

  //UnknownProvenance(MyIncrement.asInstanceOf[Function1WithProvenance[Int, Int]])

  def map[B](f: ValueWithProvenance[Function1WithProvenance[A, B]])
    (implicit
      cdsb: Codec[S[B]],
      cdf: Codec[Function1WithProvenance[A, B]],
      cdb: Codec[B]
    ): MapWithProvenance[A, S, B]#Call =
      new MapWithProvenance[A, S, B].apply(call, f)

  def map[B](f: Function1WithProvenance[A, B])
    (implicit
      cdsb: Codec[S[B]],
      cdf: Codec[Function1WithProvenance[A, B]],
      cdb: Codec[B]
    ): MapWithProvenance[A, S, B]#Call = {
      new MapWithProvenance[A, S, B].apply(call, UnknownProvenance.apply(f.asInstanceOf[Function1WithProvenance[A, B]]))
  }

  def scatter(implicit rt: ResultTracker): S[FunctionCallWithProvenance[A]] = {
    val indicesResult: IndicesTraversableWithProvenance[S, A]#Result = IndicesTraversableWithProvenance[S, A].apply(call).resolve
    val indexesTraversable: S[Int] = indicesResult.output
    def getApply(n: Int): FunctionCallWithProvenance[A] = ApplyWithProvenance[S, A].apply(call, n)
    hok.map(getApply)(indexesTraversable)
  }
}
