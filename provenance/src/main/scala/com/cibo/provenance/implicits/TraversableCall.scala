package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * TraversableCall[A] is extended by the implicit class TraversableCallExt,
  * which extends FunctionCallWithProvenance[O] when O is an Traversable[_] (Seq, List, Vector).
  *
  */

import com.cibo.provenance.monadics.{ApplyWithProvenance, IndicesRangeWithProvenance, IndicesTraversableWithProvenance, MapWithProvenance}
import com.cibo.provenance._
import io.circe._

import scala.language.higherKinds
import scala.reflect.ClassTag


class TraversableCall[S[_], A](call: FunctionCallWithProvenance[S[A]])(
  implicit hok: Traversable[S],
  cta: ClassTag[A],
  ctsa: ClassTag[S[A]],
  ctsi: ClassTag[S[Int]],
  ea: Encoder[A],
  esa: Encoder[S[A]],
  esi: Encoder[S[Int]],
  da: Decoder[A],
  dsa: Decoder[S[A]],
  dsi: Decoder[S[Int]]
) {
  implicit lazy val ctr: ClassTag[Range] = ClassTag(classOf[Range])

  def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[S, A]#Call =
    ApplyWithProvenance[S, A].apply(call, n)

  def indices: IndicesTraversableWithProvenance[S, A]#Call =
    IndicesTraversableWithProvenance[S, A].apply(call)

  //UnknownProvenance(MyIncrement.asInstanceOf[Function1WithProvenance[Int, Int]])

  def map[B](f: ValueWithProvenance[Function1WithProvenance[B, A]])
    (implicit
      ctsb: ClassTag[S[B]],
      ctb: ClassTag[B],
      esb: Encoder[S[B]],
      eb: Encoder[B],
      dsb: Decoder[S[B]],
      db: Decoder[B]
    ): MapWithProvenance[B, A, S]#Call =
      new MapWithProvenance[B, A, S].apply(call, f)

  def map[B](f: Function1WithProvenance[B, A])
    (implicit
      ctsb: ClassTag[S[B]],
      ctb: ClassTag[B],
      esb: Encoder[S[B]],
      eb: Encoder[B],
      dsb: Decoder[S[B]],
      db: Decoder[B]
    ): MapWithProvenance[B, A, S]#Call =
    new MapWithProvenance[B, A, S].apply(call, UnknownProvenance(f.asInstanceOf[Function1WithProvenance[B, A]]))

  def scatter(implicit rt: ResultTracker): S[FunctionCallWithProvenance[A]] = {
    val indicesResult: IndicesTraversableWithProvenance[S, A]#Result = IndicesTraversableWithProvenance[S, A].apply(call).resolve
    val indexesTraversable: S[Int] = indicesResult.output
    def getApply(n: Int): FunctionCallWithProvenance[A] = ApplyWithProvenance[S, A].apply(call, n)
    hok.map(getApply)(indexesTraversable)
  }
}