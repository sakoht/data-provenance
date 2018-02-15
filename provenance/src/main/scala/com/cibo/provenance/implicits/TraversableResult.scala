package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * TraversableResult[A] is extended by the implicit class TraversableResultExt,
  * which extends FunctionCallResultWithProvenance[O] when O is an Traversable[_] (Seq, List, Vector).
  *
  */

import com.cibo.provenance.monadics._
import com.cibo.provenance.{ResultTracker, _}
import io.circe.{Decoder, Encoder}

import scala.language.higherKinds
import scala.reflect.ClassTag

class TraversableResult[S[_], A](result: FunctionCallResultWithProvenance[S[A]])
  (implicit
    hok: Traversable[S],
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
      ApplyWithProvenance[S, A].apply(result, n)

    def indices: IndicesTraversableWithProvenance[S, A]#Call =
      IndicesTraversableWithProvenance[S, A].apply(result)

    def map[B](f: ValueWithProvenance[Function1WithProvenance[B, A]])
      (implicit
        ctsb: ClassTag[S[B]],
        ctb: ClassTag[B],
        esb: Encoder[S[B]],
        eb: Encoder[B],
        dsb: Decoder[S[B]],
        db: Decoder[B]
      ): MapWithProvenance[B, A, S]#Call =
        new MapWithProvenance[B, A, S].apply(result, f)

    def map[B](f: Function1WithProvenance[B, A])
      (implicit
        ctsb: ClassTag[S[B]],
        ctb: ClassTag[B],
        esb: Encoder[S[B]],
        eb: Encoder[B],
        dsb: Decoder[S[B]],
        db: Decoder[B]
      ): MapWithProvenance[B, A, S]#Call =
      new MapWithProvenance[B, A, S].apply(result, UnknownProvenance(f.asInstanceOf[Function1WithProvenance[B, A]]))


    def scatter(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[A]] = {
      val call1: FunctionCallWithProvenance[S[A]] = result.call
      val call2: TraversableCall[S, A] =
        FunctionCallWithProvenance.TraversableCallExt[S, A](call1)(hok, cta, ctsa, ctsi, ea, esa, esi, da, dsa, dsi)
      val oneCallPerMember: S[FunctionCallWithProvenance[A]] = call2.scatter
      def getResult(call: FunctionCallWithProvenance[A]): FunctionCallResultWithProvenance[A] = call.resolve
      hok.map(getResult)(oneCallPerMember)
    }
  }