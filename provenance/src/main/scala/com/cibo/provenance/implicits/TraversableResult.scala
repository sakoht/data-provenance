package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/12/17.
  *
  * TraversableResult[A] is extended by the implicit class TraversableResultExt,
  * which extends FunctionCallResultWithProvenance[O] when O is an Traversable[_] (Seq, List, Vector).
  *
  */

import scala.language.higherKinds

import com.cibo.provenance._

class TraversableResult[S[_], A](result: FunctionCallResultWithProvenance[S[A]])
  (implicit
    hok: Traversable[S],
    cda: Codec[A],
    cdsa: Codec[S[A]],
    cdsi: Codec[S[Int]]
  ) {
    import com.cibo.provenance.monadics._
    import scala.reflect.ClassTag

    implicit lazy val ctr: ClassTag[Range] = ClassTag(classOf[Range])

    def apply(n: ValueWithProvenance[Int]): ApplyWithProvenance[S, A]#Call =
      ApplyWithProvenance[S, A].apply(result, n)

    def indices: IndicesTraversableWithProvenance[S, A]#Call =
      IndicesTraversableWithProvenance[S, A].apply(result)

    def map[B, F <: Function1WithProvenance[A, B] : Codec](f: ValueWithProvenance[F])
      (implicit
        ctsb: ClassTag[S[B]],
        ctb: ClassTag[B],
        cb: Codec[B],
        csb: Codec[S[B]]
      ): MapWithProvenance[A, S, B]#Call =
      new MapWithProvenance[A, S, B].apply(result, f)

    def scatter(implicit rt: ResultTracker): S[FunctionCallResultWithProvenance[A]] = {
      val call1: FunctionCallWithProvenance[S[A]] = result.call
      val call2: TraversableCall[S, A] =
        FunctionCallWithProvenance.TraversableCallExt[S, A](call1)(hok, cda, cdsa, cdsi)
      val oneCallPerMember: S[FunctionCallWithProvenance[A]] = call2.scatter
      def getResult(call: FunctionCallWithProvenance[A]): FunctionCallResultWithProvenance[A] = call.resolve
      hok.map(getResult)(oneCallPerMember)
    }
  }
