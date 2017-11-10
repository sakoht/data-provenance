package com.cibo.provenance.monaidcs

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls indices on a Seq.
  *
  */

import scala.language.higherKinds

import com.cibo.provenance._
import scala.reflect.ClassTag

class IndicesWithProvenance[T : ClassTag] extends Function1WithProvenance[Seq[Int], Seq[T]] {
  val currentVersion: NoVersion.type = NoVersion
  def impl(seq: Seq[T]): Seq[Int] = seq.indices.toList // Range is not serializable, sadly.
}

object IndicesWithProvenance {
  def apply[A : ClassTag] = new IndicesWithProvenance[A]
}

//

import com.cibo.provenance._

class IndicesWithProvenance2[S[_], A] extends Function2WithProvenance[S[Int], Applicable[S], S[A]]  {
  val currentVersion: Version = NoVersion
  def impl(mp: Applicable[S], seq: S[A]): S[Int] = mp.indices(seq)
}

object IndicesWithProvenance2 {
  def apply[S[_], A] = new IndicesWithProvenance2[S, A]
}
