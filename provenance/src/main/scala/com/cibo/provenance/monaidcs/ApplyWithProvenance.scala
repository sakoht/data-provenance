package com.cibo.provenance.monaidcs

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls apply(i) on a Seq.
  *
  */

import com.cibo.provenance._
import scala.language.higherKinds

class ApplyWithProvenance[O] extends Function2WithProvenance[O, Seq[O], Int]  {
  val currentVersion: Version = NoVersion
  def impl(seq: Seq[O], n: Int): O = seq(n)
}

object ApplyWithProvenance {
  def apply[O] = new ApplyWithProvenance[O]
}

//

import com.cibo.provenance._

class ApplyWithProvenance2[S[_], A] extends Function3WithProvenance[A, Applicable[S], S[A], Int]  {
  val currentVersion: Version = NoVersion
  def impl(mp: Applicable[S], seq: S[A], n: Int): A = mp.apply(n)(seq)
}

object ApplyWithProvenance2 {
  def apply[S[_], A] = new ApplyWithProvenance2[S, A]
}

