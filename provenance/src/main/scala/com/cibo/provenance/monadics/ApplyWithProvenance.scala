package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls apply(i) on a Seq.
  *
  */

import scala.language.higherKinds

import com.cibo.provenance._

class ApplyWithProvenance[S[_], O](implicit hok: Applicable[S]) extends Function2WithProvenance[O, S[O], Int]  {
  val currentVersion: Version = NoVersion
  def impl(s: S[O], n: Int): O =
    hok.apply(n)(s)
}

object ApplyWithProvenance {
  def apply[S[_], A](implicit converter: Applicable[S]) = new ApplyWithProvenance[S, A]
}

object ApplyToRangeWithProvenance extends Function2WithProvenance[Int, Range, Int] {
  val currentVersion: Version = NoVersion
  def impl(range: Range, idx: Int): Int = range(idx)
}
