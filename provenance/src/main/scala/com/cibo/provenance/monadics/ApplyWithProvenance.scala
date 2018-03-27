package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `apply(i)` on a Traversable.
  *
  */

import scala.language.higherKinds
import com.cibo.provenance.{implicits, _}
import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag

class ApplyWithProvenance[S[_], O : ClassTag : Codec](implicit hok: implicits.Traversable[S]) extends Function2WithProvenance[S[O], Int, O]  {
  val currentVersion: Version = NoVersion
  def impl(s: S[O], n: Int): O =
    hok.apply(n)(s)
}

object ApplyWithProvenance {
  def apply[S[_], A : ClassTag : Codec](implicit hok: implicits.Traversable[S]) = new ApplyWithProvenance[S, A]
}

object ApplyToRangeWithProvenance extends Function2WithProvenance[Range, Int, Int] {
  val currentVersion: Version = NoVersion
  def impl(range: Range, idx: Int): Int = range(idx)
}
