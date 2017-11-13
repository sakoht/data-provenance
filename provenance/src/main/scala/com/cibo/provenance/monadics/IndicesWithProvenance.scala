package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `indices` on a Traversable.
  *
  */

import scala.language.higherKinds
import com.cibo.provenance.{implicits, _}

class IndicesWithProvenance[S[_], O](implicit hok: implicits.Traversable[S]) extends Function1WithProvenance[Range, S[O]]  {
  val currentVersion: Version = NoVersion
  def impl(s: S[O]): Range = hok.indices(s)
}

object IndicesWithProvenance {
  def apply[S[_], A](implicit converter: implicits.Traversable[S]) = new IndicesWithProvenance[S, A]
}

object IndicesOfRangeWithProvenance extends Function1WithProvenance[Range, Range] {
  val currentVersion: Version = NoVersion
  def impl(range: Range): Range = range.indices
}