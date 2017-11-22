package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `indices` on a Traversable.
  *
  * NOTE: This implementation has a Range output.
  * See IndicesTraversableWithProvenance to maintain the higher-order type S[Int]
  *
  * TODO: This could actually shortcut and _not_ resolve its inputs before returning indices.
  * We know that myList.map(f1).map(f2).map(f3).indices are the indices of myList w/o calling the functions.
  *
  */

import scala.language.higherKinds
import com.cibo.provenance.{implicits, _}

class IndicesRangeWithProvenance[S[_], O](implicit hok: implicits.Traversable[S]) extends Function1WithProvenance[Range, S[O]]  {
  val currentVersion: Version = NoVersion
  def impl(s: S[O]): Range = hok.indicesRange(s)
}

object IndicesRangeWithProvenance {
  def apply[S[_], A](implicit converter: implicits.Traversable[S]) = new IndicesRangeWithProvenance[S, A]
}

object IndicesOfRangeWithProvenance extends Function1WithProvenance[Range, Range] {
  val currentVersion: Version = NoVersion
  def impl(range: Range): Range = range.indices
}