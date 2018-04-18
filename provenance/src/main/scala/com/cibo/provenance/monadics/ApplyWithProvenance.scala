package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls `apply(i)` on a Traversable.
  *
  */

import scala.language.higherKinds
import com.cibo.provenance.{implicits, _}

class ApplyWithProvenance[S[_], O : Codec](implicit hok: implicits.Traversable[S]) extends Function2WithProvenance[S[O], Int, O]  {
  self =>

  val currentVersion: Version = NoVersion

  def impl(s: S[O], n: Int): O =
    hok.apply(n)(s)

  override lazy val typeParameterTypeNames: Seq[String] =
    Seq(
      ReflectUtil.classToName(hok.outerClassTag),
      ReflectUtil.classToName(implicitly[Codec[O]].valueClassTag)
    )
}

object ApplyWithProvenance {
  def apply[S[_], A : Codec](implicit hok: implicits.Traversable[S]) = new ApplyWithProvenance[S, A]
}

object ApplyToRangeWithProvenance extends Function2WithProvenance[Range, Int, Int] {
  val currentVersion: Version = NoVersion
  def impl(range: Range, idx: Int): Int = range(idx)
}
