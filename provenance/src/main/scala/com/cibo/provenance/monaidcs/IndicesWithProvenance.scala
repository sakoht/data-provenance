package com.cibo.provenance.monaidcs

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls indices on a Seq.
  *
  */

import com.cibo.provenance._
import scala.reflect.ClassTag

class IndicesWithProvenance[T : ClassTag] extends Function1WithProvenance[Seq[Int], Seq[T]] {
  val currentVersion: NoVersion.type = NoVersion
  def impl(seq: Seq[T]): Seq[Int] = seq.indices.toList // Range is not serializable, sadly.
}

object IndicesWithProvenance {
  def apply[A : ClassTag] = new IndicesWithProvenance[A]
}
