package com.cibo.provenance.mappable

/**
  * Created by ssmith on 11/06/17.
  *
  * A builtin FunctionWithProvenance that calls apply(i) on a Seq.
  *
  */

import com.cibo.provenance.{Function2WithProvenance, NoVersion}

class ApplyWithProvenance[O] extends Function2WithProvenance[O, Seq[O], Int]  {
  val currentVersion = NoVersion
  def impl(seq: Seq[O], n: Int): O = seq(n)
}

object ApplyWithProvenance {
  def apply[O] = new ApplyWithProvenance[O]
}

