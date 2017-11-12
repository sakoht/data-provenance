package com.cibo.provenance

import com.cibo.provenance.monadics.{ApplyWithProvenance, IndicesWithProvenance, MapWithProvenance, Traversable}
import com.cibo.provenance.tracker.ResultTracker

import scala.reflect.ClassTag

/**
  * Created by ssmith on 9/12/17.
  *
  * Note: The base class FunctionCallSignatureWithProvenance is defined in VirtualValue.scala,
  * since VirtualValue[T] is a sealed trait!
  *
  */

class Function0CallResultWithProvenance[O](f: Function0CallWithProvenance[O], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) with Serializable {
  def provenance: Function0CallWithProvenance[O] = f
}

class Function1CallResultWithProvenance[O, I1](f: Function1CallWithProvenance[O, I1], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function1CallWithProvenance[O, I1] = f
}

class Function2CallResultWithProvenance[O, I1, I2](f: Function2CallWithProvenance[O, I1, I2], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function2CallWithProvenance[O, I1, I2] = f
}

class Function3CallResultWithProvenance[O, I1, I2, I3](f: Function3CallWithProvenance[O, I1, I2, I3], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function3CallWithProvenance[O, I1, I2, I3] = f
}

class Function4CallResultWithProvenance[O, I1, I2, I3, I4](f: Function4CallWithProvenance[O, I1, I2, I3, I4], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function4CallWithProvenance[O, I1, I2, I3, I4] = f
}
