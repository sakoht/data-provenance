package com.cibo.provenance

import com.cibo.provenance.monadics.{ApplyWithProvenance, IndicesRangeWithProvenance, MapWithProvenance}

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


class Function5CallResultWithProvenance[O, I1, I2, I3, I4,I5](f: Function5CallWithProvenance[O, I1, I2, I3, I4, I5], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function5CallWithProvenance[O, I1, I2, I3, I4, I5] = f
}

class Function6CallResultWithProvenance[O, I1, I2, I3, I4,I5, I6](f: Function6CallWithProvenance[O, I1, I2, I3, I4, I5, I6], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function6CallWithProvenance[O, I1, I2, I3, I4, I5, I6] = f
}

class Function7CallResultWithProvenance[O, I1, I2, I3, I4,I5, I6, I7](f: Function7CallWithProvenance[O, I1, I2, I3, I4, I5, I6, I7], output: VirtualValue[O])(implicit bi: BuildInfo) extends  FunctionCallResultWithProvenance(f, output, bi) {
  def provenance: Function7CallWithProvenance[O, I1, I2, I3, I4, I5, I6, I7] = f
}