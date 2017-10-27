package com.cibo.provenance

/**
  * Created by ssmith on 9/11/17.
  *
  * Note: The FunctionCallWithProvenance code lives in VirtualValue.scala (sealed trait).
  *
  * This file contains the Function{N}CallSignatureWithProvenance subclass implementations.
  *
  */

import com.cibo.provenance.tracker.ResultTracker

import scala.language.implicitConversions
import scala.reflect.ClassTag


abstract class Function0CallWithProvenance[O : ClassTag](v: ValueWithProvenance[Version])(f: (Version) => O) extends FunctionCallWithProvenance[O](v) with Serializable {
  val impl = f
  def getInputs: Seq[ValueWithProvenance[_]] = Seq.empty
  def resolveInputs(implicit rt: ResultTracker): Function0CallWithProvenance[O] =
    duplicate(v.resolve)
  def unresolveInputs(implicit rt: ResultTracker): Function0CallWithProvenance[O] =
    duplicate(v.unresolve)
  /*def collapseInputs(implicit rt: ResultTracker): Function0CallWithProvenance[O] =
    duplicate(v.collapse)
  def uncollapseInputs(implicit rt: ResultTracker): Function0CallWithProvenance[O] =
    (v.uncollapse)*/
  def run(implicit rt: ResultTracker): Function0CallResultWithProvenance[O] = {
    val o: O = f(getVersionValue)
    new Function0CallResultWithProvenance(this, output = Deflatable(o))(rt.getCurrentBuildInfo)
  }
  def newResult(value: Deflatable[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]
  def duplicate(vv: ValueWithProvenance[Version]): Function0CallWithProvenance[O]
}


abstract class Function1CallWithProvenance[O : ClassTag, I1](i1: ValueWithProvenance[I1], v: ValueWithProvenance[Version])(f: (I1, Version) => O) extends FunctionCallWithProvenance[O](v) with Serializable {
  val impl = f
  def getInputs: Seq[ValueWithProvenance[_]] = Seq(i1)
  def inputTuple: Tuple1[ValueWithProvenance[I1]] = Tuple1(i1)
  def resolveInputs(implicit rt: ResultTracker): Function1CallWithProvenance[O, I1] =
    duplicate(i1.resolve, v.resolve)
  def unresolveInputs(implicit rt: ResultTracker): Function1CallWithProvenance[O, I1] =
    duplicate(i1.unresolve, v.unresolve)
  /*def collapseInputs(implicit rt: ResultTracker): Function1CallWithProvenance[O, I1] =
    duplicate(i1.collapse, v.collapse)
  def uncollapseInputs(implicit rt: ResultTracker): Function1CallWithProvenance[O, I1] =
    (i1.uncollapse, v.uncollapse)*/
  def run(implicit rt: ResultTracker): Function1CallResultWithProvenance[O, I1] = {
    val output: O = f(i1.resolve.getOutputValue, getVersionValue)
    new Function1CallResultWithProvenance(this, output = Deflatable(output))(rt.getCurrentBuildInfo)
  }
  def newResult(value: Deflatable[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]
  def duplicate(v1: ValueWithProvenance[I1], vv: ValueWithProvenance[Version]): Function1CallWithProvenance[O, I1]
}


abstract class Function2CallWithProvenance[O : ClassTag, I1, I2](i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], v: ValueWithProvenance[Version])(f: (I1, I2, Version) => O) extends FunctionCallWithProvenance[O](v) with Serializable {
  val impl = f
  def getInputs: Seq[ValueWithProvenance[_]] = Seq(i1, i2)
  def inputTuple = (i1, i2)
  def resolveInputs(implicit rt: ResultTracker): Function2CallWithProvenance[O, I1, I2] =
    duplicate(i1.resolve, i2.resolve, v.resolve)
  def unresolveInputs(implicit rt: ResultTracker): Function2CallWithProvenance[O, I1, I2] =
    duplicate(i1.unresolve, i2.unresolve, v.unresolve)
  /*def collapseInputs(implicit rt: ResultTracker): Function2CallWithProvenance[O, I1, I2] =
    duplicate(i1.collapse, i2.collapse, v.collapse)
  def uncollapseInputs(implicit rt: ResultTracker): Function2CallWithProvenance[O, I1, I2] =
    (i1.uncollapse, i2.uncollapse, v.uncollapse)*/
  private implicit val i1ct: ClassTag[I1] = i1.getOutputClassTag
  private implicit val i2ct: ClassTag[I2] = i2.getOutputClassTag
  def run(implicit rt: ResultTracker): Function2CallResultWithProvenance[O, I1, I2] = {
    val output: O = f(i1.resolve.getOutputValue, i2.resolve.getOutputValue, getVersionValue)
    new Function2CallResultWithProvenance(this, output = Deflatable(output))(rt.getCurrentBuildInfo)
  }
  def newResult(value: Deflatable[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]
  def duplicate(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], vv: ValueWithProvenance[Version]): Function2CallWithProvenance[O, I1, I2]
}


abstract class Function3CallWithProvenance[O : ClassTag, I1, I2, I3](i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], v: ValueWithProvenance[Version])(f: (I1, I2, I3, Version) => O) extends FunctionCallWithProvenance[O](v) {
  val impl = f
  def getInputs: Seq[ValueWithProvenance[_]] = Seq(i1, i2, i3)
  def inputTuple = (i1, i2, i3)
  def resolveInputs(implicit rt: ResultTracker): Function3CallWithProvenance[O, I1, I2, I3] =
    duplicate(i1.resolve, i2.resolve, i3.resolve, v.resolve)
  def unresolveInputs(implicit rt: ResultTracker): Function3CallWithProvenance[O, I1, I2, I3] =
    duplicate(i1.unresolve, i2.unresolve, i3.unresolve, v.unresolve)
  /*def collapseInputs(implicit rt: ResultTracker): Function3CallWithProvenance[O, I1, I2, I3] =
    duplicate(i1.collapse, i2.collapse, i3.collapse, v.collapse)
  def uncollapseInputs(implicit rt: ResultTracker): Function3CallWithProvenance[O, I1, I2, I3] =
    (i1.uncollapse, i2.uncollapse, i3.uncollapse, v.uncollapse)*/
  private implicit val i1ct: ClassTag[I1] = i1.getOutputClassTag
  private implicit val i2ct: ClassTag[I2] = i2.getOutputClassTag
  private implicit val i3ct: ClassTag[I3] = i3.getOutputClassTag
  def run(implicit rt: ResultTracker): Function3CallResultWithProvenance[O, I1, I2, I3] = {
    val output: O = f(i1.resolve.getOutputValue, i2.resolve.getOutputValue, i3.resolve.getOutputValue, getVersionValue)
    new Function3CallResultWithProvenance(this, output = Deflatable(output))(rt.getCurrentBuildInfo)
  }
  def newResult(value: Deflatable[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]
  def duplicate(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], v3: ValueWithProvenance[I3], vv: ValueWithProvenance[Version]): Function3CallWithProvenance[O, I1, I2, I3]
}


abstract class Function4CallWithProvenance[O : ClassTag, I1, I2, I3, I4](i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], v: ValueWithProvenance[Version])(f: (I1, I2, I3, I4, Version) => O) extends FunctionCallWithProvenance[O](v) {
  val impl = f
  def getInputs: Seq[ValueWithProvenance[_]] = Seq(i1, i2, i3, i4, v)
  def inputTuple = (i1, i2, i3, i4)
  def resolveInputs(implicit rt: ResultTracker): Function4CallWithProvenance[O, I1, I2, I3, I4] =
    duplicate(i1.resolve, i2.resolve, i3.resolve, i4.resolve, v.resolve)
  def unresolveInputs(implicit rt: ResultTracker): Function4CallWithProvenance[O, I1, I2, I3, I4] =
    duplicate(i1.unresolve, i2.unresolve, i3.unresolve, i4.unresolve, v.unresolve)
  /*def collapseInputs(implicit rt: ResultTracker): Function4CallWithProvenance[O, I1, I2, I3, I4] =
    duplicate(i1.collapse, i2.collapse, i3.collapse, i4.collapse, v.collapse)
  def uncollapseInputs(implicit rt: ResultTracker): Function4CallWithProvenance[O, I1, I2, I3, I4] =
    (i1.uncollapse, i2.uncollapse, i3.uncollapse, i4.uncollapse, v.uncollapse)*/
  private implicit val i1ct: ClassTag[I1] = i1.getOutputClassTag
  private implicit val i2ct: ClassTag[I2] = i2.getOutputClassTag
  private implicit val i3ct: ClassTag[I3] = i3.getOutputClassTag
  private implicit val i4ct: ClassTag[I4] = i4.getOutputClassTag
  def run(implicit rt: ResultTracker): Function4CallResultWithProvenance[O, I1, I2, I3, I4] = {
    val output: O = f(i1.resolve.getOutputValue, i2.resolve.getOutputValue, i3.resolve.getOutputValue, i4.resolve.getOutputValue, getVersionValue)
    new Function4CallResultWithProvenance(this, output = Deflatable(output))(rt.getCurrentBuildInfo)
  }
  def newResult(value: Deflatable[O])(implicit bi: BuildInfo): FunctionCallResultWithProvenance[O]
  def duplicate(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], v3: ValueWithProvenance[I3], v4: ValueWithProvenance[I4], vv: ValueWithProvenance[Version]): Function4CallWithProvenance[O, I1, I2, I3, I4]
}
