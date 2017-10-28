package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  */


import com.cibo.provenance.tracker.ResultTracker

import scala.language.implicitConversions
import scala.reflect.ClassTag


trait FunctionWithProvenance[O] extends Serializable {

  val currentVersion: Version

  val loadableVersions: Seq[Version] = Seq(currentVersion)
  val runnableVersions: Seq[Version] = Seq(currentVersion)

  lazy val loadableVersionSet: Set[Version] = loadableVersions.toSet
  lazy val runnableVersionSet: Set[Version] = runnableVersions.toSet

  protected def throwInvalidVersionException(v: Version): Unit = {
    if (runnableVersions.contains(v)) {
      throw new RuntimeException(f"Version $v of $this is in the runnableVersions list, but implVersion is not overridden to handle it!")
    } else if (loadableVersions.contains(v)) {
      throw UnrunnableVersionException(v, this)
    } else {
      throw UnknownVersionException(v, this)
    }
  }
}


trait Function0WithProvenance[O] extends FunctionWithProvenance[O] with Serializable {
  self =>

  val currentVersion: Version

  def impl(): O

  def apply(v: ValueWithProvenance[Version] = currentVersion)(implicit outputClassTag: ClassTag[O]): Call = Call(v)(outputClassTag)

  protected def implVersion(v: Version): O = {
    if (v != currentVersion)
      throw new RuntimeException(f"Cannot run old version $v!  Current version is $currentVersion")
    impl()
  }

  case class Call(vv: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function0CallWithProvenance[O](vv)(implVersion) {
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + "()" // no Tuple0
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutput)(rt.getCurrentBuildInfo)
    def newResult(output: Deflatable[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(vv: ValueWithProvenance[Version]): Call = copy(vv)
  }

  case class Result(provenance: Call, data: Deflatable[O])(implicit bi: BuildInfo) extends Function0CallResultWithProvenance(provenance, data)(bi)

  implicit private def convertResult(r: Function0CallResultWithProvenance[O]): Result = Result(r.getProvenanceValue, r.getOutput)(r.getOutputBuildInfo)

  implicit private def convertProvenance(f: Function0CallWithProvenance[O]): Call = Call(f.getVersion)(f.getOutputClassTag)
}


trait Function1WithProvenance[O, I1] extends FunctionWithProvenance[O] with Serializable {
  self =>

  val currentVersion: Version

  def impl(i1: I1): O

  def apply(i1: ValueWithProvenance[I1], v: ValueWithProvenance[Version] = currentVersion)(implicit outputClassTag: ClassTag[O]): Call = Call(i1, v)

  protected def implVersion(i1: I1, v: Version): O = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(i1)
  }

  case class Call(v1: ValueWithProvenance[I1], vv: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function1CallWithProvenance[O, I1](v1, vv)(implVersion) with Serializable {
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutput)(rt.getCurrentBuildInfo)
    def newResult(output: Deflatable[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], vv: ValueWithProvenance[Version]): Call = copy(v1, vv)
  }

  case class Result(provenance: Call, data: Deflatable[O])(implicit bi: BuildInfo) extends Function1CallResultWithProvenance(provenance, data)(bi)

  implicit private def convertResult(r: Function1CallResultWithProvenance[O, I1]): Result = Result(r.getProvenanceValue, r.getOutput)(r.getOutputBuildInfo)

  implicit private def convertProvenance(f: Function1CallWithProvenance[O, I1]): Call = {
    val i = f.inputTuple
    Call(i._1, f.getVersion)(f.getOutputClassTag)
  }
}


trait Function2WithProvenance[O, I1, I2] extends FunctionWithProvenance[O] with Serializable {
  self =>

  val currentVersion: Version

  def impl(i1: I1, i2: I2): O
  
  def apply(i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], v: ValueWithProvenance[Version] = currentVersion)(implicit outputClassTag: ClassTag[O]): Call =
    Call(i1, i2, v)(outputClassTag)

  protected def implVersion(i1: I1, i2: I2, v: Version): O = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(i1, i2)
  }

  case class Call(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], vv: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function2CallWithProvenance[O, I1, I2](v1, v2, vv)(implVersion) with Serializable {
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutput)(rt.getCurrentBuildInfo)
    def newResult(output: Deflatable[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], vv: ValueWithProvenance[Version]): Call = copy(v1, v2, vv)
  }

  case class Result(provenance: Call, data: Deflatable[O])(implicit bi: BuildInfo) extends Function2CallResultWithProvenance(provenance, data)(bi)
  
  implicit private def convertResultWithVersion(r: Function2CallResultWithProvenance[O, I1, I2]): Result =
    Result(r.getProvenanceValue, r.getOutput)(r.getOutputBuildInfo)

  implicit private def convertProvenanceWithVersion(f: Function2CallWithProvenance[O, I1, I2]): Call = {
    val i = f.inputTuple
    Call(i._1, i._2, f.getVersion)(f.getOutputClassTag)
  }
}


trait Function3WithProvenance[O, I1, I2, I3] extends FunctionWithProvenance[O] {
  self =>

  val currentVersion: Version

  def impl(i1: I1, i2: I2, i3: I3): O

  def apply(i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], v: ValueWithProvenance[Version] = currentVersion)(implicit outputClassTag: ClassTag[O]): Call =
    Call(i1, i2, i3, v)

  protected def implVersion(i1: I1, i2: I2, i3: I3, v: Version): O = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(i1, i2, i3)
  }

  case class Call(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], v3: ValueWithProvenance[I3], vv: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function3CallWithProvenance[O, I1, I2, I3](v1, v2, v3, vv)(implVersion) {
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutput)(rt.getCurrentBuildInfo)
    def newResult(output: Deflatable[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], v3: ValueWithProvenance[I3], vv: ValueWithProvenance[Version]): Call = copy(v1, v2, v3, vv)
  }

  case class Result(provenance: Call, data: Deflatable[O])(implicit bi: BuildInfo) extends Function3CallResultWithProvenance(provenance, data)(bi)

  implicit private def convertResult(r: Function3CallResultWithProvenance[O, I1, I2, I3]): Result =
    Result(r.getProvenanceValue, r.getOutput)(r.getOutputBuildInfo)

  implicit private def convertProvenance(f: Function3CallWithProvenance[O, I1, I2, I3]): Call = {
    val i = f.inputTuple
    Call(i._1, i._2, i._3, f.getVersion)(f.getOutputClassTag)
  }
}


trait Function4WithProvenance[O, I1, I2, I3, I4] extends FunctionWithProvenance[O] {
  self =>

  val currentVersion: Version

  def impl(i1: I1, i2: I2, i3: I3, i4: I4): O

  def apply(i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], v: ValueWithProvenance[Version] = currentVersion)(implicit outputClassTag: ClassTag[O]): Call =
    Call(i1, i2, i3, i4, v)

  protected def implVersion(i1: I1, i2: I2, i3: I3, i4: I4, v: Version): O = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(i1, i2, i3, i4)
  }

  case class Call(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], v3: ValueWithProvenance[I3], v4: ValueWithProvenance[I4], vv: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function4CallWithProvenance[O, I1, I2, I3, I4](v1, v2, v3, v4, vv)(implVersion) {
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutput)(rt.getCurrentBuildInfo)
    def newResult(output: Deflatable[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], v3: ValueWithProvenance[I3], v4: ValueWithProvenance[I4], vv: ValueWithProvenance[Version]): Call = copy(v1, v2, v3, v4, vv)
  }

  case class Result(provenance: Call, data: Deflatable[O])(implicit bi: BuildInfo) extends Function4CallResultWithProvenance(provenance, data)(bi)

  implicit private def convertResult(r: Function4CallResultWithProvenance[O, I1, I2, I3, I4]): Result =
    Result(r.getProvenanceValue, r.getOutput)(r.getOutputBuildInfo)

  implicit private def convertProvenance(f: Function4CallWithProvenance[O, I1, I2, I3, I4]): Call = {
    val i = f.inputTuple
    Call(i._1, i._2, i._3, i._4, f.getVersion)(f.getOutputClassTag)
  }
}

