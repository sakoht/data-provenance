package com.cibo.provenance

/**
  * Created by ssmith on 9/12/17.
  */

import com.cibo.provenance.tracker.ResultTracker

import scala.language.implicitConversions
import scala.reflect.ClassTag


trait FunctionWithProvenance[O] extends Serializable {

  val currentVersion: Version

  lazy val loadableVersions: Seq[Version] = Seq(currentVersion)
  lazy val runnableVersions: Seq[Version] = Seq(currentVersion)

  lazy val loadableVersionSet: Set[Version] = loadableVersions.toSet
  lazy val runnableVersionSet: Set[Version] = runnableVersions.toSet

  def name = getClass.getName.stripSuffix("$")
  override def toString = f"$name@$currentVersion"

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
    val function = self
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + "()" // no Tuple0
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutputVirtual)(rt.getCurrentBuildInfo)
    override def resolve(implicit rt: ResultTracker): Result = super.resolve(rt).asInstanceOf[Result]
    def newResult(output: VirtualValue[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(vv: ValueWithProvenance[Version]): Call = copy(vv)
  }

  case class Result(call: Call, data: VirtualValue[O])(implicit bi: BuildInfo) extends Function0CallResultWithProvenance(call, data)(bi)

  implicit private def convertResult(r: Function0CallResultWithProvenance[O]): Result = Result(r.provenance, r.getOutputVirtual)(r.getOutputBuildInfoBrief)

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

  case class Call(v1: ValueWithProvenance[I1], v: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function1CallWithProvenance[O, I1](v1, v)(implVersion) with Serializable {
    val function = self
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutputVirtual)(rt.getCurrentBuildInfo)
    override def resolve(implicit rt: ResultTracker): Result = super.resolve(rt).asInstanceOf[Result]
    def newResult(output: VirtualValue[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], v: ValueWithProvenance[Version]): Call = copy(v1, v)
  }

  case class Result(call: Call, data: VirtualValue[O])(implicit bi: BuildInfo) extends Function1CallResultWithProvenance(call, data)(bi)

  implicit private def convertResult(r: Function1CallResultWithProvenance[O, I1]): Result = Result(r.provenance, r.getOutputVirtual)(r.getOutputBuildInfoBrief)

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

  // This method is "pulled-up" from Call into the function so they can be overridden
  // by provenance infrastructural functions.  In general, the impl() has access to only the raw values,
  // not the ValueWithProvenance[T] wrappers.

  protected def runCall(call: Call)(implicit rt: ResultTracker): Result = {
    implicit val ct: ClassTag[O] = call.getOutputClassTag
    val output: O = implVersion(call.v1.resolve.output, call.v2.resolve.output, call.getVersionValue)
    call.newResult(VirtualValue(output))(rt.getCurrentBuildInfo)
  }

  case class Call(v1: ValueWithProvenance[I1], v2: ValueWithProvenance[I2], vv: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function2CallWithProvenance[O, I1, I2](v1, v2, vv)(implVersion) with Serializable {
    val function = self
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = runCall(this)(rt)
    override def resolve(implicit rt: ResultTracker): Result = rt.resolve(this).asInstanceOf[Result]
    def newResult(output: VirtualValue[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], v: ValueWithProvenance[Version]): Call = copy(v1, i2, v)
  }

  case class Result(call: Call, data: VirtualValue[O])(implicit bi: BuildInfo) extends Function2CallResultWithProvenance(call, data)(bi)
  
  implicit private def convertResultWithVersion(r: Function2CallResultWithProvenance[O, I1, I2]): Result =
    Result(r.provenance, r.getOutputVirtual)(r.getOutputBuildInfoBrief)

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
    val function = self
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutputVirtual)(rt.getCurrentBuildInfo)
    override def resolve(implicit rt: ResultTracker): Result = super.resolve(rt).asInstanceOf[Result]
    def newResult(output: VirtualValue[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], v: ValueWithProvenance[Version]): Call = copy(v1, i2, i3, v)
  }

  case class Result(call: Call, data: VirtualValue[O])(implicit bi: BuildInfo) extends Function3CallResultWithProvenance(call, data)(bi)

  implicit private def convertResult(r: Function3CallResultWithProvenance[O, I1, I2, I3]): Result =
    Result(r.provenance, r.getOutputVirtual)(r.getOutputBuildInfoBrief)

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

  case class Call(v1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], v: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function4CallWithProvenance[O, I1, I2, I3, I4](v1, i2, i3, i4, v)(implVersion) {
    val function = self
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutputVirtual)(rt.getCurrentBuildInfo)
    override def resolve(implicit rt: ResultTracker): Result = super.resolve(rt).asInstanceOf[Result]
    def newResult(output: VirtualValue[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], v: ValueWithProvenance[Version]): Call = copy(v1, i2, i3, i4, v)
  }

  case class Result(call: Call, data: VirtualValue[O])(implicit bi: BuildInfo) extends Function4CallResultWithProvenance(call, data)(bi)

  implicit private def convertResult(r: Function4CallResultWithProvenance[O, I1, I2, I3, I4]): Result =
    Result(r.provenance, r.getOutputVirtual)(r.getOutputBuildInfoBrief)

  implicit private def convertProvenance(f: Function4CallWithProvenance[O, I1, I2, I3, I4]): Call = {
    val i = f.inputTuple
    Call(i._1, i._2, i._3, i._4, f.getVersion)(f.getOutputClassTag)
  }
}

trait Function7WithProvenance[O, I1, I2, I3, I4, I5, I6, I7] extends FunctionWithProvenance[O] {
  self =>

  val currentVersion: Version

  def impl(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7): O

  def apply(i1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], i5: ValueWithProvenance[I5], i6: ValueWithProvenance[I6], i7: ValueWithProvenance[I7], v: ValueWithProvenance[Version] = currentVersion)(implicit outputClassTag: ClassTag[O]): Call =
    Call(i1, i2, i3, i4, i5, i6, i7, v)

  protected def implVersion(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, v: Version): O = {
    if (v != currentVersion)
      throwInvalidVersionException(v)
    impl(i1, i2, i3, i4, i5, i6, i7)
  }

  case class Call(v1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], i5: ValueWithProvenance[I5], i6: ValueWithProvenance[I6], i7: ValueWithProvenance[I7], v: ValueWithProvenance[Version])(implicit outputClassTag: ClassTag[O]) extends Function7CallWithProvenance[O, I1, I2, I3, I4, I5, I6, I7](v1, i2, i3, i4, i5, i6, i7, v)(implVersion) {
    val function = self
    val functionName: String = self.getClass.getName.stripSuffix("$")
    override def toString: String = self.getClass.getSimpleName.stripSuffix("$") + inputTuple.toString
    override def run(implicit rt: ResultTracker): Result = newResult(super.run(rt).getOutputVirtual)(rt.getCurrentBuildInfo)
    override def resolve(implicit rt: ResultTracker): Result = super.resolve(rt).asInstanceOf[Result]
    def newResult(output: VirtualValue[O])(implicit bi: BuildInfo): Result = Result(this, output)(bi)
    def duplicate(v1: ValueWithProvenance[I1], i2: ValueWithProvenance[I2], i3: ValueWithProvenance[I3], i4: ValueWithProvenance[I4], i5: ValueWithProvenance[I5], i6: ValueWithProvenance[I6], i7: ValueWithProvenance[I7], v: ValueWithProvenance[Version]): Call = copy(v1, i2, i3, i4, i5, i6, i7, v)
  }

  case class Result(call: Call, data: VirtualValue[O])(implicit bi: BuildInfo) extends Function7CallResultWithProvenance(call, data)(bi)

  implicit private def convertResult(r: Function7CallResultWithProvenance[O, I1, I2, I3, I4, I5, I6, I7]): Result =
    Result(r.provenance, r.getOutputVirtual)(r.getOutputBuildInfoBrief)

  implicit private def convertProvenance(f: Function7CallWithProvenance[O, I1, I2, I3, I4, I5, I6, I7]): Call = {
    val i = f.inputTuple
    Call(i._1, i._2, i._3, i._4, i._5, i._6, i._7, f.getVersion)(f.getOutputClassTag)
  }
}
