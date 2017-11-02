package com.cibo.provenance.tracker

/**
  * Created by ssmith on 5/16/17.
  *
  * A ResultTracker manages storage by mapping a FunctionCallWithProvenance to a FunctionCallResultWithProvenance.
  *
  * It also can determine the status of functions that are in the process of producing results, or have failed,
  * and decides if/how to move things into that state (run things).
  */

import java.io.{ByteArrayInputStream, File, FileInputStream, ObjectInputStream}

import com.cibo.provenance._

import scala.reflect.ClassTag


trait ResultTracker {

  // core method

  def resolve2[O](f: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    val f2 = f.resolveInputs(rt=this)
    val f3 = f2.deflateInputs(rt=this)
    loadResultForCallOption[O](f3) match {
      case Some(existingResult) =>
        existingResult
      case None =>
        val newResult = f2.run(this)
        saveResult(newResult)
        newResult
    }
  }

  def resolve[O](f: FunctionCallWithProvenance[O]): FunctionCallResultWithProvenance[O] = {
    val callWithInputDigests = f.resolveInputs(rt=this)
    loadResultForCallOption[O](callWithInputDigests) match {
      case Some(existingResult) =>
        existingResult
      case None =>
        val newResult = callWithInputDigests.run(this)
        saveResult(newResult)
        newResult
    }
  }



  // abstract interface

  def getCurrentBuildInfo: BuildInfo

  def saveCall[O](v: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O]

  def saveResult[O](v: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O]

  def saveValue[T : ClassTag](obj: T): Digest


  def hasResultFor[O](v: FunctionCallWithProvenance[O]): Boolean

  def hasValue[T : ClassTag](obj: T): Boolean

  def hasValue(digest: Digest): Boolean


  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]]

  def loadCallOption[O : ClassTag](className: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]]

  def loadValueOption[O : ClassTag](digest: Digest): Option[O]

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]]  // included to work with foreign data

  // support methods

  def loadValueSerializedDataOption[O : ClassTag](digest: Digest): Option[Array[Byte]] = {
    val ct = implicitly[ClassTag[O]]
    loadValueSerializedDataOption(ct.runtimeClass.getName, digest)
  }

  def loadValue[T : ClassTag](id: Digest): T =
    loadValueOption[T](id).get


  // protected methods

  protected def loadObjectFromFile[T](f: File): T =
    new ObjectInputStream(new FileInputStream(f)).readObject.asInstanceOf[T]

  protected def bytesToObject[T](a: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(a))
    val o = ois.readObject
    o.asInstanceOf[T]
  }


}


