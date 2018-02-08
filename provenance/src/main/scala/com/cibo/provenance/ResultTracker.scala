package com.cibo.provenance

/**
  * Created by ssmith on 5/16/17.
  *
  * A ResultTracker manages storage by mapping a FunctionCallWithProvenance to a FunctionCallResultWithProvenance.
  *
  * It also can determine the status of functions that are in the process of producing results, or have failed,
  * and decides if/how to move things into that state (run things).
  */

import java.io.{ByteArrayInputStream, File, FileInputStream, ObjectInputStream}

import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag


trait ResultTracker {

  // core method

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

  def saveValue[T : ClassTag : Encoder : Decoder](obj: T): Digest


  def hasResultForCall[O](v: FunctionCallWithProvenance[O]): Boolean

  def hasValue[T : ClassTag : Encoder : Decoder](obj: T): Boolean

  def hasValue(digest: Digest): Boolean


  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]]

  def loadCallOption[O : ClassTag : Encoder : Decoder](className: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]]

  def loadCallDeflatedOption[O : ClassTag : Encoder : Decoder](className: String, version: Version, digest: Digest): Option[FunctionCallWithProvenanceDeflated[O]]

  def loadValueOption[O : ClassTag : Encoder : Decoder](digest: Digest): Option[O]

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]]  // included to work with foreign data

  // support methods

  def loadValueSerializedDataOption[O : ClassTag : Encoder : Decoder](digest: Digest): Option[Array[Byte]] = {
    val ct = implicitly[ClassTag[O]]
    loadValueSerializedDataOption(ct.runtimeClass.getName, digest)
  }

  def loadValue[T : ClassTag : Encoder : Decoder](id: Digest): T =
    loadValueOption[T](id) match {
      case Some(obj) =>
        obj
      case None =>
        val ct = implicitly[ClassTag[T]]
        throw new NoSuchElementException(f"Failed to find content for $ct with ID $id!")
    }


  // protected methods

  protected def loadObjectFromFile[T](f: File): T =
    new ObjectInputStream(new FileInputStream(f)).readObject.asInstanceOf[T]

  protected def bytesToObject[T](a: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(a))
    val o = ois.readObject
    o.asInstanceOf[T]
  }

}


