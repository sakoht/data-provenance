package com.cibo.provenance

import scala.reflect.ClassTag

/**
  * Created by ssmith on 10/12/17.
  *
  * Collapsed values reduce a ValueWithProvenance to data used for serialization.
  * They are currently only used by the ResultTrackers.
  *
  */

abstract class CollapsedValue[O : ClassTag] { // extends ValueWithProvenance[O] {
  def getOutputClassTag = implicitly[ClassTag[O]]

  val outputDigest: Digest

  /*
  def resolve(implicit rt: ResultTracker): FunctionCallResultWithProvenance[O] =
    ???

  def unresolve(implicit rt: ResultTracker): FunctionCallWithProvenance[O] =
    ???

  // This transitions an object from a collapsed state to an uncollapsed one.
  def uncollapse(implicit rt: ResultTracker): ValueWithProvenance[O] = {
    val ct = implicitly[ClassTag[O]]
    rt.loadValueOption(outputDigest)(ct) match {
      case Some(obj) =>
        obj
      case None =>
        throw new RuntimeException(f"Failed to find object of type $ct for $outputDigest in $rt!")
    }
  }

  def collapse(implicit rt: ResultTracker) = this
  */
}

case class CollapsedCallResult[O : ClassTag](outputDigest: Digest, functionName: String, functionVersion: Version, functionCallDigest: Digest, inputGroupDigest: Digest) extends CollapsedValue[O] {
  val isResolved: Boolean = true
}

case class CollapsedIdentityValue[O : ClassTag](outputDigest: Digest) extends CollapsedValue[O] {
  val isResolved: Boolean = true
}
