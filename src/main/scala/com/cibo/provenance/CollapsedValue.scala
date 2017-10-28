package com.cibo.provenance

import scala.reflect.ClassTag

/**
  * Created by ssmith on 10/12/17.
  *
  * Collapsed values reduce a ValueWithProvenance to data used for serialization.
  * They are currently only used by the ResultTrackers.
  *
  */

abstract class CollapsedValue[O : ClassTag] {
  def getOutputClassTag: ClassTag[O] = implicitly[ClassTag[O]]

  val outputDigest: Digest
}

case class CollapsedCallResult[O : ClassTag](
  outputDigest: Digest,
  functionName: String,
  functionVersion: Version,
  functionCallDigest: Digest,
  inputGroupDigest: Digest
) extends CollapsedValue[O] {
  val isResolved: Boolean = true
}

case class CollapsedIdentityValue[O : ClassTag](outputDigest: Digest) extends CollapsedValue[O] {
  val isResolved: Boolean = true
}
